"""
daq-service  –  gRPC Service server

센서 목록: cam0, cam1, cam2, gnss, cpu/storage

저장 모드 자동 분기:
  USB(SSD) 연결 시  → 로컬 저장  {USB_MOUNT_ROOT}/{yyyymmdd}/cam{n}/{ts_ns}.jpg
                                  {USB_MOUNT_ROOT}/{yyyymmdd}/imu/imu.parquet
  USB(SSD) 없을 시  → Kafka REST Proxy 전송

env:
  GRPC_PORT         (default 50051)
  CAM0_DEVICE       (default /dev/video0)
  CAM1_DEVICE       (default /dev/video1)
  CAM2_DEVICE       (default /dev/video2)
  CAM_WIDTH         (default 640)
  CAM_HEIGHT        (default 360)
  JPEG_QUALITY      (default 90)
  GNSS_UDP_PORT     (default 1111)
  GNSS_SRC_IP       (default 192.168.20.50)
  USB_MOUNT_ROOT    (default /media/usb)
  BROKER_REST_URL   Kafka REST Proxy URL  e.g. http://10.0.0.1:8082
  BROKER_TOPIC_CAM0 (default sensor.cam0.jpeg)
  BROKER_TOPIC_CAM1 (default sensor.cam1.jpeg)
  BROKER_TOPIC_CAM2 (default sensor.cam2.jpeg)
  BROKER_TOPIC_GNSS (default sensor.gnss)
"""

import os
import asyncio
import logging
import socket
import struct
import time
import json
import threading
import subprocess
import base64
import shutil
from datetime import datetime
from typing import Optional

import grpc
from grpc import aio
import httpx
import pandas as pd

import daq_service_pb2 as pb
import daq_service_pb2_grpc as pb_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("daq-service")

# ── 환경변수 ──────────────────────────────────────────────────
GRPC_PORT      = int(os.environ.get("GRPC_PORT", "50051"))
CAM_DEVICES    = [
    os.environ.get("CAM0_DEVICE", "/dev/video0"),
    os.environ.get("CAM1_DEVICE", "/dev/video1"),
    os.environ.get("CAM2_DEVICE", "/dev/video2"),
]
CAM_WIDTH      = int(os.environ.get("CAM_WIDTH",    "640"))
CAM_HEIGHT     = int(os.environ.get("CAM_HEIGHT",   "360"))
JPEG_QUALITY   = int(os.environ.get("JPEG_QUALITY", "90"))
GNSS_UDP_PORT  = int(os.environ.get("GNSS_UDP_PORT", "1111"))
GNSS_SRC_IP    = os.environ.get("GNSS_SRC_IP", "192.168.20.50")
USB_MOUNT_ROOT = os.environ.get("USB_MOUNT_ROOT", "/media/usb")
BROKER_REST_URL = os.environ.get("BROKER_REST_URL", "http://localhost:8082")
BROKER_CLUSTER_ID = os.environ.get("BROKER_CLUSTER_ID", "")
VEHICLE_ID      = os.environ.get("VEHICLE_ID", "unknown")
BROKER_TOPICS  = {
    "cam0": os.environ.get("BROKER_TOPIC_CAM0", "sensor.cam0.jpeg"),
    "cam1": os.environ.get("BROKER_TOPIC_CAM1", "sensor.cam1.jpeg"),
    "cam2": os.environ.get("BROKER_TOPIC_CAM2", "sensor.cam2.jpeg"),
    "gnss": os.environ.get("BROKER_TOPIC_GNSS", "sensor.gnss"),
}

SENSOR_NAMES = ["cam0", "cam1", "cam2", "gnss", "cpu/storage"]


# ── USB helper ────────────────────────────────────────────────
def _find_usb_mount() -> Optional[str]:
    """USB_MOUNT_ROOT 또는 그 하위에서 마운트 포인트 탐색"""
    root = USB_MOUNT_ROOT
    if os.path.ismount(root):
        return root
    try:
        for name in sorted(os.listdir(root)):
            candidate = os.path.join(root, name)
            if os.path.ismount(candidate):
                return candidate
    except FileNotFoundError:
        pass
    return None

def _usb_available() -> bool:
    return _find_usb_mount() is not None

def _get_save_dir(subdir: str) -> Optional[str]:
    mount = _find_usb_mount()
    if not mount:
        return None
    path = os.path.join(mount, datetime.now().strftime("%Y%m%d"), subdir)
    os.makedirs(path, exist_ok=True)
    return path

def _usb_disk_usage() -> dict:
    """USB 마운트 경로 disk usage → bytes 단위 반환"""
    mount = _find_usb_mount()
    if not mount:
        return {"total": 0, "used": 0, "free": 0}
    usage = shutil.disk_usage(mount)
    return {"total": usage.total, "used": usage.used, "free": usage.free}


# ── NOVATEL 파서 ──────────────────────────────────────────────
SYNC            = (0xAA, 0x44, 0x12)
HEADER_LEN      = 28
MSG_INSPVAXB    = 1465
MSG_BESTGNSSPOS = 1429

def parse_novatel(data: bytes) -> Optional[dict]:
    if len(data) < HEADER_LEN:
        return None
    if data[0] != SYNC[0] or data[1] != SYNC[1] or data[2] != SYNC[2]:
        return None
    hdr_len, msg_len, msg_id = struct.unpack_from("<BBHH", data, 3)
    week, ms = struct.unpack_from("<HI", data, 14)
    payload  = data[hdr_len: hdr_len + msg_len]
    gps_ts   = week * 604800.0 + ms / 1000.0

    if msg_id == MSG_INSPVAXB and len(payload) >= 88:
        (_, _, lat, lon, hgt, _, vn, ve, vu, roll, pitch, azimuth) = \
            struct.unpack_from("<IIdddddddddd", payload)
        return {
            "type": "ins_pvax", "ts_ns": time.time_ns(), "gps_ts": gps_ts,
            "latitude": lat, "longitude": lon, "height": hgt,
            "vel_north": vn, "vel_east": ve, "vel_up": vu,
            "roll": roll, "pitch": pitch, "azimuth": azimuth,
        }
    if msg_id == MSG_BESTGNSSPOS and len(payload) >= 44:
        _, _, lat, lon, hgt, _ = struct.unpack_from("<IIdddf", payload)
        return {
            "type": "gnss_pos", "ts_ns": time.time_ns(), "gps_ts": gps_ts,
            "latitude": lat, "longitude": lon, "height": hgt,
            "vel_north": 0.0, "vel_east": 0.0, "vel_up": 0.0,
            "roll": 0.0, "pitch": 0.0, "azimuth": 0.0,
        }
    return None


# ── Parquet appender ─────────────────────────────────────────
PARQUET_ROLL_ROWS = int(os.environ.get("PARQUET_ROLL_ROWS", "1000"))

class ParquetAppender:
    """
    imu parquet 롤링 writer (pandas + fastparquet)
    - PARQUET_ROLL_ROWS(기본 1000) 행 차면 → 다음 파일로 롤링
      imu_{ts_ns}_{idx:03d}.parquet
    - 수집 종료(close) 시 → 행 수 관계없이 즉시 저장
    """

    IMU_COLUMNS = [
        "ts_ns", "gps_ts", "type",
        "latitude", "longitude", "height",
        "vel_north", "vel_east", "vel_up",
        "roll", "pitch", "azimuth",
    ]

    def __init__(self):
        self._lock      = threading.Lock()
        self._rows:     list = []
        self._save_dir: Optional[str] = None
        self._file_idx  = 0
        self._first_ts: Optional[int] = None

    def _flush(self):
        """현재 버퍼를 parquet 파일로 저장"""
        if not self._rows or not self._save_dir:
            return
        ts   = self._first_ts or time.time_ns()
        path = os.path.join(
            self._save_dir,
            f"imu_{ts}_{self._file_idx:03d}.parquet"
        )
        df = pd.DataFrame(self._rows, columns=self.IMU_COLUMNS)
        df.to_parquet(path, engine="fastparquet", compression="snappy", index=False)
        log.info("ParquetAppender saved: %s (%d rows)", path, len(self._rows))
        self._rows      = []
        self._first_ts  = None

    def append(self, data: dict):
        with self._lock:
            save_dir = _get_save_dir("imu")
            if not save_dir:
                return

            ts_ns = data.get("ts_ns", time.time_ns())

            # 날짜 바뀐 경우 → flush 후 새 dir
            if self._save_dir and self._save_dir != save_dir:
                self._flush()
                self._file_idx = 0

            self._save_dir = save_dir
            if self._first_ts is None:
                self._first_ts = ts_ns

            row = [
                data.get("ts_ns",     0),
                data.get("gps_ts",    0.0),
                data.get("type",      ""),
                data.get("latitude",  0.0),
                data.get("longitude", 0.0),
                data.get("height",    0.0),
                data.get("vel_north", 0.0),
                data.get("vel_east",  0.0),
                data.get("vel_up",    0.0),
                data.get("roll",      0.0),
                data.get("pitch",     0.0),
                data.get("azimuth",   0.0),
            ]
            self._rows.append(row)

            # 롤링: PARQUET_ROLL_ROWS 도달 시 flush
            if len(self._rows) >= PARQUET_ROLL_ROWS:
                self._flush()
                self._file_idx += 1

    def close(self):
        """수집 종료 시 호출 — 행 수 관계없이 즉시 저장"""
        with self._lock:
            self._flush()
            self._file_idx = 0
            log.info("ParquetAppender closed")


# ── GStreamer snapshot helper ─────────────────────────────────
def _gst_capture_jpeg(device: str) -> Optional[bytes]:
    cmd = [
        "gst-launch-1.0", "-q",
        "v4l2src", f"device={device}", "num-buffers=1",
        "!", "video/x-raw,format=UYVY",
        "!", "videoconvert",
        "!", "jpegenc", f"quality={JPEG_QUALITY}",
        "!", "fdsink", "fd=1",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=5)
        if result.returncode == 0 and result.stdout:
            return result.stdout
    except Exception as e:
        log.warning("gst snapshot failed device=%s err=%s", device, e)
    return None


# ── CameraWorker ──────────────────────────────────────────────
class CameraWorker:
    """
    GStreamer → JPEG 1fps
    USB 있으면 → {USB}/{yyyymmdd}/cam{n}/{ts_ns}.jpg 저장
    USB 없으면 → Kafka REST Proxy 전송
    """

    def __init__(self, cam_id: int, device: str):
        self.cam_id   = cam_id
        self.device   = device
        self.name     = f"cam{cam_id}"
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._proc:   Optional[subprocess.Popen] = None
        self._last_jpeg: Optional[bytes] = None
        self._lock    = threading.Lock()
        self._http    = httpx.Client(timeout=5.0)

    @property
    def is_acquiring(self) -> bool:
        return self._running

    def get_snapshot(self) -> Optional[bytes]:
        with self._lock:
            if self._last_jpeg:
                return self._last_jpeg
        return _gst_capture_jpeg(self.device)

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info("CameraWorker %s started device=%s", self.name, self.device)

    def stop(self):
        self._running = False
        if self._proc:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=3)
            except Exception:
                pass
            self._proc = None
        log.info("CameraWorker %s stopped", self.name)

    def _run(self):
        cmd = [
            "gst-launch-1.0", "-q",
            "v4l2src", f"device={self.device}",
            "!", "video/x-raw,format=UYVY",
            "!", "videoconvert",
            "!", "jpegenc", f"quality={JPEG_QUALITY}",
            "!", "fdsink", "fd=1",
        ]
        try:
            self._proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            while self._running:
                jpeg = self._read_jpeg_frame()
                if jpeg is None:
                    break
                with self._lock:
                    self._last_jpeg = jpeg
                if _usb_available():
                    self._save_local(jpeg)
                else:
                    self._send_kafka(jpeg)
        except Exception as e:
            log.error("CameraWorker %s error: %s", self.name, e)
        finally:
            self._running = False

    def _read_jpeg_frame(self) -> Optional[bytes]:
        buf = b""
        try:
            while self._running:
                chunk = self._proc.stdout.read(4096)
                if not chunk:
                    return None
                buf += chunk
                soi = buf.find(b"\xff\xd8")
                if soi == -1:
                    buf = buf[-2:]
                    continue
                buf = buf[soi:]
                eoi = buf.find(b"\xff\xd9")
                if eoi == -1:
                    continue
                frame = buf[:eoi + 2]
                buf   = buf[eoi + 2:]
                return frame
        except Exception:
            return None

    def _save_local(self, jpeg: bytes):
        save_dir = _get_save_dir(self.name)
        if not save_dir:
            return
        ts_ns    = time.time_ns()
        filename = os.path.join(save_dir, f"{ts_ns}.jpg")
        try:
            with open(filename, "wb") as f:
                f.write(jpeg)
        except Exception as e:
            log.warning("CameraWorker %s local save failed: %s", self.name, e)

    def _send_kafka(self, jpeg: bytes):
        ts_ns   = time.time_ns()
        encoded = base64.b64encode(
            struct.pack(">QI", ts_ns, len(jpeg)) + jpeg).decode()
        payload = {
            "key": {
                "type": "BINARY",
                "data": base64.b64encode(VEHICLE_ID.encode()).decode()
            },
            "value": {
                "type": "BINARY",
                "data": encoded
            },
            "headers": [
                {"name": "vehicle_id", "value": base64.b64encode(VEHICLE_ID.encode()).decode()},
                {"name": "sensor",     "value": base64.b64encode(self.name.encode()).decode()},
                {"name": "ts_ns",      "value": base64.b64encode(str(ts_ns).encode()).decode()},
            ]
        }
        try:
            topic = BROKER_TOPICS[self.name]
            self._http.post(
                f"{BROKER_REST_URL}/v3/clusters/{BROKER_CLUSTER_ID}/topics/{topic}/records",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            log.warning("CameraWorker %s kafka send failed: %s", self.name, e)

# ── GnssWorker ────────────────────────────────────────────────
class GnssWorker:
    """
    UDP NOVATEL binary → parse
    USB 있으면 → imu.parquet append
    USB 없으면 → Kafka REST Proxy 전송
    """

    def __init__(self):
        self.name     = "gnss"
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_data: Optional[dict] = None
        self._sock:   Optional[socket.socket] = None
        self._parquet = ParquetAppender()
        self._http    = httpx.Client(timeout=5.0)

    @property
    def is_acquiring(self) -> bool:
        return self._running

    def get_snapshot(self) -> Optional[dict]:
        return self._last_data

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info("GnssWorker started port=%d src=%s", GNSS_UDP_PORT, GNSS_SRC_IP)

    def stop(self):
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        self._parquet.close()
        log.info("GnssWorker stopped")

    def _run(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        self._sock.settimeout(1.0)
        self._sock.bind(("0.0.0.0", GNSS_UDP_PORT))
        while self._running:
            try:
                raw, addr = self._sock.recvfrom(65535)
                if addr[0] != GNSS_SRC_IP:
                    continue
                parsed = parse_novatel(raw)
                if parsed:
                    self._last_data = parsed
                    if _usb_available():
                        self._parquet.append(parsed)
                    else:
                        self._send_kafka(parsed)
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    log.error("GnssWorker error: %s", e)

    def _send_kafka(self, data: dict):
        payload = {
            "records": [{
                "key":   f"{VEHICLE_ID}/{data['gps_ts']}",
                "value": json.dumps(data, ensure_ascii=False),
                "headers": [
                    {"key": "vehicle_id", "value": VEHICLE_ID},
                    {"key": "sensor",     "value": "imu"},
                ]
            }]
        }
        try:
            self._http.post(
                f"{BROKER_REST_URL}/topics/{BROKER_TOPICS['gnss']}",
                json=payload,
                headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
            )
        except Exception as e:
            log.warning("GnssWorker kafka send failed: %s", e)


# ── gRPC Servicer ─────────────────────────────────────────────
class DaqServicer(pb_grpc.ServiceServicer):

    def __init__(self):
        self._cams = [CameraWorker(i, CAM_DEVICES[i]) for i in range(3)]
        self._gnss = GnssWorker()
        self._workers = {
            "cam0":        self._cams[0],
            "cam1":        self._cams[1],
            "cam2":        self._cams[2],
            "gnss":        self._gnss,
            "cpu/storage": None,   # storage는 worker 없음, snapshot만 반환
        }

    def _worker(self, name: str):
        return self._workers.get(name)

    async def ping(self, request, context):
        return pb.void()

    async def get_sensors(self, request, context):
        return pb.Sensors(list=SENSOR_NAMES)

    async def is_connected(self, request, context):
        name = request.name
        if name == "cpu/storage":
            usb = _usb_available()
            state = pb.Connection.State.CONNECTED if usb else pb.Connection.State.DISCONNECTED
            return pb.Connection(name=name, state=state)
        w = self._workers.get(name)
        if w is None:
            return pb.Connection(name=name, state=pb.Connection.State.UNKNOWN)
        if name.startswith("cam"):
            idx = int(name[-1])
            ok  = os.path.exists(CAM_DEVICES[idx])
            state = pb.Connection.State.CONNECTED if ok else pb.Connection.State.DISCONNECTED
        else:
            state = pb.Connection.State.CONNECTED if w.is_acquiring else pb.Connection.State.DISCONNECTED
        return pb.Connection(name=name, state=state)

    async def is_healthy(self, request, context):
        name = request.name
        if name == "cpu/storage":
            usb = _usb_available()
            status = pb.Health.Status.GOOD if usb else pb.Health.Status.BAAD
            reason = "" if usb else "USB not mounted"
            return pb.Health(name=name, status=status, reason=reason)
        w = self._workers.get(name)
        if w is None:
            return pb.Health(name=name, status=pb.Health.Status.UNKNOWN, reason="unknown sensor")
        if name.startswith("cam"):
            idx    = int(name[-1])
            ok     = os.path.exists(CAM_DEVICES[idx])
            status = pb.Health.Status.GOOD if ok else pb.Health.Status.BAAD
            reason = "" if ok else f"{CAM_DEVICES[idx]} not found"
        else:
            ok     = self._gnss._last_data is not None
            status = pb.Health.Status.GOOD if ok else pb.Health.Status.WARN
            reason = "" if ok else "no gnss data yet"
        return pb.Health(name=name, status=status, reason=reason)

    async def get_snapshot(self, request, context):
        name = request.name
        if name == "cpu/storage":
            usage = _usb_disk_usage()
            return pb.SensorSnapshot(
                name=name,
                content_type="application/json",
                data=json.dumps(usage).encode()
            )
        w = self._workers.get(name)
        if w is None:
            return pb.SensorSnapshot(name=name)
        if name.startswith("cam"):
            jpeg = w.get_snapshot()
            if jpeg:
                return pb.SensorSnapshot(
                    name=name, content_type="image/jpeg", data=jpeg)
        else:
            data = w.get_snapshot()
            if data:
                return pb.SensorSnapshot(
                    name=name,
                    content_type="application/json",
                    data=json.dumps(data).encode()
                )
        return pb.SensorSnapshot(name=name)

    async def get_snapshots(self, request, context):
        snaps = []
        # storage
        usage = _usb_disk_usage()
        snaps.append(pb.SensorSnapshot(
            name="cpu/storage",
            content_type="application/json",
            data=json.dumps(usage).encode()
        ))
        for name, w in self._workers.items():
            if name == "cpu/storage" or w is None:
                continue
            if name.startswith("cam"):
                jpeg = w.get_snapshot()
                if jpeg:
                    snaps.append(pb.SensorSnapshot(
                        name=name, content_type="image/jpeg", data=jpeg))
            else:
                data = w.get_snapshot()
                if data:
                    snaps.append(pb.SensorSnapshot(
                        name=name,
                        content_type="application/json",
                        data=json.dumps(data).encode()
                    ))
        return pb.SensorSnapshots(list=snaps)

    async def is_acquiring(self, request, context):
        name = request.name
        if name == "cpu/storage":
            return pb.Acquisition(name=name, state=pb.Acquisition.State.NOT_ACQUIRING)
        w = self._workers.get(name)
        if w is None:
            return pb.Acquisition(name=name, state=pb.Acquisition.State.UNKNOWN)
        state = pb.Acquisition.State.ACQUIRING if w.is_acquiring \
            else pb.Acquisition.State.NOT_ACQUIRING
        return pb.Acquisition(name=name, state=state)

    async def start_acquisition(self, request, context):
        name = request.name
        if name == "cpu/storage":
            return pb.Acquisition(name=name, state=pb.Acquisition.State.NOT_ACQUIRING)
        w = self._workers.get(name)
        if w is None:
            return pb.Acquisition(name=name, state=pb.Acquisition.State.UNKNOWN,
                                  reason="unknown sensor")
        w.start()
        mode = "local USB" if _usb_available() else "Kafka"
        log.info("Acquisition started: %s → %s", name, mode)
        return pb.Acquisition(name=name, state=pb.Acquisition.State.ACQUIRING)

    async def stop_acquisition(self, request, context):
        name = request.name
        if name == "cpu/storage":
            return pb.Acquisition(name=name, state=pb.Acquisition.State.NOT_ACQUIRING)
        w = self._workers.get(name)
        if w is None:
            return pb.Acquisition(name=name, state=pb.Acquisition.State.UNKNOWN,
                                  reason="unknown sensor")
        w.stop()
        return pb.Acquisition(name=name, state=pb.Acquisition.State.NOT_ACQUIRING)


async def serve():
    server = aio.server()
    pb_grpc.add_ServiceServicer_to_server(DaqServicer(), server)
    listen_addr = f"0.0.0.0:{GRPC_PORT}"
    server.add_insecure_port(listen_addr)
    await server.start()
    log.info("daq-service gRPC listening on %s", listen_addr)
    log.info("USB_MOUNT_ROOT=%s  broker=%s", USB_MOUNT_ROOT, BROKER_REST_URL)
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())