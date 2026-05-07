"""
daq-service  –  gRPC Service server
 - cam0/1/2 : GStreamer V4L2 → JPEG snapshot / streaming acquisition
 - gnss      : UDP NOVATEL binary → JSON → Remote Kafka REST Proxy 전송

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
  BROKER_REST_URL   Remote Kafka REST Proxy URL  e.g. http://10.0.0.1:8082
  BROKER_TOPIC_CAM  (default sensor.cam{n}.jpeg)
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
import queue
import base64
from typing import Optional

import grpc
from grpc import aio

import httpx

# proto 생성 파일 import
import daq_service_pb2 as pb
import daq_service_pb2_grpc as pb_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("daq-service")

# ── 환경변수 ──────────────────────────────────────────────────
GRPC_PORT       = int(os.environ.get("GRPC_PORT", "50051"))
CAM_DEVICES     = [
    os.environ.get("CAM0_DEVICE", "/dev/video0"),
    os.environ.get("CAM1_DEVICE", "/dev/video1"),
    os.environ.get("CAM2_DEVICE", "/dev/video2"),
]
CAM_WIDTH       = int(os.environ.get("CAM_WIDTH",    "640"))
CAM_HEIGHT      = int(os.environ.get("CAM_HEIGHT",   "360"))
JPEG_QUALITY    = int(os.environ.get("JPEG_QUALITY", "90"))
GNSS_UDP_PORT   = int(os.environ.get("GNSS_UDP_PORT", "1111"))
GNSS_SRC_IP     = os.environ.get("GNSS_SRC_IP", "192.168.20.50")
BROKER_REST_URL = os.environ.get("BROKER_REST_URL", "http://localhost:8082")
TOPIC_GNSS      = os.environ.get("BROKER_TOPIC_GNSS", "sensor.gnss")

SENSOR_NAMES = ["cam0", "cam1", "cam2", "gnss"]

# ── NOVATEL 파서 ──────────────────────────────────────────────
SYNC = (0xAA, 0x44, 0x12)
HEADER_LEN     = 28
MSG_INSPVAXB   = 1465
MSG_BESTGNSSPOS = 1429

def parse_novatel(data: bytes) -> Optional[dict]:
    if len(data) < HEADER_LEN:
        return None
    if data[0] != SYNC[0] or data[1] != SYNC[1] or data[2] != SYNC[2]:
        return None
    hdr_len, msg_len, msg_id = struct.unpack_from("<BBHH", data, 3)
    week, ms = struct.unpack_from("<HI", data, 14)
    payload = data[hdr_len: hdr_len + msg_len]
    gps_ts = week * 604800.0 + ms / 1000.0

    if msg_id == MSG_INSPVAXB and len(payload) >= 88:
        (_, _, lat, lon, hgt, _, vn, ve, vu, roll, pitch, azimuth) = \
            struct.unpack_from("<IIdddddddddd", payload)
        return {
            "type": "ins_pvax", "ts_ns": time.time_ns(),
            "gps_ts": gps_ts,
            "latitude": lat, "longitude": lon, "height": hgt,
            "vel_north": vn, "vel_east": ve, "vel_up": vu,
            "roll": roll, "pitch": pitch, "azimuth": azimuth,
        }
    if msg_id == MSG_BESTGNSSPOS and len(payload) >= 44:
        sol_status, pos_type, lat, lon, hgt, undulation = \
            struct.unpack_from("<IIdddf", payload)
        return {
            "type": "gnss_pos", "ts_ns": time.time_ns(),
            "gps_ts": gps_ts,
            "latitude": lat, "longitude": lon, "height": hgt,
        }
    return None


# ── GStreamer 캡처 helper ────────────────────────────────────
def _gst_capture_jpeg(device: str) -> Optional[bytes]:
    """GStreamer로 단일 JPEG 프레임 캡처 (snapshot용)"""
    cmd = [
        "gst-launch-1.0", "-q",
        "v4l2src", f"device={device}", "num-buffers=1",
        "!", "videoconvert",
        "!", f"video/x-raw,width={CAM_WIDTH},height={CAM_HEIGHT}",
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


class CameraWorker:
    """acquisition 상태에서 GStreamer 파이프 실행 → REST Proxy 전송"""

    def __init__(self, cam_id: int, device: str):
        self.cam_id   = cam_id
        self.device   = device
        self.name     = f"cam{cam_id}"
        self.topic    = os.environ.get(f"BROKER_TOPIC_CAM{cam_id}", f"sensor.cam{cam_id}.jpeg")
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._proc: Optional[subprocess.Popen] = None
        self._last_jpeg: Optional[bytes] = None
        self._lock = threading.Lock()

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
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info("CameraWorker cam%d started device=%s", self.cam_id, self.device)

    def stop(self):
        self._running = False
        if self._proc:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=3)
            except Exception:
                pass
            self._proc = None
        log.info("CameraWorker cam%d stopped", self.cam_id)

    def _run(self):
        cmd = [
            "gst-launch-1.0", "-q",
            "v4l2src", f"device={self.device}",
            "!", "videoconvert",
            "!", f"video/x-raw,width={CAM_WIDTH},height={CAM_HEIGHT},framerate=1/1",
            "!", "jpegenc", f"quality={JPEG_QUALITY}",
            "!", "fdsink", "fd=1",
        ]
        try:
            self._proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            client = httpx.Client(timeout=5.0)
            while self._running:
                # 4B 길이 헤더 읽기
                hdr = self._proc.stdout.read(4)
                if not hdr or len(hdr) < 4:
                    break
                jpeg_len = struct.unpack(">I", hdr)[0]
                if jpeg_len == 0 or jpeg_len > 5_000_000:
                    continue
                jpeg = self._proc.stdout.read(jpeg_len)
                if not jpeg:
                    break
                with self._lock:
                    self._last_jpeg = jpeg
                # REST Proxy 전송
                self._send_to_broker(client, jpeg)
        except Exception as e:
            log.error("CameraWorker cam%d error: %s", self.cam_id, e)
        finally:
            self._running = False

    def _send_to_broker(self, client: httpx.Client, jpeg: bytes):
        ts_ns = time.time_ns()
        encoded = base64.b64encode(
            struct.pack(">QI", ts_ns, len(jpeg)) + jpeg
        ).decode()
        payload = {
            "records": [{
                "key": str(ts_ns),
                "value": encoded,
            }]
        }
        try:
            url = f"{BROKER_REST_URL}/topics/{self.topic}"
            client.post(url, json=payload,
                        headers={"Content-Type": "application/vnd.kafka.binary.v2+json"})
        except Exception as e:
            log.warning("broker send failed cam%d: %s", self.cam_id, e)


class GnssWorker:
    """UDP NOVATEL → parse → REST Proxy 전송"""

    def __init__(self):
        self.name = "gnss"
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_data: Optional[dict] = None
        self._sock: Optional[socket.socket] = None

    @property
    def is_acquiring(self) -> bool:
        return self._running

    def get_snapshot(self) -> Optional[dict]:
        return self._last_data

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info("GnssWorker started port=%d src=%s", GNSS_UDP_PORT, GNSS_SRC_IP)

    def stop(self):
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        log.info("GnssWorker stopped")

    def _run(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        self._sock.settimeout(1.0)
        self._sock.bind(("0.0.0.0", GNSS_UDP_PORT))
        client = httpx.Client(timeout=5.0)
        while self._running:
            try:
                raw, addr = self._sock.recvfrom(65535)
                if addr[0] != GNSS_SRC_IP:
                    continue
                parsed = parse_novatel(raw)
                if parsed:
                    self._last_data = parsed
                    self._send_to_broker(client, parsed)
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    log.error("GnssWorker error: %s", e)

    def _send_to_broker(self, client: httpx.Client, data: dict):
        payload = {
            "records": [{
                "key": str(data["gps_ts"]),
                "value": json.dumps(data, ensure_ascii=False),
            }]
        }
        try:
            url = f"{BROKER_REST_URL}/topics/{TOPIC_GNSS}"
            client.post(url, json=payload,
                        headers={"Content-Type": "application/vnd.kafka.json.v2+json"})
        except Exception as e:
            log.warning("broker send failed gnss: %s", e)


# ── gRPC Servicer ─────────────────────────────────────────────
class DaqServicer(pb_grpc.ServiceServicer):

    def __init__(self):
        self._cams = [CameraWorker(i, CAM_DEVICES[i]) for i in range(3)]
        self._gnss = GnssWorker()
        self._workers = {
            "cam0": self._cams[0],
            "cam1": self._cams[1],
            "cam2": self._cams[2],
            "gnss": self._gnss,
        }

    def _worker(self, name: str):
        return self._workers.get(name)

    async def ping(self, request, context):
        return pb.void()

    async def get_sensors(self, request, context):
        return pb.Sensors(list=SENSOR_NAMES)

    async def is_connected(self, request, context):
        name = request.name
        w = self._worker(name)
        if w is None:
            return pb.Connection(name=name, state=pb.Connection.State.UNKNOWN)
        # cam: device 존재 여부 확인
        if name.startswith("cam"):
            idx = int(name[-1])
            dev_exists = os.path.exists(CAM_DEVICES[idx])
            state = pb.Connection.State.CONNECTED if dev_exists else pb.Connection.State.DISCONNECTED
        else:
            # gnss: 소켓 바인딩 상태로 판단 (간단히 running 여부)
            state = pb.Connection.State.CONNECTED if w.is_acquiring else pb.Connection.State.DISCONNECTED
        return pb.Connection(name=name, state=state)

    async def is_healthy(self, request, context):
        name = request.name
        w = self._worker(name)
        if w is None:
            return pb.Health(name=name, status=pb.Health.Status.UNKNOWN, reason="unknown sensor")
        if name.startswith("cam"):
            idx = int(name[-1])
            ok = os.path.exists(CAM_DEVICES[idx])
            status = pb.Health.Status.GOOD if ok else pb.Health.Status.BAAD
            reason = "" if ok else f"{CAM_DEVICES[idx]} not found"
        else:
            ok = self._gnss._last_data is not None
            status = pb.Health.Status.GOOD if ok else pb.Health.Status.WARN
            reason = "" if ok else "no gnss data yet"
        return pb.Health(name=name, status=status, reason=reason)

    async def get_snapshot(self, request, context):
        name = request.name
        w = self._worker(name)
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
        for name, w in self._workers.items():
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
        w = self._worker(name)
        if w is None:
            return pb.Acquisition(name=name, state=pb.Acquisition.State.UNKNOWN)
        state = pb.Acquisition.State.ACQUIRING if w.is_acquiring \
            else pb.Acquisition.State.NOT_ACQUIRING
        return pb.Acquisition(name=name, state=state)

    async def start_acquisition(self, request, context):
        name = request.name
        w = self._worker(name)
        if w is None:
            return pb.Acquisition(name=name, state=pb.Acquisition.State.UNKNOWN,
                                  reason="unknown sensor")
        w.start()
        return pb.Acquisition(name=name, state=pb.Acquisition.State.ACQUIRING)

    async def stop_acquisition(self, request, context):
        name = request.name
        w = self._worker(name)
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
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
