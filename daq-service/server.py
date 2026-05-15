"""
daq-service  –  gRPC Service server  (Kafka REST Proxy v3)

센서 목록: cam0, cam1, cam2, gnss, cpu/storage

저장 모드:
  USB(SSD) 연결 시  → 로컬 저장
    cam  : {USB_MOUNT}/{USB_NAME}/{yyyymmdd}/cam{n}/{ts_ns}.jpg
    imu  : {USB_MOUNT}/{USB_NAME}/{yyyymmdd}/imu/gnss.jsonl  (append)
    fused: {USB_MOUNT}/{USB_NAME}/{yyyymmdd}/fused/{ts_ns}.bin
  USB(SSD) 없을 시  → Kafka REST Proxy v3 전송

BynavX1 GNSS 통신 구조:
  FakeReceiver  : TCP {GNSS_SRC_IP}:{GNSS_TCP_PORT} 연결 유지
                  → 장비가 WebSocket 데이터 전송 유지하도록
  BynavX1Proxy  : WebSocket ws://{GNSS_SRC_IP}/webSocket
                  → #BESTPOSA / #BESTGNSSPOSA NMEA ASCII 파싱
                  → lat / lon / postype / solstat 추출

env:
  GRPC_PORT              (default 50051)
  CAM0/1/2_DEVICE        (default /dev/video0~2)
  CAM_WIDTH / CAM_HEIGHT (default 640 / 360)
  JPEG_QUALITY           (default 90)
  CAM_PUBLISH_FPS        (default 1)
  GNSS_SRC_IP            (default 192.168.20.50)
  GNSS_TCP_PORT          (default 1111)   ← FakeReceiver TCP 포트
  USB_MOUNT_ROOT         (default /media/swm)

  VEHICLE_ID
  BROKER_TOPIC_CAM0/1/2  (default sensor.cam0/1/2.jpeg)
  BROKER_TOPIC_GNSS      (default sensor.gnss)
"""

import asyncio
import json
import logging
import os
import random
import shutil
import socket
import struct
import subprocess
import threading
import time
from datetime import datetime
from typing import Optional

from grpc import aio

import daq_service_pb2 as pb
import daq_service_pb2_grpc as pb_grpc

from kafka_client import get_kafka
from fused_producer import FusedFrameProducer

import websocket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("daq-service")

# ── 환경변수 ──────────────────────────────────────────────────
GRPC_PORT       = int(os.environ.get("GRPC_PORT", "50051"))
CAM_DEVICES     = [
    os.environ.get("CAM0_DEVICE", "/dev/video0"),
    os.environ.get("CAM1_DEVICE", "/dev/video1"),
    os.environ.get("CAM2_DEVICE", "/dev/video2"),
]
CAM_WIDTH       = int(os.environ.get("CAM_WIDTH",       "640"))
CAM_HEIGHT      = int(os.environ.get("CAM_HEIGHT",      "360"))
JPEG_QUALITY    = int(os.environ.get("JPEG_QUALITY",    "90"))
CAM_PUBLISH_FPS = int(os.environ.get("CAM_PUBLISH_FPS", "1"))
GNSS_SRC_IP     = os.environ.get("GNSS_SRC_IP",   "192.168.20.50")
GNSS_TCP_PORT   = int(os.environ.get("GNSS_TCP_PORT", "1111"))
USB_MOUNT_ROOT  = os.environ.get("USB_MOUNT_ROOT", "/media/swm")
VEHICLE_ID      = os.environ.get("VEHICLE_ID",     "unknown")
BROKER_TOPICS   = {
    "cam0": os.environ.get("BROKER_TOPIC_CAM0", "sensor.cam0.jpeg"),
    "cam1": os.environ.get("BROKER_TOPIC_CAM1", "sensor.cam1.jpeg"),
    "cam2": os.environ.get("BROKER_TOPIC_CAM2", "sensor.cam2.jpeg"),
    "gnss": os.environ.get("BROKER_TOPIC_GNSS", "sensor.gnss"),
}

SENSOR_NAMES = ["cam0", "cam1", "cam2", "gnss", "cpu/storage"]

_fused = FusedFrameProducer()


# ── USB helper ────────────────────────────────────────────────
def _find_usb_mount() -> Optional[str]:
    """
    /proc/mounts 기준으로 USB_MOUNT_ROOT 하위에 실제 블록 디바이스가
    마운트된 경로를 찾아 반환.
    os.path.ismount()는 bind mount 환경에서 오탐 발생하므로 사용 안 함.
    예: /dev/sda1 on /media/swm/SSD → /media/swm/SSD 반환
    """
    root = USB_MOUNT_ROOT.rstrip("/")
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 2:
                    continue
                device, mountpoint = parts[0], parts[1]
                # 실제 외장 USB 디바이스만 (sd* 계열)
                # nvme/mmcblk 등 내장 디스크 제외
                dev_name = device.split("/")[-1]
                if not dev_name.startswith("sd"):
                    continue
                # USB_MOUNT_ROOT 하위에 마운트된 것만
                if mountpoint == root or mountpoint.startswith(root + "/"):
                    return mountpoint
    except Exception as e:
        log.warning("_find_usb_mount error: %s", e)
    return None

def _usb_available() -> bool:
    return _find_usb_mount() is not None

def _get_save_dir(subdir: str) -> Optional[str]:
    """
    저장 경로: {USB_MOUNT}/{yyyymmdd}/{subdir}/
    예: /media/swm/SSD/20260515/cam0/
    """
    mount = _find_usb_mount()
    if not mount:
        return None
    path = os.path.join(mount, datetime.now().strftime("%Y%m%d"), subdir)
    os.makedirs(path, exist_ok=True)
    return path

def _usb_disk_usage() -> dict:
    mount = _find_usb_mount()
    if not mount:
        return {"total": 0, "used": 0, "free": 0}
    u = shutil.disk_usage(mount)
    return {"total": u.total, "used": u.used, "free": u.free}


# ── Parquet appender ─────────────────────────────────────────
class JsonlAppender:
    """
    GNSS 데이터를 JSONL(JSON Lines) 형식으로 저장.
    - 1행씩 즉시 append → rows 임계값 없음, 프로세스 죽어도 손실 없음
    - 저장 경로: {USB_MOUNT}/{yyyymmdd}/imu/gnss.jsonl
    - 날짜 바뀌면 새 파일로 자동 전환
    - pandas로 읽기: pd.read_json("gnss.jsonl", lines=True)
    """

    ROLL_MINUTES = int(os.environ.get("GNSS_ROLL_MINUTES", "1"))  # 분 단위 롤링 (default 1분)

    def __init__(self):
        self._lock       = threading.Lock()
        self._path:      Optional[str] = None
        self._file       = None
        self._roll_ts:   float = 0.0   # 현재 파일의 롤링 기준 시각 (time.time())

    def _get_dir(self) -> Optional[str]:
        return _get_save_dir("imu")

    def _need_roll(self, save_dir: str) -> bool:
        """새 파일이 필요한지 확인: 파일 없음 / 날짜 변경 / 분 단위 롤링"""
        if self._file is None:
            return True
        if not self._path.startswith(save_dir):   # 날짜 변경
            return True
        if time.time() - self._roll_ts >= self.ROLL_MINUTES * 60:  # 분 롤링
            return True
        return False

    def append(self, data: dict):
        with self._lock:
            save_dir = self._get_dir()
            if not save_dir:
                return
            try:
                if self._need_roll(save_dir):
                    if self._file:
                        self._file.flush()
                        self._file.close()
                    ts_ns = data.get("ts_ns", time.time_ns())
                    filename = f"gnss_{ts_ns}.jsonl"
                    self._path = os.path.join(save_dir, filename)
                    self._file = open(self._path, "a", buffering=1)  # line-buffered
                    self._roll_ts = time.time()
                    log.info("JsonlAppender rolled → %s", self._path)
                self._file.write(json.dumps(data, ensure_ascii=False) + "\n")
            except Exception as e:
                log.warning("JsonlAppender write failed: %s", e)
                if self._file:
                    try:
                        self._file.close()
                    except Exception:
                        pass
                self._file = None
                self._path = None

    def close(self):
        with self._lock:
            if self._file:
                try:
                    self._file.flush()
                    self._file.close()
                except Exception:
                    pass
                self._file = None
                self._path = None
            log.info("JsonlAppender closed")


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
        r = subprocess.run(cmd, capture_output=True, timeout=5)
        if r.returncode == 0 and r.stdout:
            return r.stdout
    except Exception as e:
        log.warning("gst snapshot failed device=%s: %s", device, e)
    return None


# ── CameraWorker ──────────────────────────────────────────────
class CameraWorker:
    def __init__(self, cam_id: int, device: str):
        self.cam_id   = cam_id
        self.device   = device
        self.name     = f"cam{cam_id}"
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._proc:   Optional[subprocess.Popen] = None
        self._last_jpeg: Optional[bytes] = None
        self._lock    = threading.Lock()

    @property
    def is_acquiring(self) -> bool:
        return self._running

    def get_snapshot(self) -> Optional[bytes]:
        # 수집 중이면 _last_jpeg (GStreamer 파이프라인에서 실시간 갱신됨)
        # 수집 중 아니면 즉석 캡처
        with self._lock:
            if self._running and self._last_jpeg:
                return self._last_jpeg
        if not self._running:
            return _gst_capture_jpeg(self.device)
        return None

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info("CameraWorker %s started  device=%s  fps=%d",
                 self.name, self.device, CAM_PUBLISH_FPS)

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
            "!", "videorate",
            "!", f"video/x-raw,framerate={CAM_PUBLISH_FPS}/1",
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
                ts_ns = time.time_ns()
                with self._lock:
                    self._last_jpeg = jpeg
                _fused.put_cam(self.cam_id, jpeg, ts_ns)
                if _usb_available():
                    self._save_local(jpeg, ts_ns)
                else:
                    self._send_kafka(jpeg, ts_ns)
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

    def _save_local(self, jpeg: bytes, ts_ns: int):
        save_dir = _get_save_dir(self.name)
        if not save_dir:
            return
        try:
            with open(os.path.join(save_dir, f"{ts_ns}.jpg"), "wb") as f:
                f.write(jpeg)
        except Exception as e:
            log.warning("%s local save failed: %s", self.name, e)

    def _send_kafka(self, jpeg: bytes, ts_ns: int):
        value = struct.pack(">QI", ts_ns, len(jpeg)) + jpeg
        get_kafka().produce(
            topic=BROKER_TOPICS[self.name],
            value=value,
            key=f"{VEHICLE_ID}/{ts_ns}",
            headers={"vehicle_id": VEHICLE_ID, "sensor": self.name,
                     "ts_ns": str(ts_ns)},
        )


# ── BynavX1 FakeReceiver ──────────────────────────────────────
class FakeReceiver(threading.Thread):
    """
    TCP 연결 유지 전용.
    BynavX1은 TCP 클라이언트가 연결돼 있어야 WebSocket 데이터를 계속 전송함.
    실제 데이터 파싱은 BynavX1Proxy(WebSocket)에서 수행.
    """
    def __init__(self, ip: str, port: int):
        super().__init__(daemon=True)
        self.addr  = (ip, port)
        self._sock: Optional[socket.socket] = None
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        self.join(timeout=3)

    def run(self):
        buf_size = 1024 * 1024
        while not self._stop.is_set():
            if not self._sock:
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._sock.settimeout(1)
                try:
                    self._sock.connect(self.addr)
                    log.info("FakeReceiver connected  %s:%d", *self.addr)
                except Exception as e:
                    log.debug("FakeReceiver connect failed: %s", e)
                    self._sock = None
                    self._stop.wait(timeout=2)
                    continue
            try:
                self._sock.recv(buf_size)
            except socket.timeout:
                continue   # 연결 살아있음, 계속 대기
            except Exception as e:
                log.debug("FakeReceiver recv error: %s", e)
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None


# ── BynavX1 WebSocket Proxy ───────────────────────────────────
class BynavX1Proxy(threading.Thread):
    """
    ws://{ip}/webSocket 연결.
    #BESTPOSA / #BESTGNSSPOSA NMEA ASCII → lat/lon/postype/solstat 파싱.
    """
    def __init__(self, ip: str):
        super().__init__(daemon=True)
        self.url   = f"ws://{ip}/webSocket"
        self._ws:  Optional[websocket.WebSocket] = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._data: dict = {}
        self._ka_timer: Optional[threading.Timer] = None

    def stop(self):
        self._stop.set()
        if self._ka_timer:
            self._ka_timer.cancel()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        self.join(timeout=3)

    def has_data(self) -> bool:
        with self._lock:
            return bool(self._data)

    def get_data(self) -> dict:
        with self._lock:
            return dict(self._data)

    def run(self):
        while not self._stop.is_set():
            try:
                self._ws = websocket.WebSocket()
                self._ws.connect(self.url)
                log.info("BynavX1Proxy WebSocket connected: %s", self.url)

                # init 코드 전송
                self._ws.send("init " + str(round(random.random() * 4294967294 + 1)))
                self._start_keep_alive()

                while not self._stop.is_set():
                    try:
                        self._ws.settimeout(2.0)
                        msg = self._ws.recv()
                        parsed = self._parse(msg)
                        if parsed:
                            with self._lock:
                                self._data.update(parsed)
                    except websocket.WebSocketTimeoutException:
                        continue

            except Exception as e:
                log.warning("BynavX1Proxy error: %s  retry 2s", e)
                self._stop.wait(timeout=2)
            finally:
                if self._ka_timer:
                    self._ka_timer.cancel()
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
                self._ws = None

    def _start_keep_alive(self):
        if self._stop.is_set():
            return
        try:
            if self._ws:
                self._ws.send("ping")
        except Exception:
            pass
        self._ka_timer = threading.Timer(5, self._start_keep_alive)
        self._ka_timer.daemon = True
        self._ka_timer.start()

    def _parse(self, message: str) -> dict:
        try:
            begin = message.find("#")
            if begin == -1:
                begin = message.find("$")
            if begin == -1:
                return {}
            line = message[begin:]

            is_novatel = (line[0] == "#")
            if is_novatel:
                head, body = line.split(";", 1)
                nmea = head.split(",") + body.split(",")
            else:
                nmea = line.split(",")

            last = nmea.pop()
            if "*" not in last:
                return {}
            val, _ = last.split("*", 1)
            nmea.append(val)

            msg_type = nmea[0][1:]   # # 또는 $ 제거
            handler  = getattr(self, f"_msg_{msg_type}", None)
            if handler:
                return handler(nmea)
        except Exception:
            pass
        return {}

    # #BESTPOSA
    def _msg_BESTPOSA(self, nmea: list) -> dict:
        if len(nmea) < 25:
            return {}
        return self._parse_best(nmea)

    # #BESTGNSSPOSA
    def _msg_BESTGNSSPOSA(self, nmea: list) -> dict:
        if len(nmea) < 25:
            return {}
        return self._parse_best(nmea)

    @staticmethod
    def _parse_best(nmea: list) -> dict:
        try:
            return {
                "solstat": nmea[10],
                "postype": nmea[11],
                "lat":     float(nmea[12]),
                "lon":     float(nmea[13]),
                "height":  float(nmea[14]) if len(nmea) > 14 else 0.0,
                "latstd":  float(nmea[17]) if len(nmea) > 17 else 0.0,
                "lonstd":  float(nmea[18]) if len(nmea) > 18 else 0.0,
                "diffage": float(nmea[21]) if len(nmea) > 21 else 0.0,
                "solnsvs": int(nmea[24])   if len(nmea) > 24 else 0,
                "ts_ns":   time.time_ns(),
            }
        except (ValueError, IndexError):
            return {}


# ── GnssWorker ────────────────────────────────────────────────
class GnssWorker:
    """
    BynavX1 수신:
      FakeReceiver → TCP 1111 연결 유지
      BynavX1Proxy → WebSocket NMEA 파싱
      저장: USB 있음 → parquet / 없음 → Kafka
    """
    def __init__(self):
        self.name        = "gnss"
        self._running    = False
        self._thread:    Optional[threading.Thread] = None
        self._last_data: Optional[dict] = None
        self._proxy:     Optional[BynavX1Proxy] = None
        self._fake_recv: Optional[FakeReceiver] = None
        self._jsonl = JsonlAppender()

    @property
    def is_acquiring(self) -> bool:
        return self._running

    def get_snapshot(self) -> Optional[dict]:
        return self._last_data

    def start(self):
        if self._running:
            return
        self._running = True
        self._fake_recv = FakeReceiver(GNSS_SRC_IP, GNSS_TCP_PORT)
        self._fake_recv.start()
        self._proxy = BynavX1Proxy(GNSS_SRC_IP)
        self._proxy.start()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info("GnssWorker started  tcp=%s:%d  ws=ws://%s/webSocket",
                 GNSS_SRC_IP, GNSS_TCP_PORT, GNSS_SRC_IP)

    def stop(self):
        self._running = False
        if self._fake_recv:
            self._fake_recv.stop()
        if self._proxy:
            self._proxy.stop()
        self._jsonl.close()
        log.info("GnssWorker stopped")

    def _run(self):
        while self._running:
            try:
                if not self._proxy or not self._proxy.has_data():
                    time.sleep(0.1)
                    continue

                raw = self._proxy.get_data()
                data = {
                    "type":      "gnss_pos",
                    "ts_ns":     raw.get("ts_ns",   time.time_ns()),
                    "gps_ts":    raw.get("ts_ns",   time.time_ns()) / 1e9,
                    "latitude":  raw.get("lat",      0.0),
                    "longitude": raw.get("lon",      0.0),
                    "height":    raw.get("height",   0.0),
                    "vel_north": 0.0,
                    "vel_east":  0.0,
                    "vel_up":    0.0,
                    "roll":      0.0,
                    "pitch":     0.0,
                    "azimuth":   0.0,
                    "solstat":   raw.get("solstat",  ""),
                    "postype":   raw.get("postype",  ""),
                    "diffage":   raw.get("diffage",  0.0),
                    "solnsvs":   raw.get("solnsvs",  0),
                }
                self._last_data = data
                log.debug("GnssWorker: lat=%.6f lon=%.6f postype=%s",
                          data["latitude"], data["longitude"], data["postype"])

                _fused.put_gnss(data, data["ts_ns"])

                if _usb_available():
                    self._jsonl.append(data)
                else:
                    self._send_kafka(data)

                time.sleep(0.1)   # 10Hz

            except Exception as e:
                if self._running:
                    log.error("GnssWorker error: %s", e)
                time.sleep(0.5)

    def _send_kafka(self, data: dict):
        value = json.dumps(data, ensure_ascii=False).encode()
        get_kafka().produce(
            topic=BROKER_TOPICS["gnss"],
            value=value,
            key=f"{VEHICLE_ID}/{data['ts_ns']}",
            headers={"vehicle_id": VEHICLE_ID, "sensor": "gnss",
                     "ts_ns": str(data["ts_ns"])},
        )


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
            "cpu/storage": None,
        }

    async def ping(self, request, context):
        return pb.void()

    async def get_sensors(self, request, context):
        return pb.Sensors(list=SENSOR_NAMES)

    async def is_connected(self, request, context):
        name = request.name
        if name == "cpu/storage":
            usb   = _usb_available()
            state = pb.Connection.State.CONNECTED if usb else pb.Connection.State.DISCONNECTED
            return pb.Connection(name=name, state=state)
        w = self._workers.get(name)
        if w is None:
            return pb.Connection(name=name, state=pb.Connection.State.UNKNOWN)
        if name.startswith("cam"):
            ok    = os.path.exists(CAM_DEVICES[int(name[-1])])
            state = pb.Connection.State.CONNECTED if ok else pb.Connection.State.DISCONNECTED
        else:
            # gnss: WebSocket proxy 살아있는지
            ok    = (self._gnss._proxy is not None and self._gnss._proxy.is_alive())
            state = pb.Connection.State.CONNECTED if ok else pb.Connection.State.DISCONNECTED
        return pb.Connection(name=name, state=state)

    async def is_healthy(self, request, context):
        name = request.name
        if name == "cpu/storage":
            usb    = _usb_available()
            status = pb.Health.Status.GOOD if usb else pb.Health.Status.BAAD
            return pb.Health(name=name, status=status,
                             reason="" if usb else "USB not mounted")
        w = self._workers.get(name)
        if w is None:
            return pb.Health(name=name, status=pb.Health.Status.UNKNOWN,
                             reason="unknown sensor")
        if name.startswith("cam"):
            ok     = os.path.exists(CAM_DEVICES[int(name[-1])])
            status = pb.Health.Status.GOOD if ok else pb.Health.Status.BAAD
            reason = "" if ok else f"{CAM_DEVICES[int(name[-1])]} not found"
        else:
            if self._gnss._last_data is None:
                status = pb.Health.Status.WARN
                reason = "no gnss data yet"
            else:
                postype = self._gnss._last_data.get("postype", "")
                if postype in ("NARROW_INT", "INS_RTKFLOAT", "INS_RTKFIXED"):
                    status = pb.Health.Status.GOOD
                    reason = ""
                elif postype in ("SINGLE", "PSRDIFF", "WIDE_INT"):
                    status = pb.Health.Status.WARN
                    reason = f"postype={postype}"
                else:
                    status = pb.Health.Status.WARN
                    reason = f"postype={postype}" if postype else "no fix"
        return pb.Health(name=name, status=status, reason=reason)

    async def get_snapshot(self, request, context):
        name = request.name
        if name == "cpu/storage":
            return pb.SensorSnapshot(name=name, content_type="application/json",
                                     data=json.dumps(_usb_disk_usage()).encode())
        w = self._workers.get(name)
        if w is None:
            return pb.SensorSnapshot(name=name)
        if name.startswith("cam"):
            jpeg = w.get_snapshot()
            if jpeg:
                return pb.SensorSnapshot(name=name, content_type="image/jpeg", data=jpeg)
        else:
            data = w.get_snapshot()
            if data:
                return pb.SensorSnapshot(name=name, content_type="application/json",
                                         data=json.dumps(data).encode())
        return pb.SensorSnapshot(name=name)

    async def get_snapshots(self, request, context):
        snaps = [pb.SensorSnapshot(name="cpu/storage", content_type="application/json",
                                   data=json.dumps(_usb_disk_usage()).encode())]
        for name, w in self._workers.items():
            if name == "cpu/storage" or w is None:
                continue
            if name.startswith("cam"):
                jpeg = w.get_snapshot()
                if jpeg:
                    snaps.append(pb.SensorSnapshot(name=name,
                                                   content_type="image/jpeg", data=jpeg))
            else:
                data = w.get_snapshot()
                if data:
                    snaps.append(pb.SensorSnapshot(name=name,
                                                   content_type="application/json",
                                                   data=json.dumps(data).encode()))
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
        log.info("Acquisition started: %s → %s", name,
                 "local USB" if _usb_available() else "Kafka")
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


# ── main ──────────────────────────────────────────────────────
async def serve():
    server = aio.server()
    pb_grpc.add_ServiceServicer_to_server(DaqServicer(), server)
    server.add_insecure_port(f"0.0.0.0:{GRPC_PORT}")
    await server.start()
    _fused.start()
    log.info("daq-service listening  port=%d  fps=%d", GRPC_PORT, CAM_PUBLISH_FPS)
    log.info("USB_MOUNT_ROOT=%s  usb=%s", USB_MOUNT_ROOT, _usb_available())
    log.info("GNSS  tcp=%s:%d  ws=ws://%s/webSocket",
             GNSS_SRC_IP, GNSS_TCP_PORT, GNSS_SRC_IP)
    try:
        await server.wait_for_termination()
    finally:
        _fused.stop()
        get_kafka().close()


if __name__ == "__main__":
    asyncio.run(serve())