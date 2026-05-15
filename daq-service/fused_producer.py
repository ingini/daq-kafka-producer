"""
fused_producer.py  –  FusedFrameProducer  (REST Proxy v3 / USB aware)

cam0/1/2 + GNSS 를 공통 sync_ts_ns 기준으로 묶어 FusedFrame protobuf 로 전송.

저장 분기:
  USB 있음 → {USB_MOUNT_ROOT}/{yyyymmdd}/fused/{ts_ns}.bin  (protobuf raw)
  USB 없음 → Kafka REST Proxy v3  sensor.fused 토픽

timesync 윈도우:
  _try_fuse() 호출 시점의 now를 sync_ts_ns 로 잡고,
  각 센서의 (now - capture_ns) 가 FUSED_WINDOW_MS 이내인 것만 묶음.
  초과한 센서는 경고 로그 후 제외. MIN_CAMS 미달이면 해당 주기 skip.

환경변수:
  USB_MOUNT_ROOT            server.py 에서 관리
  BROKER_TOPIC_FUSED        (default: sensor.fused)
  VEHICLE_ID
  FUSED_WINDOW_MS           허용 오차 ms  (default: 200)
  FUSED_INTERVAL_S          publish 주기 초  (default: 1.0)
  FUSED_MIN_CAMS            최소 카메라 수  (default: 1)
  FUSED_REQUIRE_GNSS        0: gnss 없어도 전송 / 1: gnss 없으면 skip  (default: 0)
  CAM_WIDTH / CAM_HEIGHT    CameraFrame 메타  (default: 640/360)
"""

import os
import time
import logging
import threading
from datetime import datetime
from typing import Dict, Optional, Tuple

from kafka_client import get_kafka

BROKER_TOPIC_FUSED = os.environ.get("BROKER_TOPIC_FUSED", "sensor.fused")
VEHICLE_ID         = os.environ.get("VEHICLE_ID",         "unknown")
USB_MOUNT_ROOT     = os.environ.get("USB_MOUNT_ROOT",     "/media/usb")
WINDOW_MS          = int(os.environ.get("FUSED_WINDOW_MS",    "200"))
FUSE_INTERVAL_S    = float(os.environ.get("FUSED_INTERVAL_S", "1.0"))
MIN_CAMS           = int(os.environ.get("FUSED_MIN_CAMS",      "1"))
REQUIRE_GNSS       = os.environ.get("FUSED_REQUIRE_GNSS", "0") == "1"
CAM_WIDTH          = int(os.environ.get("CAM_WIDTH",  "640"))
CAM_HEIGHT         = int(os.environ.get("CAM_HEIGHT", "360"))

log = logging.getLogger("fused-producer")


def _load_pb():
    try:
        import fused_frame_pb2 as pb
        return pb
    except ImportError:
        return None


def _find_usb_mount() -> Optional[str]:
    """server.py 와 동일한 USB 탐색 로직"""
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


class FusedFrameProducer:
    """
    외부 인터페이스:
      put_cam(cam_id, jpeg, capture_ns)   ← CameraWorker 에서 호출
      put_gnss(gnss_dict, capture_ns)     ← GnssWorker 에서 호출
      start() / stop()                    ← serve() 에서 호출
    """

    def __init__(self):
        self._pb = _load_pb()
        if self._pb is None:
            log.warning(
                "fused_frame_pb2 not found — proto 컴파일 필요:\n"
                "  python -m grpc_tools.protoc -I protos "
                "--python_out=daq-service protos/fused_frame.proto"
            )

        self._lock  = threading.Lock()
        self._cam:  Dict[int, Tuple[bytes, int]] = {}   # cam_id → (jpeg, capture_ns)
        self._gnss: Optional[Tuple[dict, int]]   = None # (gnss_dict, capture_ns)

        self._seq     = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ── put API ───────────────────────────────────────────────
    def put_cam(self, cam_id: int, jpeg: bytes, capture_ns: int):
        """USB/Kafka 분기 밖에서 항상 호출됨"""
        with self._lock:
            self._cam[cam_id] = (jpeg, capture_ns)

    def put_gnss(self, gnss: dict, capture_ns: int):
        """USB/Kafka 분기 밖에서 항상 호출됨"""
        with self._lock:
            self._gnss = (gnss, capture_ns)

    # ── 생명주기 ──────────────────────────────────────────────
    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._fuse_loop, daemon=True)
        self._thread.start()
        log.info(
            "FusedFrameProducer started  topic=%s  window=%dms  "
            "interval=%.1fs  min_cams=%d  require_gnss=%s",
            BROKER_TOPIC_FUSED, WINDOW_MS, FUSE_INTERVAL_S, MIN_CAMS, REQUIRE_GNSS,
        )

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)
        log.info("FusedFrameProducer stopped")

    # ── 핵심 루프 ─────────────────────────────────────────────
    def _fuse_loop(self):
        while self._running:
            time.sleep(FUSE_INTERVAL_S)
            try:
                self._try_fuse()
            except Exception as e:
                log.warning("fuse error: %s", e)

    def _try_fuse(self):
        if self._pb is None:
            return

        pb        = self._pb
        sync_ns   = time.time_ns()
        window_ns = WINDOW_MS * 1_000_000

        with self._lock:
            cam_snapshot  = dict(self._cam)
            gnss_snapshot = self._gnss

        # ── 카메라 수집 ──────────────────────────────────────
        cam_frames: list = []
        skew_ms: Dict[str, int] = {}

        for cam_id in range(3):
            entry = cam_snapshot.get(cam_id)
            if entry is None:
                continue
            jpeg, cap_ns = entry
            delta_ns = sync_ns - cap_ns
            if delta_ns > window_ns:
                log.warning("cam%d stale %.0fms > %dms — skip", cam_id, delta_ns / 1e6, WINDOW_MS)
                continue
            name = f"cam{cam_id}"
            skew_ms[name] = int(delta_ns / 1_000_000)
            cam_frames.append(pb.CameraFrame(
                sensor_name=name, capture_ns=cap_ns,
                jpeg=jpeg, width=CAM_WIDTH, height=CAM_HEIGHT,
            ))

        if len(cam_frames) < MIN_CAMS:
            log.debug("not enough cams (%d/%d) — skip", len(cam_frames), MIN_CAMS)
            return

        # ── GNSS 수집 ────────────────────────────────────────
        gnss_frame = None
        if gnss_snapshot is not None:
            g, cap_ns = gnss_snapshot
            delta_ns  = sync_ns - cap_ns
            if delta_ns > window_ns:
                log.warning("gnss stale %.0fms > %dms — excluded", delta_ns / 1e6, WINDOW_MS)
            else:
                skew_ms["gnss"] = int(delta_ns / 1_000_000)
                gnss_frame = pb.GnssFrame(
                    capture_ns=cap_ns,
                    gps_ts=g.get("gps_ts", 0.0),    fix_type=g.get("type", ""),
                    latitude=g.get("latitude", 0.0), longitude=g.get("longitude", 0.0),
                    height=g.get("height", 0.0),
                    vel_north=g.get("vel_north", 0.0), vel_east=g.get("vel_east", 0.0),
                    vel_up=g.get("vel_up", 0.0),
                    roll=g.get("roll", 0.0), pitch=g.get("pitch", 0.0),
                    azimuth=g.get("azimuth", 0.0),
                )

        if REQUIRE_GNSS and gnss_frame is None:
            log.debug("gnss required but absent/stale — skip")
            return

        # ── FusedFrame 직렬화 ─────────────────────────────────
        self._seq += 1
        fused = pb.FusedFrame(
            vehicle_id=VEHICLE_ID, sync_ts_ns=sync_ns, seq=self._seq,
            cameras=cam_frames, gnss=gnss_frame, skew_ms=skew_ms,
        )
        raw      = fused.SerializeToString()
        max_skew = max(abs(v) for v in skew_ms.values()) if skew_ms else 0

        # ── USB / Kafka 분기 ──────────────────────────────────
        mount = _find_usb_mount()
        if mount:
            self._save_local(raw, sync_ns, mount)
        else:
            self._send_kafka(raw, sync_ns, len(cam_frames), gnss_frame is not None, max_skew)

        log.debug(
            "fused seq=%d  cams=%d  gnss=%s  skew=%s  size=%d B  dst=%s",
            self._seq, len(cam_frames), "yes" if gnss_frame else "no",
            skew_ms, len(raw), "usb" if mount else "kafka",
        )

    def _save_local(self, raw: bytes, sync_ns: int, mount: str):
        save_dir = os.path.join(
            mount, datetime.now().strftime("%Y%m%d"), "fused"
        )
        os.makedirs(save_dir, exist_ok=True)
        path = os.path.join(save_dir, f"{sync_ns}.bin")
        try:
            with open(path, "wb") as f:
                f.write(raw)
        except Exception as e:
            log.warning("fused local save failed: %s", e)

    def _send_kafka(self, raw: bytes, sync_ns: int,
                    cam_count: int, has_gnss: bool, max_skew: int):
        get_kafka().produce(
            topic=BROKER_TOPIC_FUSED,
            value=raw,
            key=f"{VEHICLE_ID}/{sync_ns}",
            headers={
                "vehicle_id":   VEHICLE_ID,
                "cam_count":    str(cam_count),
                "has_gnss":     "1" if has_gnss else "0",
                "sync_skew_ms": str(max_skew),
            },
        )