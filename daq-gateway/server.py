"""
daq-gateway  –  gRPC Gateway server

기존 DAQ gateway 패턴 적용:
  - 동기 grpc.server + ThreadPoolExecutor
  - daq-service 채널 싱글톤 유지
  - DeviceReqBuffer: connection/health/snapshot 백그라운드 캐싱
    → dashboard 요청 오면 캐시 즉시 반환 (Wi-Fi 지연 영향 없음)
  - 캐시 TTL(CACHE_TTL초) 초과 시 DISCONNECTED/BAAD 강제 반환
    → USB 뽑거나 센서 끊기면 앱에 즉시 반영
  - acquisition: 실시간 직접 호출

env:
  GRPC_PORT          (default 50050)
  SERVICE_HOST       (default 127.0.0.1)
  SERVICE_PORT       (default 50051)
  CALL_TIMEOUT       daq-service RPC timeout 초 (default 3.0)
  HEALTH_INTERVAL    백그라운드 체크 주기 초  (default 1.0)
  SNAPSHOT_INTERVAL  스냅샷 캐싱 주기 초     (default 1.0)
  CACHE_TTL          캐시 만료 시간 초       (default 5.0)
                     이 시간 동안 갱신 없으면 DISCONNECTED/BAAD 반환
"""

import os
import logging
import threading
import time
from concurrent import futures
from typing import Tuple, Optional, Dict

import grpc

import daq_gateway_pb2 as gw_pb
import daq_gateway_pb2_grpc as gw_grpc
import daq_service_pb2 as svc_pb
import daq_service_pb2_grpc as svc_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("daq-gateway")

GRPC_PORT         = int(os.environ.get("GRPC_PORT",         "50050"))
SERVICE_HOST      = os.environ.get("SERVICE_HOST",      "127.0.0.1")
SERVICE_PORT      = int(os.environ.get("SERVICE_PORT",      "50051"))
CALL_TIMEOUT      = float(os.environ.get("CALL_TIMEOUT",    "3.0"))
HEALTH_INTERVAL   = float(os.environ.get("HEALTH_INTERVAL", "1.0"))
SNAPSHOT_INTERVAL = float(os.environ.get("SNAPSHOT_INTERVAL", "1.0"))


# ── DeviceReqBuffer ───────────────────────────────────────────
class DeviceReqBuffer(threading.Thread):
    """
    백그라운드 스레드로 connection/health/snapshot 주기적 캐싱.
    dashboard 요청 → 캐시값 즉시 반환 (Wi-Fi 지연 영향 없음).

    캐시 TTL:
      - 마지막 갱신으로부터 CACHE_TTL초 초과 시 DISCONNECTED/BAAD 강제 반환
      - 초기(한 번도 갱신 안 된 상태)도 DISCONNECTED/BAAD 반환
      → USB 뽑거나 daq-service 죽으면 앱에 즉시 반영
    """

    def __init__(self, stub: svc_grpc.ServiceStub):
        super().__init__(daemon=True)
        self._stub    = stub
        self._stop_ev = threading.Event()

        # 캐시 만료 시간: 이 시간 이상 갱신 없으면 DISCONNECTED/BAAD 반환
        self._cache_ttl = float(os.environ.get("CACHE_TTL", "5.0"))

        self._sensors_lock = threading.Lock()
        self._sensors: list = []

        self._conn_lock = threading.Lock()
        self._conn: Dict[str, int] = {}
        self._conn_ts: Dict[str, float] = {}    # 마지막 갱신 시각

        self._health_lock = threading.Lock()
        self._health: Dict[str, Tuple] = {}
        self._health_ts: Dict[str, float] = {}  # 마지막 갱신 시각

        self._snap_lock = threading.Lock()
        self._snap: Dict[str, Tuple] = {}       # name → (content_type, data)

        self._last_snap_time = 0.0

    def stop(self):
        self._stop_ev.set()
        self.join(timeout=3)

    def run(self):
        while not self._stop_ev.is_set():
            try:
                self._refresh_sensors()
                self._refresh_connection()
                self._refresh_health()
                now = time.time()
                if now - self._last_snap_time >= SNAPSHOT_INTERVAL:
                    self._refresh_snapshots()
                    self._last_snap_time = now
            except Exception as e:
                log.warning("DeviceReqBuffer refresh error: %s", e)
            self._stop_ev.wait(timeout=HEALTH_INTERVAL)

    def _refresh_sensors(self):
        try:
            resp = self._stub.get_sensors(svc_pb.void(), timeout=CALL_TIMEOUT)
            with self._sensors_lock:
                self._sensors = list(resp.list)
        except grpc.RpcError:
            pass

    def _refresh_connection(self):
        with self._sensors_lock:
            sensors = list(self._sensors)
        now = time.time()
        for name in sensors:
            try:
                resp = self._stub.is_connected(
                    svc_pb.Sensor(name=name), timeout=CALL_TIMEOUT)
                with self._conn_lock:
                    self._conn[name]    = resp.state
                    self._conn_ts[name] = now
            except grpc.RpcError:
                # RPC 실패도 DISCONNECTED로 기록 + ts 갱신
                with self._conn_lock:
                    self._conn[name]    = svc_pb.Connection.State.DISCONNECTED
                    self._conn_ts[name] = now

    def _refresh_health(self):
        with self._sensors_lock:
            sensors = list(self._sensors)
        now = time.time()
        for name in sensors:
            try:
                resp = self._stub.is_healthy(
                    svc_pb.Sensor(name=name), timeout=CALL_TIMEOUT)
                with self._health_lock:
                    self._health[name]    = (resp.status, resp.reason)
                    self._health_ts[name] = now
            except grpc.RpcError as e:
                # RPC 실패 시 ts를 갱신하지 않음
                # → TTL이 만료되면 get_health()가 BAAD("stale") 반환하고
                #   다음 주기에 재시도하므로 BAAD 고착 방지
                with self._health_lock:
                    self._health[name] = (svc_pb.Health.Status.BAAD, str(e))
                log.debug("_refresh_health RPC failed for %s: %s", name, e)

    def _refresh_snapshots(self):
        try:
            resp = self._stub.get_snapshots(svc_pb.void(), timeout=CALL_TIMEOUT)
            with self._snap_lock:
                for s in resp.list:
                    self._snap[s.name] = (s.content_type, s.data)
        except grpc.RpcError:
            pass

    # ── 캐시 조회 ─────────────────────────────────────────────
    def get_sensors(self) -> list:
        with self._sensors_lock:
            return list(self._sensors)

    def get_connection(self, name: str) -> int:
        with self._conn_lock:
            ts  = self._conn_ts.get(name, 0.0)
            val = self._conn.get(name, svc_pb.Connection.State.DISCONNECTED)
        # TTL 만료(초기 포함) → DISCONNECTED
        if time.time() - ts > self._cache_ttl:
            return svc_pb.Connection.State.DISCONNECTED
        return val

    def get_health(self, name: str) -> Tuple:
        with self._health_lock:
            ts  = self._health_ts.get(name, 0.0)
            val = self._health.get(name, (svc_pb.Health.Status.BAAD, "not yet checked"))
        # TTL 만료(초기 포함) → BAAD
        if time.time() - ts > self._cache_ttl:
            return (svc_pb.Health.Status.BAAD, "stale: no recent data")
        return val

    def get_snapshot(self, name: str) -> Tuple:
        with self._snap_lock:
            return self._snap.get(name, ("", b""))

    def get_all_snapshots(self) -> Dict[str, Tuple]:
        with self._snap_lock:
            return dict(self._snap)


# ── GatewayServicer ───────────────────────────────────────────
class GatewayServicer(gw_grpc.GatewayServicer):

    def __init__(self, stub: svc_grpc.ServiceStub, buf: DeviceReqBuffer):
        self._stub = stub
        self._buf  = buf

    # ── ping ──────────────────────────────────────────────────
    def ping(self, request, context):
        try:
            self._stub.ping(svc_pb.void(), timeout=CALL_TIMEOUT)
        except grpc.RpcError:
            pass
        return gw_pb.void_()

    # ── sensors ───────────────────────────────────────────────
    def get_sensors(self, request, context):
        return gw_pb.Sensors(list=self._buf.get_sensors())

    # ── Connection (캐시, TTL 적용) ───────────────────────────
    def is_sensor_connected(self, request, context):
        state = self._buf.get_connection(request.name)
        return gw_pb.Connection(name=request.name, state=state)

    def is_all_sensor_connected(self, request, context):
        conns = [
            gw_pb.Connection(name=n, state=self._buf.get_connection(n))
            for n in self._buf.get_sensors()
        ]
        return gw_pb.Connections(list=conns)

    # ── Health (캐시, TTL 적용) ───────────────────────────────
    def is_sensor_healthy(self, request, context):
        status, reason = self._buf.get_health(request.name)
        return gw_pb.Health(name=request.name, status=status, reason=reason)

    def is_all_sensor_healthy(self, request, context):
        healths = []
        for n in self._buf.get_sensors():
            status, reason = self._buf.get_health(n)
            healths.append(gw_pb.Health(name=n, status=status, reason=reason))
        return gw_pb.Healths(list=healths)

    # ── Snapshot (캐시) ───────────────────────────────────────
    def get_sensor_snapshot(self, request, context):
        content_type, data = self._buf.get_snapshot(request.name)
        return gw_pb.SensorSnapshot(
            name=request.name,
            content_type=content_type,
            data=data
        )

    def get_all_sensor_snapshot(self, request, context):
        snaps = []
        for name, (content_type, data) in self._buf.get_all_snapshots().items():
            if data:
                snaps.append(gw_pb.SensorSnapshot(
                    name=name,
                    content_type=content_type,
                    data=data
                ))
        return gw_pb.SensorSnapshots(list=snaps)

    # ── Acquisition (실시간 직접 호출) ────────────────────────
    def is_sensor_acquiring(self, request, context):
        try:
            resp = self._stub.is_acquiring(
                svc_pb.Sensor(name=request.name), timeout=CALL_TIMEOUT)
            return gw_pb.Acquisition(
                name=resp.name, state=resp.state, reason=resp.reason)
        except grpc.RpcError:
            return gw_pb.Acquisition(
                name=request.name, state=gw_pb.Acquisition.State.UNKNOWN)

    def is_all_sensor_acquiring(self, request, context):
        acqs = []
        for name in self._buf.get_sensors():
            try:
                r = self._stub.is_acquiring(
                    svc_pb.Sensor(name=name), timeout=CALL_TIMEOUT)
                acqs.append(gw_pb.Acquisition(
                    name=r.name, state=r.state, reason=r.reason))
            except grpc.RpcError:
                acqs.append(gw_pb.Acquisition(
                    name=name, state=gw_pb.Acquisition.State.UNKNOWN))
        return gw_pb.Acquisitions(list=acqs)

    def start_sensor_acquisition(self, request, context):
        try:
            resp = self._stub.start_acquisition(
                svc_pb.Sensor(name=request.name), timeout=CALL_TIMEOUT)
            log.info("Acquisition started: %s", request.name)
            return gw_pb.Acquisition(
                name=resp.name, state=resp.state, reason=resp.reason)
        except grpc.RpcError as e:
            log.error("start_acquisition failed: %s", e)
            return gw_pb.Acquisition(
                name=request.name, state=gw_pb.Acquisition.State.UNKNOWN,
                reason=str(e))

    def start_all_sensor_acquisition(self, request, context):
        acqs = []
        for name in self._buf.get_sensors():
            try:
                r = self._stub.start_acquisition(
                    svc_pb.Sensor(name=name), timeout=CALL_TIMEOUT)
                acqs.append(gw_pb.Acquisition(
                    name=r.name, state=r.state, reason=r.reason))
                log.info("Acquisition started: %s", name)
            except grpc.RpcError as e:
                acqs.append(gw_pb.Acquisition(
                    name=name, state=gw_pb.Acquisition.State.UNKNOWN,
                    reason=str(e)))
        return gw_pb.Acquisitions(list=acqs)

    def stop_sensor_acquisition(self, request, context):
        try:
            resp = self._stub.stop_acquisition(
                svc_pb.Sensor(name=request.name), timeout=CALL_TIMEOUT)
            log.info("Acquisition stopped: %s", request.name)
            return gw_pb.Acquisition(
                name=resp.name, state=resp.state, reason=resp.reason)
        except grpc.RpcError as e:
            log.error("stop_acquisition failed: %s", e)
            return gw_pb.Acquisition(
                name=request.name, state=gw_pb.Acquisition.State.UNKNOWN,
                reason=str(e))

    def stop_all_sensor_acquisition(self, request, context):
        acqs = []
        for name in self._buf.get_sensors():
            try:
                r = self._stub.stop_acquisition(
                    svc_pb.Sensor(name=name), timeout=CALL_TIMEOUT)
                acqs.append(gw_pb.Acquisition(
                    name=r.name, state=r.state, reason=r.reason))
                log.info("Acquisition stopped: %s", name)
            except grpc.RpcError as e:
                acqs.append(gw_pb.Acquisition(
                    name=name, state=gw_pb.Acquisition.State.UNKNOWN,
                    reason=str(e)))
        return gw_pb.Acquisitions(list=acqs)


# ── main ──────────────────────────────────────────────────────
def serve():
    svc_addr = f"{SERVICE_HOST}:{SERVICE_PORT}"
    channel  = grpc.insecure_channel(svc_addr)
    stub     = svc_grpc.ServiceStub(channel)

    buf = DeviceReqBuffer(stub)
    buf.start()
    log.info("DeviceReqBuffer started (health=%.1fs snapshot=%.1fs cache_ttl=%.1fs)",
             HEALTH_INTERVAL, SNAPSHOT_INTERVAL, buf._cache_ttl)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gw_grpc.add_GatewayServicer_to_server(GatewayServicer(stub, buf), server)
    server.add_insecure_port(f"0.0.0.0:{GRPC_PORT}")
    server.start()
    log.info("daq-gateway gRPC listening on 0.0.0.0:%d", GRPC_PORT)
    log.info("→ forwarding to daq-service %s", svc_addr)

    try:
        server.wait_for_termination()
    finally:
        buf.stop()
        channel.close()
        log.info("daq-gateway shutdown complete")


if __name__ == "__main__":
    serve()
