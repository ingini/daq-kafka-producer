"""
daq-gateway  –  gRPC Gateway server

기존 DAQ gateway 패턴 적용:
  - 동기 grpc.server + ThreadPoolExecutor
  - daq-service 채널 싱글톤 유지
  - DeviceReqBuffer: connection/health 백그라운드 1초마다 캐싱
  - snapshot/acquisition: 실시간 직접 호출

env:
  GRPC_PORT          (default 50050)
  SERVICE_HOST       (default 127.0.0.1)
  SERVICE_PORT       (default 50051)
  CALL_TIMEOUT       daq-service RPC timeout 초 (default 0.5)
  HEALTH_INTERVAL    백그라운드 체크 주기 초  (default 1.0)
"""

import os
import logging
import threading
import time
from concurrent import futures
from typing import Tuple, Optional

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

GRPC_PORT       = int(os.environ.get("GRPC_PORT",       "50050"))
SERVICE_HOST    = os.environ.get("SERVICE_HOST",    "127.0.0.1")
SERVICE_PORT    = int(os.environ.get("SERVICE_PORT",    "50051"))
CALL_TIMEOUT    = float(os.environ.get("CALL_TIMEOUT",  "0.5"))
HEALTH_INTERVAL = float(os.environ.get("HEALTH_INTERVAL", "1.0"))


# ── DeviceReqBuffer ───────────────────────────────────────────
class DeviceReqBuffer(threading.Thread):
    """
    백그라운드 스레드로 connection/health 를 주기적으로 캐싱.
    dashboard 요청 → 캐시값 즉시 반환 (network 왕복 없음).
    """

    def __init__(self, stub: svc_grpc.ServiceStub):
        super().__init__(daemon=True)
        self._stub    = stub
        self._stop_ev = threading.Event()

        # connection 캐시
        self._conn_lock = threading.Lock()
        self._conn: dict[str, svc_pb.Connection.State] = {}

        # health 캐시
        self._health_lock = threading.Lock()
        self._health: dict[str, Tuple] = {}

        # sensor 목록 캐시
        self._sensors: list[str] = []
        self._sensors_lock = threading.Lock()

    def stop(self):
        self._stop_ev.set()
        self.join(timeout=3)

    def run(self):
        while not self._stop_ev.is_set():
            try:
                self._refresh_sensors()
                self._refresh_connection()
                self._refresh_health()
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
        for name in sensors:
            try:
                resp = self._stub.is_connected(
                    svc_pb.Sensor(name=name), timeout=CALL_TIMEOUT)
                with self._conn_lock:
                    self._conn[name] = resp.state
            except grpc.RpcError:
                with self._conn_lock:
                    self._conn[name] = svc_pb.Connection.State.DISCONNECTED

    def _refresh_health(self):
        with self._sensors_lock:
            sensors = list(self._sensors)
        for name in sensors:
            try:
                resp = self._stub.is_healthy(
                    svc_pb.Sensor(name=name), timeout=CALL_TIMEOUT)
                with self._health_lock:
                    self._health[name] = (resp.status, resp.reason)
            except grpc.RpcError as e:
                with self._health_lock:
                    self._health[name] = (svc_pb.Health.Status.BAAD, str(e))

    # ── 캐시 조회 ─────────────────────────────────────────────
    def get_sensors(self) -> list[str]:
        with self._sensors_lock:
            return list(self._sensors)

    def get_connection(self, name: str) -> svc_pb.Connection.State:
        with self._conn_lock:
            return self._conn.get(name, svc_pb.Connection.State.UNKNOWN)

    def get_health(self, name: str) -> Tuple:
        with self._health_lock:
            return self._health.get(name,
                (svc_pb.Health.Status.UNKNOWN, "not yet checked"))


# ── GatewayServicer ───────────────────────────────────────────
class GatewayServicer(gw_grpc.GatewayServicer):

    def __init__(self,
                 stub: svc_grpc.ServiceStub,
                 buf:  DeviceReqBuffer):
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

    # ── Connection (캐시 반환) ────────────────────────────────
    def is_sensor_connected(self, request, context):
        state = self._buf.get_connection(request.name)
        return gw_pb.Connection(name=request.name, state=state)

    def is_all_sensor_connected(self, request, context):
        conns = [
            gw_pb.Connection(name=n, state=self._buf.get_connection(n))
            for n in self._buf.get_sensors()
        ]
        return gw_pb.Connections(list=conns)

    # ── Health (캐시 반환) ────────────────────────────────────
    def is_sensor_healthy(self, request, context):
        status, reason = self._buf.get_health(request.name)
        return gw_pb.Health(name=request.name, status=status, reason=reason)

    def is_all_sensor_healthy(self, request, context):
        healths = []
        for n in self._buf.get_sensors():
            status, reason = self._buf.get_health(n)
            healths.append(gw_pb.Health(name=n, status=status, reason=reason))
        return gw_pb.Healths(list=healths)

    # ── Snapshot (실시간 직접 호출) ───────────────────────────
    def get_sensor_snapshot(self, request, context):
        try:
            resp = self._stub.get_snapshot(
                svc_pb.Sensor(name=request.name))
            return gw_pb.SensorSnapshot(
                name=resp.name,
                content_type=resp.content_type,
                data=resp.data)
        except grpc.RpcError:
            return gw_pb.SensorSnapshot(name=request.name)

    def get_all_sensor_snapshot(self, request, context):
        try:
            resp = self._stub.get_snapshots(svc_pb.void())
            return gw_pb.SensorSnapshots(list=[
                gw_pb.SensorSnapshot(
                    name=s.name,
                    content_type=s.content_type,
                    data=s.data)
                for s in resp.list
            ])
        except grpc.RpcError:
            return gw_pb.SensorSnapshots(list=[])

    # ── Acquisition (실시간 직접 호출) ───────────────────────
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

    # 싱글톤 채널 — 서버 종료 전까지 유지
    channel = grpc.insecure_channel(svc_addr)
    stub    = svc_grpc.ServiceStub(channel)

    # 백그라운드 캐싱 스레드 시작
    buf = DeviceReqBuffer(stub)
    buf.start()
    log.info("DeviceReqBuffer started (interval=%.1fs)", HEALTH_INTERVAL)

    # 동기 gRPC 서버
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