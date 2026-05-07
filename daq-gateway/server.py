"""
daq-gateway  –  gRPC Gateway server
 - daq-dashboard 의 요청을 받아 daq-service 로 relay
 - 여러 daq-service 인스턴스 지원 (현재는 단일)

env:
  GRPC_PORT          dashboard → gateway 수신 포트 (default 50050)
  SERVICE_HOST       daq-service 주소            (default daq-service)
  SERVICE_PORT       daq-service gRPC 포트        (default 50051)
"""

import os
import asyncio
import logging
from typing import List

import grpc
from grpc import aio

import daq_gateway_pb2 as gw_pb
import daq_gateway_pb2_grpc as gw_grpc
import daq_service_pb2 as svc_pb
import daq_service_pb2_grpc as svc_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("daq-gateway")

GRPC_PORT    = int(os.environ.get("GRPC_PORT",    "50050"))
SERVICE_HOST = os.environ.get("SERVICE_HOST", "daq-service")
SERVICE_PORT = int(os.environ.get("SERVICE_PORT", "50051"))


def _make_service_channel() -> aio.Channel:
    return aio.insecure_channel(f"{SERVICE_HOST}:{SERVICE_PORT}")


class GatewayServicer(gw_grpc.GatewayServicer):

    def _svc_stub(self, channel: aio.Channel) -> svc_grpc.ServiceStub:
        return svc_grpc.ServiceStub(channel)

    async def ping(self, request, context):
        async with _make_service_channel() as ch:
            await self._svc_stub(ch).ping(svc_pb.void())
        return gw_pb.void()

    async def get_sensors(self, request, context):
        async with _make_service_channel() as ch:
            resp = await self._svc_stub(ch).get_sensors(svc_pb.void())
        return gw_pb.Sensors(list=resp.list, reason=resp.reason)

    # ── Connection ────────────────────────────────────────────
    async def is_sensor_connected(self, request, context):
        async with _make_service_channel() as ch:
            resp = await self._svc_stub(ch).is_connected(
                svc_pb.Sensor(name=request.name))
        return gw_pb.Connection(name=resp.name, state=resp.state)

    async def is_all_sensor_connected(self, request, context):
        async with _make_service_channel() as ch:
            stub = self._svc_stub(ch)
            sensors = await stub.get_sensors(svc_pb.void())
            conns = []
            for name in sensors.list:
                r = await stub.is_connected(svc_pb.Sensor(name=name))
                conns.append(gw_pb.Connection(name=r.name, state=r.state))
        return gw_pb.Connections(list=conns)

    # ── Health ────────────────────────────────────────────────
    async def is_sensor_healthy(self, request, context):
        async with _make_service_channel() as ch:
            resp = await self._svc_stub(ch).is_healthy(
                svc_pb.Sensor(name=request.name))
        return gw_pb.Health(name=resp.name, status=resp.status, reason=resp.reason)

    async def is_all_sensor_healthy(self, request, context):
        async with _make_service_channel() as ch:
            stub = self._svc_stub(ch)
            sensors = await stub.get_sensors(svc_pb.void())
            healths = []
            for name in sensors.list:
                r = await stub.is_healthy(svc_pb.Sensor(name=name))
                healths.append(gw_pb.Health(name=r.name, status=r.status, reason=r.reason))
        return gw_pb.Healths(list=healths)

    # ── Snapshot ──────────────────────────────────────────────
    async def get_sensor_snapshot(self, request, context):
        async with _make_service_channel() as ch:
            resp = await self._svc_stub(ch).get_snapshot(
                svc_pb.Sensor(name=request.name))
        return gw_pb.SensorSnapshot(
            name=resp.name, content_type=resp.content_type, data=resp.data)

    async def get_all_sensor_snapshot(self, request, context):
        async with _make_service_channel() as ch:
            resp = await self._svc_stub(ch).get_snapshots(svc_pb.void())
        snaps = [
            gw_pb.SensorSnapshot(name=s.name, content_type=s.content_type, data=s.data)
            for s in resp.list
        ]
        return gw_pb.SensorSnapshots(list=snaps)

    # ── Acquisition ───────────────────────────────────────────
    async def is_sensor_acquiring(self, request, context):
        async with _make_service_channel() as ch:
            resp = await self._svc_stub(ch).is_acquiring(
                svc_pb.Sensor(name=request.name))
        return gw_pb.Acquisition(name=resp.name, state=resp.state, reason=resp.reason)

    async def is_all_sensor_acquiring(self, request, context):
        async with _make_service_channel() as ch:
            stub = self._svc_stub(ch)
            sensors = await stub.get_sensors(svc_pb.void())
            acqs = []
            for name in sensors.list:
                r = await stub.is_acquiring(svc_pb.Sensor(name=name))
                acqs.append(gw_pb.Acquisition(name=r.name, state=r.state, reason=r.reason))
        return gw_pb.Acquisitions(list=acqs)

    async def start_sensor_acquisition(self, request, context):
        async with _make_service_channel() as ch:
            resp = await self._svc_stub(ch).start_acquisition(
                svc_pb.Sensor(name=request.name))
        return gw_pb.Acquisition(name=resp.name, state=resp.state, reason=resp.reason)

    async def start_all_sensor_acquisition(self, request, context):
        async with _make_service_channel() as ch:
            stub = self._svc_stub(ch)
            sensors = await stub.get_sensors(svc_pb.void())
            acqs = []
            for name in sensors.list:
                r = await stub.start_acquisition(svc_pb.Sensor(name=name))
                acqs.append(gw_pb.Acquisition(name=r.name, state=r.state, reason=r.reason))
        return gw_pb.Acquisitions(list=acqs)

    async def stop_sensor_acquisition(self, request, context):
        async with _make_service_channel() as ch:
            resp = await self._svc_stub(ch).stop_acquisition(
                svc_pb.Sensor(name=request.name))
        return gw_pb.Acquisition(name=resp.name, state=resp.state, reason=resp.reason)

    async def stop_all_sensor_acquisition(self, request, context):
        async with _make_service_channel() as ch:
            stub = self._svc_stub(ch)
            sensors = await stub.get_sensors(svc_pb.void())
            acqs = []
            for name in sensors.list:
                r = await stub.stop_acquisition(svc_pb.Sensor(name=name))
                acqs.append(gw_pb.Acquisition(name=r.name, state=r.state, reason=r.reason))
        return gw_pb.Acquisitions(list=acqs)


async def serve():
    server = aio.server()
    gw_grpc.add_GatewayServicer_to_server(GatewayServicer(), server)
    listen_addr = f"0.0.0.0:{GRPC_PORT}"
    server.add_insecure_port(listen_addr)
    await server.start()
    log.info("daq-gateway gRPC listening on %s", listen_addr)
    log.info("→ forwarding to daq-service %s:%d", SERVICE_HOST, SERVICE_PORT)
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
