#!/bin/bash
# ============================================================
#  start.sh  — DAQ 수집 시작
#  사용법: ./start.sh
# ============================================================

GATEWAY_HOST=${GATEWAY_HOST:-127.0.0.1}
GATEWAY_PORT=${GATEWAY_PORT:-50050}

echo "[DAQ] Starting acquisition on ${GATEWAY_HOST}:${GATEWAY_PORT}..."

docker exec daq-gateway python3 -c "
import grpc
import sys
sys.path.insert(0, '/app')
import daq_gateway_pb2 as gw_pb
import daq_gateway_pb2_grpc as gw_grpc

try:
    channel = grpc.insecure_channel('${GATEWAY_HOST}:${GATEWAY_PORT}')
    stub = gw_grpc.GatewayStub(channel)
    resp = stub.start_all_sensor_acquisition(gw_pb.void_(), timeout=5)
    for acq in resp.list:
        print(f'  {acq.name}: {acq.state} {acq.reason}')
    channel.close()
    print('[DAQ] Acquisition started.')
except Exception as e:
    print(f'[DAQ] Error: {e}')
    sys.exit(1)
"