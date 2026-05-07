#!/bin/bash
# ============================================================
#  run.sh
#  사용법: ./run.sh [up|down|restart|status|logs]
#  처음 실행 시 docker image 자동 load
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG="$SCRIPT_DIR/config/config.env"
COMPOSE="docker compose --env-file $CONFIG -f $SCRIPT_DIR/docker-compose.yml"

# image tar → docker load (아직 없을 경우만)
_load_images() {
    for TAR in "$SCRIPT_DIR"/daq-service.tar "$SCRIPT_DIR"/daq-gateway.tar; do
        if [ -f "$TAR" ]; then
            IMG=$(basename "$TAR" .tar)
            if ! docker image inspect "$IMG" > /dev/null 2>&1; then
                echo "[run] Loading image: $TAR"
                docker load -i "$TAR"
            fi
        fi
    done
}

CMD="${1:-up}"

case "$CMD" in
  up)
    _load_images
    echo "[run] Starting daq-system..."
    $COMPOSE up -d
    GW_PORT=$(grep GATEWAY_GRPC_PORT "$CONFIG" | cut -d= -f2 | tr -d ' ')
    echo "[run] Started. Gateway gRPC → :${GW_PORT:-50050}"
    ;;
  down)
    echo "[run] Stopping daq-system..."
    $COMPOSE down
    ;;
  restart)
    $COMPOSE down
    _load_images
    $COMPOSE up -d
    ;;
  status)
    $COMPOSE ps
    ;;
  logs)
    $COMPOSE logs -f --tail=100
    ;;
  log-svc)
    docker logs -f daq-service --tail=100
    ;;
  log-gw)
    docker logs -f daq-gateway --tail=100
    ;;
  *)
    echo "Usage: $0 [up|down|restart|status|logs|log-svc|log-gw]"
    exit 1
    ;;
esac
