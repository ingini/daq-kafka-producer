# ============================================================
#  daq-kafka-producer  Makefile
#
#  수정 가능한 파일:  config/config.env
#  사용법:
#    make build    - 이미지 빌드
#    make up       - 서비스 시작
#    make down     - 서비스 중지
#    make restart  - 재시작
#    make status   - 컨테이너 상태 확인
#    make logs     - 전체 로그
#    make log-svc  - daq-service 로그
#    make log-gw   - daq-gateway 로그
#    make push     - 레지스트리에 이미지 push (REGISTRY 설정 필요)
#    make pull     - 레지스트리에서 이미지 pull
#    make clean    - 이미지 및 컨테이너 정리
# ============================================================

.PHONY: all build up down restart status logs log-svc log-gw push pull clean help

CONFIG     := config/config.env
COMPOSE    := docker compose --env-file $(CONFIG)
DOCKER     := docker

# config.env 로드
include $(CONFIG)
export

# ── 기본 타겟 ─────────────────────────────────────────────────
all: help

# ── 빌드 ──────────────────────────────────────────────────────
build:
	@echo "[daq-kafka-producer] Building images..."
	$(COMPOSE) build --no-cache
	@echo "[daq-kafka-producer] Build complete."

# ── 실행 ──────────────────────────────────────────────────────
up:
	@echo "[daq-kafka-producer] Starting services (VEHICLE_ID=$(VEHICLE_ID))..."
	$(COMPOSE) up -d
	@echo "[daq-kafka-producer] Services started."
	@$(MAKE) -s status

# ── 중지 ──────────────────────────────────────────────────────
down:
	@echo "[daq-kafka-producer] Stopping services..."
	$(COMPOSE) down
	@echo "[daq-kafka-producer] Services stopped."

# ── 재시작 ────────────────────────────────────────────────────
restart:
	@$(MAKE) -s down
	@$(MAKE) -s up

# ── 상태 확인 ─────────────────────────────────────────────────
status:
	@echo "=== Container Status ==="
	@$(COMPOSE) ps
	@echo ""
	@echo "=== Gateway gRPC (:$(GATEWAY_GRPC_PORT)) ==="
	@python3 -c "import socket; s=socket.socket(); s.settimeout(2); \
	  r=s.connect_ex(('127.0.0.1',$(GATEWAY_GRPC_PORT))); s.close(); \
	  print('  LISTENING' if r==0 else '  NOT READY')" 2>/dev/null || echo "  python3 required"
	@echo "=== Service gRPC (:$(SERVICE_GRPC_PORT)) ==="
	@python3 -c "import socket; s=socket.socket(); s.settimeout(2); \
	  r=s.connect_ex(('127.0.0.1',$(SERVICE_GRPC_PORT))); s.close(); \
	  print('  LISTENING' if r==0 else '  NOT READY')" 2>/dev/null || echo "  python3 required"

# ── 로그 ──────────────────────────────────────────────────────
logs:
	$(COMPOSE) logs -f --tail=100

log-svc:
	$(DOCKER) logs -f daq-service --tail=100

log-gw:
	$(DOCKER) logs -f daq-gateway --tail=100

# ── 레지스트리 ────────────────────────────────────────────────
push:
ifndef REGISTRY
	$(error REGISTRY is not set in config/config.env)
endif
	@echo "[daq-kafka-producer] Pushing images to $(REGISTRY)..."
	$(DOCKER) tag daq-service:$(IMAGE_TAG) $(REGISTRY)/daq-service:$(IMAGE_TAG)
	$(DOCKER) tag daq-gateway:$(IMAGE_TAG) $(REGISTRY)/daq-gateway:$(IMAGE_TAG)
	$(DOCKER) push $(REGISTRY)/daq-service:$(IMAGE_TAG)
	$(DOCKER) push $(REGISTRY)/daq-gateway:$(IMAGE_TAG)
	@echo "[daq-kafka-producer] Push complete."

pull:
ifndef REGISTRY
	$(error REGISTRY is not set in config/config.env)
endif
	@echo "[daq-kafka-producer] Pulling images from $(REGISTRY)..."
	$(DOCKER) pull $(REGISTRY)/daq-service:$(IMAGE_TAG)
	$(DOCKER) pull $(REGISTRY)/daq-gateway:$(IMAGE_TAG)
	@echo "[daq-kafka-producer] Pull complete."

# ── 정리 ──────────────────────────────────────────────────────
clean:
	@echo "[daq-kafka-producer] Cleaning containers and images..."
	$(COMPOSE) down --rmi local --volumes --remove-orphans
	@echo "[daq-kafka-producer] Clean complete."

# ── 도움말 ────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  daq-kafka-producer  배포 도구"
	@echo "  ================================"
	@echo "  설정 파일: config/config.env"
	@echo ""
	@echo "  make build    이미지 빌드"
	@echo "  make up       서비스 시작"
	@echo "  make down     서비스 중지"
	@echo "  make restart  재시작"
	@echo "  make status   상태 확인"
	@echo "  make logs     전체 로그 (Ctrl+C 로 종료)"
	@echo "  make log-svc  daq-service 로그"
	@echo "  make log-gw   daq-gateway 로그"
	@echo "  make push     레지스트리 push"
	@echo "  make pull     레지스트리 pull"
	@echo "  make clean    이미지/컨테이너 정리"
	@echo ""
