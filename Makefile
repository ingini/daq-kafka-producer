# ============================================================
#  daq-kafka-producer  Makefile
#
#  사용법 (개발 PC):
#    make ap500l          릴리즈 패키지 빌드
#    make ap500l-clean    릴리즈 폴더 삭제
#    make clean-all       전체 release/ 삭제
#    make push            Harbor push
#
#  엣지 서버에서는 Makefile 불필요.
#  release 폴더를 ~/DAQ/daq-system 으로 복사 후:
#    cd ~/DAQ/daq-system
#    docker load -i daq-service.tar
#    docker load -i daq-gateway.tar
#    docker compose --env-file config/config.env up -d
# ============================================================

TARGET  := ap500l
VERSION := $(shell cat version 2>/dev/null || echo "1.0.0")

RELEASE_DIR := release/daq-$(TARGET)-$(VERSION)
CONFIG_SRC  := config/config.env
COMPOSE_SRC := compose/docker-compose-$(TARGET).yml
IMAGE_TAG   := $(VERSION)

.PHONY: all help \
        ap500l ap500l-clean clean-all push \
        _release-dir _build-service _build-gateway \
        _copy-config _copy-compose

all: help

# ============================================================
#  ap500l 빌드
# ============================================================
ap500l: _release-dir _build-service _build-gateway _copy-config _copy-compose
	@echo ""
	@echo "================================================="
	@echo "  Release → $(RELEASE_DIR)/"
	@echo ""
	@echo "  배포:"
	@echo "    scp -r $(RELEASE_DIR) user@edge:~/DAQ/daq-system"
	@echo ""
	@echo "  엣지 서버 실행:"
	@echo "    docker load -i daq-service.tar"
	@echo "    docker load -i daq-gateway.tar"
	@echo "    docker compose --env-file config/config.env up -d"
	@echo "================================================="

_release-dir:
	@mkdir -p $(RELEASE_DIR)/config

_build-service:
	@echo "[1/2] Building daq-service:$(IMAGE_TAG)..."
	@docker build -t daq-service:$(IMAGE_TAG) ./daq-service
	@docker save daq-service:$(IMAGE_TAG) -o $(RELEASE_DIR)/daq-service.tar
	@echo "      → $(RELEASE_DIR)/daq-service.tar"

_build-gateway:
	@echo "[2/2] Building daq-gateway:$(IMAGE_TAG)..."
	@docker build -t daq-gateway:$(IMAGE_TAG) ./daq-gateway
	@docker save daq-gateway:$(IMAGE_TAG) -o $(RELEASE_DIR)/daq-gateway.tar
	@echo "      → $(RELEASE_DIR)/daq-gateway.tar"

_copy-config:
	@cp $(CONFIG_SRC) $(RELEASE_DIR)/config/config.env

_copy-compose:
	@cp $(COMPOSE_SRC) $(RELEASE_DIR)/docker-compose.yml
	@cp version        $(RELEASE_DIR)/version

# ── 정리 ──────────────────────────────────────────────────────
ap500l-clean:
	@rm -rf $(RELEASE_DIR)
	@echo "Removed $(RELEASE_DIR)"

clean-all:
	@rm -rf release/
	@echo "Removed release/"

# ── Harbor push ───────────────────────────────────────────────
push:
ifndef REGISTRY
	$(error REGISTRY is not set. Edit config/config.env)
endif
	@. $(CONFIG_SRC); \
	docker tag daq-service:$(IMAGE_TAG)  $${REGISTRY}/daq-service:$(IMAGE_TAG); \
	docker tag daq-gateway:$(IMAGE_TAG)  $${REGISTRY}/daq-gateway:$(IMAGE_TAG); \
	docker push $${REGISTRY}/daq-service:$(IMAGE_TAG); \
	docker push $${REGISTRY}/daq-gateway:$(IMAGE_TAG)
	@echo "Push done."

# ── 도움말 ────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  daq-kafka-producer  (version: $(VERSION))"
	@echo "  ----------------------------------------"
	@echo "  make ap500l       릴리즈 빌드 → $(RELEASE_DIR)/"
	@echo "  make ap500l-clean 릴리즈 폴더 삭제"
	@echo "  make clean-all    전체 release/ 삭제"
	@echo "  make push         Harbor push (REGISTRY 설정 필요)"
	@echo ""
	@echo "  수정 파일: config/config.env"
	@echo "  버전 변경: echo '1.0.1' > version"
	@echo ""
