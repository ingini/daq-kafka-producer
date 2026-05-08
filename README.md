# daq-kafka-producer

DAQ Edge Server 빌드 & 배포 패키지
카메라 3대 + GNSS 수집 → USB(SSD) 로컬 저장 또는 Remote Kafka Broker 전송

---

## 사전 요구사항

| 항목 | 설명 |
|------|------|
| `docker` | 이미지 빌드 및 컨테이너 실행 |
| `make` | 빌드 자동화 |

> Python, pip 등 별도 설치 불필요.
> 모든 의존성은 `docker build` 시 이미지 안에 자동 설치됨.

---

## 소스 구조 (개발 PC)

```
daq-kafka-producer/
├── Makefile                          ← 빌드 진입점
├── version                           ← 버전 번호
├── config/
│   └── config.env                    ← 설정 파일 (여기만 수정)
├── compose/
│   └── docker-compose-ap500l.yml
├── protos/                           ← gRPC proto 정의
├── daq-service/                      ← 센서 수집 서버 (소스 + Dockerfile)
└── daq-gateway/                      ← gRPC relay 서버 (소스 + Dockerfile)
```

---

## 빌드 & 배포 플로우

### 1. 설정 수정

```
config/config.env
```

| 항목 | 설명 |
|------|------|
| `VEHICLE_ID` | 차량 식별자 |
| `GNSS_SRC_IP` | GNSS 장비 IP |
| `USB_MOUNT_ROOT` | USB(SSD) 마운트 경로 (기본 `/media/usb`) |
| `BROKER_REST_URL` | Kafka REST Proxy URL (USB 없을 때 fallback) |
| `IMAGE_TAG` | 이미지 태그 (version 파일과 맞출 것) |

### 2. 릴리즈 패키지 빌드

```bash
make ap500l
```

결과물: `release/daq-ap500l-{version}/`

### 3. 엣지 서버에 배포

```bash
scp -r release/daq-ap500l-1.0.0 user@192.168.20.100:~/DAQ/daq-system
```

### 4. 엣지 서버에서 실행

```bash
cd ~/DAQ/daq-system

# 이미지 로드 (최초 1회)
docker load -i daq-service.tar
docker load -i daq-gateway.tar

# 서비스 시작
docker compose --env-file config/config.env up -d

# 상태 확인
docker compose --env-file config/config.env ps

# 로그
docker logs -f daq-service
docker logs -f daq-gateway

# 서비스 중지
docker compose --env-file config/config.env down
```

---

## release 폴더 구조

```
release/daq-ap500l-1.0.0/
├── docker-compose.yml
├── version
├── daq-service.tar        ← docker image (호스트 빌드 불필요)
├── daq-gateway.tar        ← docker image (호스트 빌드 불필요)
└── config/
    └── config.env         ← 엣지에서 추가 수정 가능
```

---

## 저장 모드

| 조건 | 동작 |
|------|------|
| USB(SSD) 연결됨 | `{USB_MOUNT_ROOT}/{yyyymmdd}/cam{n}/{ts_ns}.jpg` 로컬 저장<br>`{USB_MOUNT_ROOT}/{yyyymmdd}/imu/imu_{ts_ns}_{idx:03d}.parquet` |
| USB(SSD) 없음 | Kafka REST Proxy(`BROKER_REST_URL`)로 전송 |

dashboard의 Storage 위젯이 USB 연결 상태를 실시간으로 표시함.

---

## 버전 관리

```bash
echo "1.0.1" > version
# config/config.env 의 IMAGE_TAG 도 동일하게 수정
make ap500l
# → release/daq-ap500l-1.0.1/ 생성
```

---

## Makefile 타겟

```bash
make ap500l       # 릴리즈 패키지 빌드
make ap500l-clean # 해당 버전 릴리즈 폴더 삭제
make clean-all    # 전체 release/ 삭제
make push         # Harbor push (REGISTRY 설정 필요)
```
