# daq-kafka-producer

DAQ Edge Server 배포 패키지  
카메라 3대 + GNSS 수집 → Remote Kafka Broker 전송

---

## 배포 구성

```
Edge Server
├── daq-service  (gRPC :50051)  센서 수집 + 브로커 전송
└── daq-gateway  (gRPC :50050)  tablet dashboard 통신 중계
```

---

## 시작하기

### 1. 설정 파일 수정

```
config/config.env
```

| 항목 | 설명 |
|------|------|
| `VEHICLE_ID` | 차량 식별자 |
| `CAM0/1/2_DEVICE` | 카메라 장치 경로 |
| `GNSS_SRC_IP` | GNSS 장비 IP |
| `GNSS_UDP_PORT` | GNSS UDP 수신 포트 |
| `BROKER_REST_URL` | 원격 Kafka REST Proxy URL |
| `GATEWAY_GRPC_PORT` | tablet이 접속하는 gRPC 포트 (기본 50050) |
| `REGISTRY` | Harbor 레지스트리 주소 (선택) |

### 2. 빌드

```bash
make build
```

### 3. 실행

```bash
make up
```

### 4. 상태 확인

```bash
make status
```

### 5. 중지

```bash
make down
```

---

## 로그 확인

```bash
make logs       # 전체
make log-svc    # daq-service (센서/브로커)
make log-gw     # daq-gateway (dashboard 통신)
```

---

## tablet dashboard 연결

`daq-kafka-dashboard` 앱의 설정에서:

- **서버 주소**: edge server IP
- **포트**: `50050` (GATEWAY_GRPC_PORT)

---

## Harbor 레지스트리 사용 시

```bash
# config/config.env 에 REGISTRY 설정 후
make push   # 이미지 업로드
make pull   # 이미지 다운로드 (다른 차량 배포)
```
