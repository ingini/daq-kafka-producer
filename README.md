# daq-kafka-producer

DAQ Edge Server 빌드 & 배포 패키지  
카메라 3대 + GNSS(BynavX1) 수집 → USB(SSD) 로컬 저장 또는 Remote Kafka Broker 전송

---

## 사전 요구사항

| 항목 | 설명 |
|------|------|
| `docker` | 이미지 빌드 및 컨테이너 실행 |
| `make` | 빌드 자동화 |

> Python, pip 등 별도 설치 불필요.  
> 모든 의존성은 `docker build` 시 이미지 안에 자동 설치됨.  
> `fused_frame_pb2.py` proto 컴파일도 빌드 시 자동 수행됨.

---

## 소스 구조

```
daq-kafka-producer/
├── Makefile
├── version
├── docker-compose.yml                ← build context: 상위(.) 기준
├── config/
│   └── config.env                    ← 설정 파일 (여기만 수정)
├── protos/
│   ├── daq_service.proto
│   ├── daq_gateway.proto
│   └── fused_frame.proto
├── daq-service/
│   ├── Dockerfile
│   ├── server.py
│   ├── fused_producer.py
│   ├── kafka_client.py
│   └── requirements.txt
└── daq-gateway/
    ├── Dockerfile
    ├── server.py
    └── requirements.txt
```

---

## GNSS 장비 연결 구조 (BynavX1)

```
BynavX1 (192.168.20.50)
  ├── TCP :1111    ← FakeReceiver 연결 유지 (데이터 버림)
  │                  TCP 연결이 있어야 WebSocket 데이터 전송됨
  └── WebSocket    ← BynavX1Proxy NMEA ASCII 수신
       #BESTPOSA / #BESTGNSSPOSA → lat/lon/postype/solstat 파싱
```

---

## USB 저장 감지 방식

`/proc/mounts` 기준으로 `/dev/sd*` 계열 블록 디바이스가 `USB_MOUNT_ROOT` 하위에 마운트된 경우만 USB로 인식.  
Docker bind mount 오탐 방지.

| 상황 | 감지 결과 |
|------|-----------|
| `/dev/sda1 on /media/swm/SSD` | USB 있음 ✓ |
| `/dev/nvme0n1p1 on /media/swm` | USB 없음 (내장 디스크 제외) |
| USB 미연결 | USB 없음 |

---

## 설정 (`config/config.env`)

| 항목 | 기본값 | 설명 |
|------|--------|------|
| `VEHICLE_ID` | `AP500L-001` | 차량 식별자 |
| `GNSS_SRC_IP` | `192.168.20.50` | BynavX1 IP |
| `GNSS_TCP_PORT` | `1111` | FakeReceiver TCP 포트 |
| `GNSS_ROLL_MINUTES` | `1` | GNSS JSONL 파일 롤링 주기 (분) |
| `USB_MOUNT_ROOT` | `/media/swm` | USB 마운트 루트 경로 |
| `CAM_PUBLISH_FPS` | `1` | 카메라 publish FPS |
| `BROKER_REST_URL` | - | Kafka REST Proxy URL (USB 없을 때 fallback) |
| `IMAGE_TAG` | `1.0.0` | 이미지 태그 |
| `CACHE_TTL` | `5.0` | gateway 캐시 만료 시간(초) |

---

## 빌드 & 배포

### 개발 PC에서 빌드

```bash
make ap500l
# → release/daq-ap500l-{version}/ 생성
```

### 엣지 서버 배포

```bash
scp -r release/daq-ap500l-1.0.0 swm@192.168.20.100:~/DAQ/daq-system
```

### 엣지 서버 실행

```bash
cd ~/DAQ/daq-system

# 이미지 로드 (최초 1회 또는 업데이트 시)
docker load -i daq-service.tar
docker load -i daq-gateway.tar

# .env 심볼릭 링크 (최초 1회)
ln -s config/config.env .env

# 서비스 시작/중지
docker compose up -d
docker compose down

# 로그
docker logs -f daq-service
docker logs -f daq-gateway
```

---

## 저장 경로

### USB 연결 시

```
{USB_MOUNT_ROOT}/{USB_NAME}/{yyyymmdd}/
├── cam0/{ts_ns}.jpg
├── cam1/{ts_ns}.jpg
├── cam2/{ts_ns}.jpg
├── imu/gnss_{ts_ns}.jsonl     ← GNSS_ROLL_MINUTES 분마다 새 파일
└── fused/{ts_ns}.bin          ← FusedFrame protobuf
```

예시:
```
/media/swm/SSD/20260515/
├── cam0/1778818793518202997.jpg
├── imu/gnss_1778818793000000000.jsonl
└── fused/1778818793518202997.bin
```

### JSONL 읽기

```python
import pandas as pd
df = pd.read_json("/media/swm/SSD/20260515/imu/gnss_xxx.jsonl", lines=True)
```

### USB 미연결 시

Kafka REST Proxy(`BROKER_REST_URL`)로 전송

---

## Kafka 토픽

| 토픽 | 포맷 | 내용 |
|------|------|------|
| `sensor.cam0.jpeg` | binary (8B ts + 4B len + JPEG) | cam0 프레임 |
| `sensor.cam1.jpeg` | binary | cam1 프레임 |
| `sensor.cam2.jpeg` | binary | cam2 프레임 |
| `sensor.gnss` | JSON | lat/lon/postype/solstat |
| `sensor.fused` | Protobuf (FusedFrame) | cam0~2 + GNSS timesync 묶음 |

### GNSS JSON 예시

```json
{
  "type": "gnss_pos",
  "ts_ns": 1778818793518202997,
  "latitude": 37.123456,
  "longitude": 127.123456,
  "height": 45.2,
  "solstat": "SOL_COMPUTED",
  "postype": "NARROW_INT",
  "diffage": 0.8,
  "solnsvs": 12
}
```

---

## gateway 상태 관리

- 백그라운드 1초 폴링 → 캐시 → dashboard 요청 시 즉시 반환
- `CACHE_TTL`(기본 5초) 초과 시 강제 DISCONNECTED/BAAD
- USB 뽑으면 ~6초 이내 앱에 반영

---

## Makefile 타겟

```bash
make ap500l       # 릴리즈 패키지 빌드
make ap500l-clean # 해당 버전 릴리즈 폴더 삭제
make clean-all    # 전체 release/ 삭제
make push         # Harbor push (REGISTRY 설정 필요)
```
