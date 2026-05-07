# daq-kafka-producer

DAQ Edge Server 빌드 & 배포 패키지
카메라 3대 + GNSS 수집 → Remote Kafka Broker 전송

---

## 소스 구조 (개발 PC)

```
daq-kafka-producer/
├── Makefile
├── version                          ← 버전 번호
├── config/
│   └── config.env                   ← 설정 파일 (여기만 수정)
├── compose/
│   └── docker-compose-ap500l.yml
├── static/ap500l/
│   ├── install.sh
│   └── run.sh
├── daq-service/                     (소스 + Dockerfile)
└── daq-gateway/                     (소스 + Dockerfile)
```

---

## 빌드 & 배포 플로우

### 1. 설정 수정

```
config/config.env
```

| 항목 | 설명 |
|------|------|
| `BROKER_REST_URL` | 원격 Kafka REST Proxy URL |
| `GNSS_SRC_IP` | GNSS 장비 IP |
| `VEHICLE_ID` | 차량 식별자 |

### 2. 릴리즈 패키지 빌드

```bash
make ap500l
```

결과물: `release/daq-ap500l-{version}/`

### 3. 엣지 서버에 배포

```bash
# 방법 A: scp 로 복사 후 설치
scp -r release/daq-ap500l-1.0.0 user@192.168.20.100:~/DAQ
ssh user@192.168.20.100 "cd ~/DAQ/daq-ap500l-1.0.0 && ./install.sh"

# 방법 B: 직접 복사
cp -r release/daq-ap500l-1.0.0 ~/DAQ/daq-system
```

### 4. 엣지 서버에서 실행

```bash
cd ~/DAQ/daq-system
./run.sh          # 시작
./run.sh status   # 상태
./run.sh logs     # 로그
./run.sh down     # 중지
```

---

## release 폴더 구조

```
release/daq-ap500l-1.0.0/
├── docker-compose.yml
├── install.sh          → ~/DAQ/daq-system 으로 설치
├── run.sh              → 서비스 시작/중지
├── version
├── daq-service.tar     ← docker image
├── daq-gateway.tar     ← docker image
└── config/
    └── config.env      ← 엣지에서 추가 수정 가능
```

이미지를 tar 로 export 하기 때문에 엣지 서버에서 별도 빌드 불필요.

---

## 버전 관리

```bash
echo "1.0.1" > version
make ap500l
# → release/daq-ap500l-1.0.1/ 생성
```
