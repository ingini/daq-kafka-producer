FROM python:3.11-slim

# GStreamer 의존성 (실제 차량 이미지에는 이미 설치돼 있을 것)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-libav \
    libgstreamer1.0-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# proto 컴파일된 pb2 파일 + 서버
COPY . .

ENV GRPC_PORT=50051
ENV CAM0_DEVICE=/dev/video0
ENV CAM1_DEVICE=/dev/video1
ENV CAM2_DEVICE=/dev/video2
ENV CAM_WIDTH=640
ENV CAM_HEIGHT=360
ENV JPEG_QUALITY=90
ENV GNSS_UDP_PORT=1111
ENV GNSS_SRC_IP=192.168.20.50
ENV BROKER_REST_URL=http://localhost:8082
ENV BROKER_TOPIC_GNSS=sensor.gnss

EXPOSE 50051

CMD ["python", "server.py"]
