FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.9 \
    python3-pip \
    python3.9-dev \
    cmake \
    build-essential \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-libav \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libv4l-dev \
    v4l-utils \
    && rm -rf /var/lib/apt/lists/*
RUN ln -sf /usr/bin/python3.9 /usr/bin/python3 && \
    ln -sf /usr/bin/python3 /usr/bin/python
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY . .
# protos/ 는 Makefile _build-service 에서 빌드 전 임시 복사됨
RUN python3 -m grpc_tools.protoc \
    -I protos \
    --python_out=. \
    protos/fused_frame.proto
ENV GRPC_PORT=50051
ENV CAM0_DEVICE=/dev/video0
ENV CAM1_DEVICE=/dev/video1
ENV CAM2_DEVICE=/dev/video2
ENV CAM_WIDTH=640
ENV CAM_HEIGHT=360
ENV JPEG_QUALITY=90
ENV GNSS_TCP_PORT=1111
ENV GNSS_SRC_IP=192.168.20.50
ENV USB_MOUNT_ROOT=/media/swm
ENV BROKER_REST_URL=http://localhost:8082
ENV BROKER_TOPIC_GNSS=sensor.gnss
EXPOSE 50051
CMD ["python3", "server.py"]