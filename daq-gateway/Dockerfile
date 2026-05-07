FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV GRPC_PORT=50050
ENV SERVICE_HOST=daq-service
ENV SERVICE_PORT=50051

EXPOSE 50050

CMD ["python", "server.py"]
