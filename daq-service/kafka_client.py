"""
kafka_client.py  –  Kafka REST Proxy v3 공용 싱글톤 클라이언트

모든 producer(CameraWorker, GnssWorker, FusedFrameProducer)가 공유.

REST Proxy v3 endpoint:
  POST /v3/clusters/{cluster_id}/topics/{topic}/records
  Content-Type: application/json
  body = {
    "key":     {"type": "BINARY", "data": "<base64>"},
    "value":   {"type": "BINARY", "data": "<base64>"},
    "headers": [{"name": "<k>", "value": "<base64-of-str>"}, ...]
  }

환경변수:
  BROKER_REST_URL           e.g. https://221.147.232.196:8443/poc/kafka-rest
  BROKER_CLUSTER_ID         e.g. Some(red-poc-kraft-cluster)
  BROKER_REST_TLS_VERIFY    true | false  (default: true)
  VEHICLE_ID
"""

import base64
import json
import logging
import os
import threading
from typing import Dict, Optional

import httpx

log = logging.getLogger("kafka-client")

BROKER_REST_URL   = os.environ.get("BROKER_REST_URL",    "http://localhost:8082").rstrip("/")
BROKER_CLUSTER_ID = os.environ.get("BROKER_CLUSTER_ID",  "")
TLS_VERIFY        = os.environ.get("BROKER_REST_TLS_VERIFY", "true").lower() != "false"
VEHICLE_ID        = os.environ.get("VEHICLE_ID", "unknown")


def _b64s(s: str) -> str:
    return base64.b64encode(s.encode()).decode()

def _b64b(b: bytes) -> str:
    return base64.b64encode(b).decode()


class KafkaRestClient:
    def __init__(self):
        if not BROKER_CLUSTER_ID:
            log.warning("BROKER_CLUSTER_ID 미설정 — Kafka 전송 비활성화")

        self._client = httpx.Client(
            verify=TLS_VERIFY,
            timeout=30.0,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
        log.info(
            "KafkaRestClient  url=%s  cluster=%s  tls_verify=%s",
            BROKER_REST_URL, BROKER_CLUSTER_ID, TLS_VERIFY,
        )

    def _endpoint(self, topic: str) -> str:
        return f"{BROKER_REST_URL}/v3/clusters/{BROKER_CLUSTER_ID}/topics/{topic}/records"

    def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        if not BROKER_CLUSTER_ID:
            return False

        body: dict = {
            "key":   {"type": "BINARY", "data": _b64s(key or VEHICLE_ID)},
            "value": {"type": "BINARY", "data": _b64b(value)},
        }
        if headers:
            body["headers"] = [{"name": k, "value": _b64s(v)} for k, v in headers.items()]

        try:
            resp = self._client.post(
                self._endpoint(topic),
                content=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code in (200, 201, 202):
                return True
            log.warning("kafka POST %d  topic=%s  %s", resp.status_code, topic, resp.text[:300])
            return False
        except Exception as e:
            log.warning("kafka send failed  topic=%s  %s", topic, e)
            return False

    def close(self):
        self._client.close()


_kafka: Optional[KafkaRestClient] = None
_kafka_lock = threading.Lock()

def get_kafka() -> KafkaRestClient:
    global _kafka
    if _kafka is None:
        with _kafka_lock:
            if _kafka is None:
                _kafka = KafkaRestClient()
    return _kafka