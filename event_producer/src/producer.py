from __future__ import annotations

import json
import logging
from typing import Any

from confluent_kafka import Producer

from event_producer.src.config import AppConfig


logger = logging.getLogger(__name__)


class EventProducer:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._producer = Producer(config.kafka.producer_config)

    def _delivery_report(self, err: Exception | None, msg) -> None:  # type: ignore
        if err is not None:
            logger.error(
                "Message delivery failed",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "error": str(err),
                },
            )
        else:
            logger.debug(
                "Message delivered",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )

    def _serialize(self, payload: dict[str, Any]) -> bytes:
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    def publish(self, topic: str, payload: dict[str, Any]) -> None:
        if "partition_key" not in payload:
            raise ValueError("Payload missing required field: partition_key")

        key = str(payload["partition_key"]).encode("utf-8")
        value = self._serialize(payload)

        self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=self._delivery_report,
        )

        self._producer.poll(0)

    def publish_user_activity(self, payload: dict[str, Any]) -> None:
        self.publish(self.config.topics.user_activity, payload)

    def publish_order(self, payload: dict[str, Any]) -> None:
        self.publish(self.config.topics.orders, payload)

    def publish_payment(self, payload: dict[str, Any]) -> None:
        self.publish(self.config.topics.payments, payload)

    def publish_device_telemetry(self, payload: dict[str, Any]) -> None:
        self.publish(self.config.topics.device_telemetry, payload)

    def flush(self) -> None:
        self._producer.flush()