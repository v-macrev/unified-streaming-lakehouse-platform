from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from event_producer.src.config import (
    AppConfig,
    KafkaConfig,
    ProducerRuntimeConfig,
    SchemaConfig,
    TopicConfig,
)
from event_producer.src.producer import EventProducer


class FakeKafkaProducer:
    def __init__(self, config: dict[str, str | int]) -> None:
        self.config = config
        self.produced_messages: list[dict[str, Any]] = []
        self.poll_calls: list[int] = []
        self.flush_calls = 0

    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery,
    ) -> None:
        self.produced_messages.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "on_delivery": on_delivery,
            }
        )

    def poll(self, timeout: int) -> None:
        self.poll_calls.append(timeout)

    def flush(self) -> None:
        self.flush_calls += 1


def _build_app_config() -> AppConfig:
    repo_root = Path(__file__).resolve().parents[2]
    schema_base_dir = repo_root / "schemas"

    return AppConfig(
        kafka=KafkaConfig(
            bootstrap_servers="kafka:9092",
            client_id="event-producer-test",
            acks="all",
            linger_ms=10,
            batch_size=32768,
            compression_type="lz4",
        ),
        topics=TopicConfig(
            user_activity="events.user_activity.v1",
            orders="events.orders.v1",
            payments="events.payments.v1",
            device_telemetry="events.device_telemetry.v1",
        ),
        runtime=ProducerRuntimeConfig(
            app_name="event-producer",
            source_system="test-generator",
            producer_version="test-1.0.0",
            log_level="INFO",
            emit_interval_ms=1000,
            records_per_cycle=10,
            schema_validation_enabled=True,
            schema_version=1,
        ),
        schemas=SchemaConfig(
            base_dir=schema_base_dir,
            user_activity_path=schema_base_dir / "user_activity.schema.json",
            orders_path=schema_base_dir / "orders.schema.json",
            payments_path=schema_base_dir / "payments.schema.json",
            device_telemetry_path=schema_base_dir / "device_telemetry.schema.json",
        ),
    )


@pytest.fixture
def fake_producer_class(monkeypatch: pytest.MonkeyPatch) -> type[FakeKafkaProducer]:
    from event_producer.src import producer as producer_module

    monkeypatch.setattr(producer_module, "Producer", FakeKafkaProducer)
    return FakeKafkaProducer


@pytest.fixture
def app_config() -> AppConfig:
    return _build_app_config()


def test_event_producer_initialises_kafka_client_with_expected_config(
    fake_producer_class: type[FakeKafkaProducer],
    app_config: AppConfig,
) -> None:
    producer = EventProducer(app_config)

    assert isinstance(producer._producer, fake_producer_class)  # type: ignore[attr-defined]
    assert producer._producer.config == app_config.kafka.producer_config  # type: ignore[attr-defined]


def test_publish_serializes_payload_and_uses_partition_key_as_message_key(
    fake_producer_class: type[FakeKafkaProducer],
    app_config: AppConfig,
) -> None:
    producer = EventProducer(app_config)

    payload = {
        "event_id": "evt_001",
        "event_type": "user_activity",
        "partition_key": "user_123",
        "value": 42,
    }

    producer.publish("events.user_activity.v1", payload)

    produced_messages = producer._producer.produced_messages  # type: ignore[attr-defined]
    assert len(produced_messages) == 1

    message = produced_messages[0]
    assert message["topic"] == "events.user_activity.v1"
    assert message["key"] == b"user_123"

    decoded_payload = json.loads(message["value"].decode("utf-8"))
    assert decoded_payload == payload

    assert producer._producer.poll_calls == [0]  # type: ignore[attr-defined]


def test_publish_raises_error_when_partition_key_is_missing(
    fake_producer_class: type[FakeKafkaProducer],
    app_config: AppConfig,
) -> None:
    producer = EventProducer(app_config)

    payload = {
        "event_id": "evt_001",
        "event_type": "user_activity",
    }

    with pytest.raises(ValueError, match="partition_key"):
        producer.publish("events.user_activity.v1", payload)

    assert producer._producer.produced_messages == []  # type: ignore[attr-defined]
    assert producer._producer.poll_calls == []  # type: ignore[attr-defined]


def test_publish_user_activity_routes_to_user_activity_topic(
    fake_producer_class: type[FakeKafkaProducer],
    app_config: AppConfig,
) -> None:
    producer = EventProducer(app_config)

    payload = {
        "event_id": "evt_user_001",
        "event_type": "user_activity",
        "partition_key": "user_123",
    }

    producer.publish_user_activity(payload)

    message = producer._producer.produced_messages[0]  # type: ignore[attr-defined]
    assert message["topic"] == app_config.topics.user_activity


def test_publish_order_routes_to_orders_topic(
    fake_producer_class: type[FakeKafkaProducer],
    app_config: AppConfig,
) -> None:
    producer = EventProducer(app_config)

    payload = {
        "event_id": "evt_order_001",
        "event_type": "order_event",
        "partition_key": "ord_123",
    }

    producer.publish_order(payload)

    message = producer._producer.produced_messages[0]  # type: ignore[attr-defined]
    assert message["topic"] == app_config.topics.orders


def test_publish_payment_routes_to_payments_topic(
    fake_producer_class: type[FakeKafkaProducer],
    app_config: AppConfig,
) -> None:
    producer = EventProducer(app_config)

    payload = {
        "event_id": "evt_payment_001",
        "event_type": "payment_event",
        "partition_key": "ord_123",
    }

    producer.publish_payment(payload)

    message = producer._producer.produced_messages[0]  # type: ignore[attr-defined]
    assert message["topic"] == app_config.topics.payments


def test_publish_device_telemetry_routes_to_device_telemetry_topic(
    fake_producer_class: type[FakeKafkaProducer],
    app_config: AppConfig,
) -> None:
    producer = EventProducer(app_config)

    payload = {
        "event_id": "evt_device_001",
        "event_type": "device_telemetry",
        "partition_key": "device_123",
    }

    producer.publish_device_telemetry(payload)

    message = producer._producer.produced_messages[0]  # type: ignore[attr-defined]
    assert message["topic"] == app_config.topics.device_telemetry


def test_flush_delegates_to_underlying_kafka_client(
    fake_producer_class: type[FakeKafkaProducer],
    app_config: AppConfig,
) -> None:
    producer = EventProducer(app_config)

    producer.flush()

    assert producer._producer.flush_calls == 1  # type: ignore[attr-defined]