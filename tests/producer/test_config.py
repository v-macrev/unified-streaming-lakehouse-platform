from __future__ import annotations

from pathlib import Path

import pytest

from event_producer.src.config import load_config


def _set_minimal_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("KAFKA_TOPIC_USER_ACTIVITY", "events.user_activity.v1")
    monkeypatch.setenv("KAFKA_TOPIC_ORDERS", "events.orders.v1")
    monkeypatch.setenv("KAFKA_TOPIC_PAYMENTS", "events.payments.v1")
    monkeypatch.setenv("KAFKA_TOPIC_DEVICE_TELEMETRY", "events.device_telemetry.v1")

    monkeypatch.setenv("PRODUCER_APP_NAME", "event-producer")
    monkeypatch.setenv("PRODUCER_SOURCE_SYSTEM", "synthetic-event-generator")
    monkeypatch.setenv("PRODUCER_VERSION", "1.0.0")
    monkeypatch.setenv("PRODUCER_LOG_LEVEL", "INFO")
    monkeypatch.setenv("PRODUCER_INTERVAL_MS", "1000")
    monkeypatch.setenv("PRODUCER_BATCH_SIZE", "100")
    monkeypatch.setenv("PRODUCER_ENABLE_SCHEMA_VALIDATION", "true")
    monkeypatch.setenv("PRODUCER_SCHEMA_VERSION", "1")


def test_load_config_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_env(monkeypatch)

    config = load_config()

    assert config.kafka.bootstrap_servers == "kafka:9092"

    assert config.topics.user_activity == "events.user_activity.v1"
    assert config.topics.orders == "events.orders.v1"
    assert config.topics.payments == "events.payments.v1"
    assert config.topics.device_telemetry == "events.device_telemetry.v1"

    assert config.runtime.app_name == "event-producer"
    assert config.runtime.source_system == "synthetic-event-generator"
    assert config.runtime.producer_version == "1.0.0"
    assert config.runtime.schema_version == 1


def test_runtime_numeric_values_are_parsed_correctly(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_env(monkeypatch)

    config = load_config()

    assert isinstance(config.runtime.emit_interval_ms, int)
    assert isinstance(config.runtime.records_per_cycle, int)
    assert config.runtime.emit_interval_ms == 1000


def test_boolean_flags_are_parsed_correctly(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_env(monkeypatch)

    config = load_config()

    assert config.runtime.schema_validation_enabled is True


def test_schema_paths_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_env(monkeypatch)

    config = load_config()

    assert isinstance(config.schemas.base_dir, Path)

    assert config.schemas.user_activity_path.exists()
    assert config.schemas.orders_path.exists()
    assert config.schemas.payments_path.exists()
    assert config.schemas.device_telemetry_path.exists()


def test_kafka_producer_config_contains_expected_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_env(monkeypatch)

    config = load_config()

    kafka_config = config.kafka.producer_config

    assert "bootstrap.servers" in kafka_config
    assert "client.id" in kafka_config
    assert "acks" in kafka_config
    assert "compression.type" in kafka_config
    assert "linger.ms" in kafka_config