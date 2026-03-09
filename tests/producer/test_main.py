from __future__ import annotations

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
from event_producer.src import main as main_module


class FakeEventProducer:
    def __init__(self, _config: AppConfig) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.flush_calls = 0

    def publish(self, topic: str, payload: dict[str, Any]) -> None:
        self.published.append((topic, payload))

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
            emit_interval_ms=1,
            records_per_cycle=5,
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
def app_config() -> AppConfig:
    return _build_app_config()


def test_common_generator_kwargs_builds_expected_runtime_values(app_config: AppConfig) -> None:
    kwargs = main_module._common_generator_kwargs(app_config)

    assert kwargs == {
        "producer_version": app_config.runtime.producer_version,
        "schema_version": app_config.runtime.schema_version,
        "source_system": app_config.runtime.source_system,
    }


def test_generator_plan_exposes_all_four_topics(app_config: AppConfig) -> None:
    plan = main_module._generator_plan(app_config)

    assert len(plan) == 4
    topics = [topic for topic, _ in plan]

    assert topics == [
        app_config.topics.user_activity,
        app_config.topics.orders,
        app_config.topics.payments,
        app_config.topics.device_telemetry,
    ]


def test_run_publish_cycle_publishes_configured_number_of_records(
    monkeypatch: pytest.MonkeyPatch,
    app_config: AppConfig,
) -> None:
    fake_producer = FakeEventProducer(app_config)

    def fake_generator(**kwargs: Any) -> dict[str, Any]:
        return {
            "event_id": "evt_test_001",
            "event_type": "user_activity",
            "partition_key": "user_123",
            "producer_version": kwargs["producer_version"],
            "schema_version": kwargs["schema_version"],
            "source_system": kwargs["source_system"],
        }

    monkeypatch.setattr(
        main_module,
        "_generator_plan",
        lambda _config: [(app_config.topics.user_activity, fake_generator)],
    )

    published_count = main_module._run_publish_cycle(
        event_producer=fake_producer,
        config=app_config,
    )

    assert published_count == app_config.runtime.records_per_cycle
    assert len(fake_producer.published) == app_config.runtime.records_per_cycle

    for topic, payload in fake_producer.published:
        assert topic == app_config.topics.user_activity
        assert payload["partition_key"] == "user_123"
        assert payload["producer_version"] == app_config.runtime.producer_version
        assert payload["schema_version"] == app_config.runtime.schema_version
        assert payload["source_system"] == app_config.runtime.source_system


def test_run_returns_zero_on_graceful_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    app_config: AppConfig,
) -> None:
    fake_producer = FakeEventProducer(app_config)

    monkeypatch.setattr(main_module, "load_dotenv", lambda: None)
    monkeypatch.setattr(main_module, "load_config", lambda: app_config)
    monkeypatch.setattr(main_module, "_configure_logging", lambda _config: None)
    monkeypatch.setattr(main_module, "_register_signal_handlers", lambda: None)
    monkeypatch.setattr(main_module, "EventProducer", lambda _config: fake_producer)
    monkeypatch.setattr(main_module.time, "sleep", lambda _seconds: None)

    call_state = {"count": 0}

    def fake_run_publish_cycle(*, event_producer: Any, config: AppConfig) -> int:
        assert event_producer is fake_producer
        assert config is app_config
        call_state["count"] += 1
        main_module._SHOULD_STOP = True
        return 3

    monkeypatch.setattr(main_module, "_run_publish_cycle", fake_run_publish_cycle)

    main_module._SHOULD_STOP = False
    result = main_module.run()

    assert result == 0
    assert call_state["count"] == 1
    assert fake_producer.flush_calls == 1


def test_run_returns_one_when_config_loading_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "load_dotenv", lambda: None)

    def raise_config_error() -> AppConfig:
        raise ValueError("invalid config")

    monkeypatch.setattr(main_module, "load_config", raise_config_error)

    main_module._SHOULD_STOP = False
    result = main_module.run()

    assert result == 1


def test_run_returns_one_on_unhandled_runtime_error_and_flushes(
    monkeypatch: pytest.MonkeyPatch,
    app_config: AppConfig,
) -> None:
    fake_producer = FakeEventProducer(app_config)

    monkeypatch.setattr(main_module, "load_dotenv", lambda: None)
    monkeypatch.setattr(main_module, "load_config", lambda: app_config)
    monkeypatch.setattr(main_module, "_configure_logging", lambda _config: None)
    monkeypatch.setattr(main_module, "_register_signal_handlers", lambda: None)
    monkeypatch.setattr(main_module, "EventProducer", lambda _config: fake_producer)

    def raise_runtime_error(*, event_producer: Any, config: AppConfig) -> int:
        assert event_producer is fake_producer
        assert config is app_config
        raise RuntimeError("boom")

    monkeypatch.setattr(main_module, "_run_publish_cycle", raise_runtime_error)

    main_module._SHOULD_STOP = False
    result = main_module.run()

    assert result == 1
    assert fake_producer.flush_calls == 1