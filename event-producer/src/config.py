from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    return value if value else default


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"Environment variable {name} must be an integer, got {raw!r}.") from exc

    if minimum is not None and value < minimum:
        raise ValueError(f"Environment variable {name} must be >= {minimum}, got {value}.")
    return value


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    client_id: str
    acks: str
    linger_ms: int
    batch_size: int
    compression_type: str

    @property
    def producer_config(self) -> dict[str, str | int]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": self.acks,
            "linger.ms": self.linger_ms,
            "batch.size": self.batch_size,
            "compression.type": self.compression_type,
        }


@dataclass(frozen=True)
class TopicConfig:
    user_activity: str
    orders: str
    payments: str
    device_telemetry: str


@dataclass(frozen=True)
class ProducerRuntimeConfig:
    app_name: str
    source_system: str
    producer_version: str
    log_level: str
    emit_interval_ms: int
    records_per_cycle: int
    schema_validation_enabled: bool
    schema_version: int


@dataclass(frozen=True)
class SchemaConfig:
    base_dir: Path
    user_activity_path: Path
    orders_path: Path
    payments_path: Path
    device_telemetry_path: Path


@dataclass(frozen=True)
class AppConfig:
    kafka: KafkaConfig
    topics: TopicConfig
    runtime: ProducerRuntimeConfig
    schemas: SchemaConfig


def _resolve_repo_root() -> Path:
    current = Path(__file__).resolve()
    # event-producer/src/config.py -> event-producer -> repo root
    return current.parents[2]


def _build_schema_config(repo_root: Path) -> SchemaConfig:
    schema_base_dir = repo_root / "schemas"

    user_activity_path = schema_base_dir / "user_activity.schema.json"
    orders_path = schema_base_dir / "orders.schema.json"
    payments_path = schema_base_dir / "payments.schema.json"
    device_telemetry_path = schema_base_dir / "device_telemetry.schema.json"

    for path in (
        user_activity_path,
        orders_path,
        payments_path,
        device_telemetry_path,
    ):
        if not path.exists():
            raise FileNotFoundError(f"Required schema file not found: {path}")

    return SchemaConfig(
        base_dir=schema_base_dir,
        user_activity_path=user_activity_path,
        orders_path=orders_path,
        payments_path=payments_path,
        device_telemetry_path=device_telemetry_path,
    )


def load_config() -> AppConfig:
    repo_root = _resolve_repo_root()

    kafka = KafkaConfig(
        bootstrap_servers=_get_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        client_id=_get_env("PRODUCER_CLIENT_ID", "event-producer"),
        acks=_get_env("KAFKA_ACKS", "all"),
        linger_ms=_get_int("KAFKA_LINGER_MS", 10, minimum=0),
        batch_size=_get_int("KAFKA_BATCH_SIZE", 32768, minimum=1),
        compression_type=_get_env("KAFKA_COMPRESSION_TYPE", "lz4"),
    )

    topics = TopicConfig(
        user_activity=_get_env("KAFKA_TOPIC_USER_ACTIVITY", "events.user_activity.v1"),
        orders=_get_env("KAFKA_TOPIC_ORDERS", "events.orders.v1"),
        payments=_get_env("KAFKA_TOPIC_PAYMENTS", "events.payments.v1"),
        device_telemetry=_get_env("KAFKA_TOPIC_DEVICE_TELEMETRY", "events.device_telemetry.v1"),
    )

    runtime = ProducerRuntimeConfig(
        app_name=_get_env("PRODUCER_APP_NAME", "event-producer"),
        source_system=_get_env("PRODUCER_SOURCE_SYSTEM", "synthetic-event-generator"),
        producer_version=_get_env("PRODUCER_VERSION", "1.0.0"),
        log_level=_get_env("PRODUCER_LOG_LEVEL", "INFO"),
        emit_interval_ms=_get_int("PRODUCER_INTERVAL_MS", 1000, minimum=1),
        records_per_cycle=_get_int("PRODUCER_BATCH_SIZE", 100, minimum=1),
        schema_validation_enabled=_get_bool("PRODUCER_ENABLE_SCHEMA_VALIDATION", True),
        schema_version=_get_int("PRODUCER_SCHEMA_VERSION", 1, minimum=1),
    )

    schemas = _build_schema_config(repo_root)

    return AppConfig(
        kafka=kafka,
        topics=topics,
        runtime=runtime,
        schemas=schemas,
    )