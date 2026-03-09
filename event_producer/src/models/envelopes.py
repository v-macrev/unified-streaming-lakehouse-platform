from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4


EventType = Literal["user_activity", "order_event", "payment_event", "device_telemetry"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_event_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def generate_trace_id() -> str:
    return f"trace_{uuid4().hex}"


@dataclass(frozen=True)
class BaseEnvelope:
    event_id: str
    event_type: EventType
    event_time: str
    ingestion_time: str
    producer_version: str
    schema_version: int
    source_system: str
    partition_key: str
    trace_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExperimentAssignment:
    experiment_id: str
    variant: str


@dataclass(frozen=True)
class UserActivityEvent(BaseEnvelope):
    user_id: str
    session_id: str
    action_type: str
    channel: str
    platform: str
    device_type: str
    page: str
    region: str
    country_code: str
    is_authenticated: bool
    referrer: str | None = None
    campaign_id: str | None = None
    language: str | None = None
    experiment_assignments: list[ExperimentAssignment] = field(default_factory=list)
    event_properties: dict[str, str | int | float | bool | None] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["experiment_assignments"] = [asdict(item) for item in self.experiment_assignments]
        return payload


@dataclass(frozen=True)
class OrderItem:
    item_id: str
    sku: str
    quantity: int
    unit_price: float
    product_name: str | None = None
    category: str | None = None
    discount_amount: float = 0.0


@dataclass(frozen=True)
class OrderEvent(BaseEnvelope):
    order_id: str
    user_id: str
    merchant_id: str
    order_status: str
    order_currency: str
    order_amount: float
    item_count: int
    payment_required: bool
    channel: str
    platform: str
    region: str
    country_code: str
    parent_order_id: str | None = None
    discount_amount: float = 0.0
    shipping_amount: float = 0.0
    tax_amount: float = 0.0
    net_amount: float | None = None
    payment_status: str | None = None
    fulfilment_type: str | None = None
    payment_method: str | None = None
    customer_tier: str | None = None
    items: list[OrderItem] = field(default_factory=list)
    event_properties: dict[str, str | int | float | bool | None] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["items"] = [asdict(item) for item in self.items]
        return payload


@dataclass(frozen=True)
class PaymentEvent(BaseEnvelope):
    payment_id: str
    order_id: str
    user_id: str
    merchant_id: str
    payment_status: str
    payment_method: str
    payment_amount: float
    currency: str
    is_refund: bool
    channel: str
    platform: str
    region: str
    country_code: str
    parent_payment_id: str | None = None
    payment_provider: str | None = None
    authorised_amount: float | None = None
    captured_amount: float | None = None
    refunded_amount: float | None = None
    failure_reason: str | None = None
    payment_attempt: int = 1
    installments: int | None = None
    card_brand: str | None = None
    gateway_transaction_id: str | None = None
    settlement_date: str | None = None
    event_properties: dict[str, str | int | float | bool | None] = field(default_factory=dict)


@dataclass(frozen=True)
class DeviceTelemetryEvent(BaseEnvelope):
    device_id: str
    metric_name: str
    metric_type: str
    metric_value: float
    metric_unit: str
    platform: str
    device_type: str
    app_version: str
    os_name: str
    os_version: str
    region: str
    country_code: str
    user_id: str | None = None
    session_id: str | None = None
    manufacturer: str | None = None
    model: str | None = None
    network_type: str | None = None
    severity: str | None = None
    threshold_breached: bool = False
    event_properties: dict[str, str | int | float | bool | None] = field(default_factory=dict)


def build_base_envelope(
    *,
    event_type: EventType,
    partition_key: str,
    producer_version: str,
    schema_version: int,
    source_system: str,
    event_time: str | None = None,
    trace_id: str | None = None,
) -> dict[str, Any]:
    resolved_event_time = event_time or utc_now_iso()
    return {
        "event_id": generate_event_id(event_type),
        "event_type": event_type,
        "event_time": resolved_event_time,
        "ingestion_time": utc_now_iso(),
        "producer_version": producer_version,
        "schema_version": schema_version,
        "source_system": source_system,
        "partition_key": partition_key,
        "trace_id": trace_id or generate_trace_id(),
    }