from __future__ import annotations

from event_producer.src.generators.device_telemetry import generate_device_telemetry_payload
from event_producer.src.generators.orders import generate_order_payload
from event_producer.src.generators.payments import generate_payment_payload
from event_producer.src.generators.user_activity import generate_user_activity_payload


TEST_PRODUCER_VERSION = "test-1.0.0"
TEST_SCHEMA_VERSION = 1
TEST_SOURCE_SYSTEM = "test-generator"


def _build_common_kwargs() -> dict[str, str | int]:
    return {
        "producer_version": TEST_PRODUCER_VERSION,
        "schema_version": TEST_SCHEMA_VERSION,
        "source_system": TEST_SOURCE_SYSTEM,
    }


def test_generate_user_activity_payload_returns_expected_shape() -> None:
    payload = generate_user_activity_payload(**_build_common_kwargs())

    assert isinstance(payload, dict)
    assert payload["event_type"] == "user_activity"
    assert payload["producer_version"] == TEST_PRODUCER_VERSION
    assert payload["schema_version"] == TEST_SCHEMA_VERSION
    assert payload["source_system"] == TEST_SOURCE_SYSTEM

    assert payload["partition_key"] == payload["user_id"]
    assert payload["platform"] in {"web", "android", "ios"}
    assert payload["channel"] in {
        "web",
        "mobile_app",
        "email",
        "push_notification",
        "paid_media",
        "organic",
    }
    assert isinstance(payload["experiment_assignments"], list)
    assert isinstance(payload["event_properties"], dict)


def test_generate_order_payload_returns_expected_shape() -> None:
    payload = generate_order_payload(**_build_common_kwargs())

    assert isinstance(payload, dict)
    assert payload["event_type"] == "order_event"
    assert payload["producer_version"] == TEST_PRODUCER_VERSION
    assert payload["schema_version"] == TEST_SCHEMA_VERSION
    assert payload["source_system"] == TEST_SOURCE_SYSTEM

    assert payload["partition_key"] == payload["order_id"]
    assert payload["order_currency"] == "BRL"
    assert payload["item_count"] >= 1
    assert isinstance(payload["items"], list)
    assert len(payload["items"]) >= 1
    assert payload["order_amount"] >= 0
    assert payload["discount_amount"] >= 0
    assert payload["shipping_amount"] >= 0
    assert payload["tax_amount"] >= 0

    computed_quantity = sum(item["quantity"] for item in payload["items"])
    assert payload["item_count"] == computed_quantity

    computed_gross = round(sum(item["unit_price"] * item["quantity"] for item in payload["items"]), 2)
    assert payload["order_amount"] == computed_gross

    computed_discount = round(sum(item["discount_amount"] for item in payload["items"]), 2)
    assert payload["discount_amount"] == computed_discount

    expected_net = round(
        max(
            payload["order_amount"]
            - payload["discount_amount"]
            + payload["shipping_amount"]
            + payload["tax_amount"],
            0.0,
        ),
        2,
    )
    assert payload["net_amount"] == expected_net


def test_generate_payment_payload_returns_expected_shape() -> None:
    payload = generate_payment_payload(**_build_common_kwargs())

    assert isinstance(payload, dict)
    assert payload["event_type"] == "payment_event"
    assert payload["producer_version"] == TEST_PRODUCER_VERSION
    assert payload["schema_version"] == TEST_SCHEMA_VERSION
    assert payload["source_system"] == TEST_SOURCE_SYSTEM

    assert payload["partition_key"] == payload["order_id"]
    assert payload["currency"] == "BRL"
    assert payload["payment_amount"] >= 0
    assert payload["payment_attempt"] >= 1

    if payload["payment_method"] == "cash":
        assert payload["payment_provider"] is None
        assert payload["gateway_transaction_id"] is None

    if payload["payment_status"] == "failed":
        assert payload["failure_reason"] is not None

    if payload["payment_status"] in {"settled", "refunded", "chargeback"}:
        assert payload["settlement_date"] is not None

    if payload["is_refund"]:
        assert payload["payment_status"] in {"refunded", "chargeback"}
        assert payload["refunded_amount"] == payload["payment_amount"]


def test_generate_device_telemetry_payload_returns_expected_shape() -> None:
    payload = generate_device_telemetry_payload(**_build_common_kwargs())

    assert isinstance(payload, dict)
    assert payload["event_type"] == "device_telemetry"
    assert payload["producer_version"] == TEST_PRODUCER_VERSION
    assert payload["schema_version"] == TEST_SCHEMA_VERSION
    assert payload["source_system"] == TEST_SOURCE_SYSTEM

    assert payload["partition_key"] == payload["device_id"]
    assert payload["metric_value"] >= 0
    assert payload["metric_type"] in {"gauge", "counter", "histogram_sample"}
    assert payload["metric_unit"] in {"pct", "ms", "count", "mb", "celsius"}

    if payload["threshold_breached"]:
        assert payload["severity"] in {"warning", "critical"}

    if payload["platform"] in {"desktop", "web"}:
        assert payload["network_type"] in {"wifi", "ethernet", "offline", None}


def test_generators_produce_distinct_event_ids_across_multiple_calls() -> None:
    common_kwargs = _build_common_kwargs()

    payload_a = generate_user_activity_payload(**common_kwargs)
    payload_b = generate_user_activity_payload(**common_kwargs)
    payload_c = generate_order_payload(**common_kwargs)
    payload_d = generate_payment_payload(**common_kwargs)
    payload_e = generate_device_telemetry_payload(**common_kwargs)

    event_ids = {
        payload_a["event_id"],
        payload_b["event_id"],
        payload_c["event_id"],
        payload_d["event_id"],
        payload_e["event_id"],
    }

    assert len(event_ids) == 5