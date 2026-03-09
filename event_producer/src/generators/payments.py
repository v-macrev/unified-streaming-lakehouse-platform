from __future__ import annotations

import random
from dataclasses import asdict
from datetime import date, timedelta
from typing import Any

from faker import Faker

from event_producer.src.models.envelopes import (
    PaymentEvent,
    build_base_envelope,
)

fake = Faker()

PAYMENT_STATUSES = [
    "initiated",
    "pending",
    "authorised",
    "captured",
    "settled",
    "failed",
    "cancelled",
    "refunded",
    "chargeback",
]

PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "pix",
    "wallet",
    "cash",
    "bank_transfer",
]

CHANNELS = [
    "web",
    "mobile_app",
    "call_center",
    "partner_api",
    "in_store",
]

PLATFORM_BY_CHANNEL = {
    "web": ["web"],
    "mobile_app": ["android", "ios"],
    "call_center": ["backend_api"],
    "partner_api": ["backend_api"],
    "in_store": ["pos"],
}

PAYMENT_PROVIDERS = [
    "gateway_x",
    "gateway_y",
    "gateway_z",
    "internal_acquirer",
]

CARD_BRANDS = ["visa", "mastercard", "amex", "elo", "hipercard"]

REGIONS = [
    ("north", "BR"),
    ("northeast", "BR"),
    ("midwest", "BR"),
    ("southeast", "BR"),
    ("south", "BR"),
]

FAILURE_REASONS = [
    "insufficient_funds",
    "card_declined",
    "timeout",
    "risk_rejected",
    "invalid_payment_data",
    "psp_unavailable",
]

REFUNDABLE_STATUSES = {"captured", "settled", "refunded", "chargeback"}


def _random_payment_id() -> str:
    return f"pay_{random.randint(100000, 999999)}"


def _random_order_id() -> str:
    return f"ord_{random.randint(100000, 999999)}"


def _random_user_id() -> str:
    return f"user_{random.randint(10000, 99999)}"


def _random_merchant_id() -> str:
    return f"merchant_{random.randint(1000, 9999)}"


def _random_channel() -> str:
    return random.choice(CHANNELS)


def _random_platform(channel: str) -> str:
    return random.choice(PLATFORM_BY_CHANNEL[channel])


def _random_payment_method(channel: str) -> str:
    if channel == "in_store":
        return random.choice(["cash", "debit_card", "credit_card", "pix"])
    return random.choice(PAYMENT_METHODS)


def _random_payment_status(is_refund: bool) -> str:
    if is_refund:
        return random.choice(["refunded", "chargeback"])
    return random.choices(
        population=PAYMENT_STATUSES,
        weights=[8, 14, 18, 18, 16, 12, 5, 5, 4],
        k=1,
    )[0]


def _random_amount() -> float:
    return round(random.uniform(19.90, 999.90), 2)


def _derive_amount_fields(
    payment_status: str,
    payment_amount: float,
    is_refund: bool,
) -> tuple[float | None, float | None, float | None]:
    if is_refund:
        refunded_amount = payment_amount
        captured_amount = payment_amount if payment_status in {"refunded", "chargeback"} else None
        authorised_amount = payment_amount
        return authorised_amount, captured_amount, refunded_amount

    if payment_status in {"initiated", "pending"}:
        return None, None, None
    if payment_status == "authorised":
        return payment_amount, None, None
    if payment_status in {"captured", "settled"}:
        return payment_amount, payment_amount, None
    if payment_status in {"failed", "cancelled"}:
        return None, None, None
    if payment_status in {"refunded", "chargeback"}:
        return payment_amount, payment_amount, payment_amount

    return None, None, None


def _derive_failure_reason(payment_status: str) -> str | None:
    if payment_status == "failed":
        return random.choice(FAILURE_REASONS)
    return None


def _derive_installments(payment_method: str) -> int | None:
    if payment_method != "credit_card":
        return None
    return random.choices(
        population=[1, 2, 3, 6, 10, 12],
        weights=[40, 18, 16, 12, 8, 6],
        k=1,
    )[0]


def _derive_card_brand(payment_method: str) -> str | None:
    if payment_method not in {"credit_card", "debit_card"}:
        return None
    return random.choice(CARD_BRANDS)


def _derive_settlement_date(payment_status: str) -> str | None:
    if payment_status not in {"settled", "refunded", "chargeback"}:
        return None
    return (date.today() + timedelta(days=random.randint(0, 2))).isoformat()


def _derive_is_refund() -> bool:
    return random.random() < 0.08


def _derive_payment_attempt(payment_status: str) -> int:
    if payment_status == "failed":
        return random.randint(1, 3)
    return 1


def _random_event_properties(
    payment_method: str,
    payment_status: str,
) -> dict[str, str | int | float | bool | None]:
    properties: dict[str, str | int | float | bool | None] = {
        "anti_fraud_passed": payment_status not in {"failed", "chargeback"},
        "latency_ms": random.randint(100, 2500),
        "risk_score": round(random.uniform(0.01, 0.99), 2),
    }

    if payment_method == "pix":
        properties["pix_qr_dynamic"] = random.choice([True, False])

    if payment_method in {"credit_card", "debit_card"}:
        properties["tokenized_card"] = random.choice([True, False])

    return properties


def build_payment_event(
    *,
    producer_version: str,
    schema_version: int,
    source_system: str,
) -> PaymentEvent:
    order_id = _random_order_id()
    user_id = _random_user_id()
    merchant_id = _random_merchant_id()
    channel = _random_channel()
    platform = _random_platform(channel)
    region, country_code = random.choice(REGIONS)
    payment_method = _random_payment_method(channel)
    is_refund = _derive_is_refund()
    payment_status = _random_payment_status(is_refund=is_refund)
    payment_amount = _random_amount()
    authorised_amount, captured_amount, refunded_amount = _derive_amount_fields(
        payment_status=payment_status,
        payment_amount=payment_amount,
        is_refund=is_refund,
    )

    # Keep the provider nullable for cash flows to stay more realistic.
    payment_provider = None if payment_method == "cash" else random.choice(PAYMENT_PROVIDERS)

    base = build_base_envelope(
        event_type="payment_event",
        partition_key=order_id,
        producer_version=producer_version,
        schema_version=schema_version,
        source_system=source_system,
    )

    return PaymentEvent(
        **base,
        payment_id=_random_payment_id(),
        order_id=order_id,
        user_id=user_id,
        merchant_id=merchant_id,
        payment_status=payment_status,
        payment_method=payment_method,
        payment_amount=payment_amount,
        currency="BRL",
        is_refund=is_refund,
        channel=channel,
        platform=platform,
        region=region,
        country_code=country_code,
        parent_payment_id=None,
        payment_provider=payment_provider,
        authorised_amount=authorised_amount,
        captured_amount=captured_amount,
        refunded_amount=refunded_amount,
        failure_reason=_derive_failure_reason(payment_status),
        payment_attempt=_derive_payment_attempt(payment_status),
        installments=_derive_installments(payment_method),
        card_brand=_derive_card_brand(payment_method),
        gateway_transaction_id=None if payment_method == "cash" else f"gw_txn_{fake.lexify(text='????????').lower()}",
        settlement_date=_derive_settlement_date(payment_status),
        event_properties=_random_event_properties(payment_method, payment_status),
    )


def generate_payment_payload(
    *,
    producer_version: str,
    schema_version: int,
    source_system: str,
) -> dict[str, Any]:
    event = build_payment_event(
        producer_version=producer_version,
        schema_version=schema_version,
        source_system=source_system,
    )
    return asdict(event)