from __future__ import annotations

import random
from dataclasses import asdict
from typing import Any

from faker import Faker

from event_producer.src.models.envelopes import (
    OrderEvent,
    OrderItem,
    build_base_envelope,
)

fake = Faker()

ORDER_STATUSES = [
    "created",
    "submitted",
    "confirmed",
    "packed",
    "shipped",
    "delivered",
    "cancelled",
    "returned",
    "failed",
]

CHANNELS = [
    "web",
    "mobile_app",
    "call_center",
    "partner_api",
    "in_store",
]

PLATFORMS = {
    "web": ["web"],
    "mobile_app": ["android", "ios"],
    "call_center": ["backend_api"],
    "partner_api": ["backend_api"],
    "in_store": ["pos"],
}

FULFILMENT_TYPES = [
    "delivery",
    "pickup",
    "ship_to_home",
    "digital",
]

PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "pix",
    "wallet",
    "cash",
    "bank_transfer",
]

CUSTOMER_TIERS = [
    "new",
    "returning",
    "vip",
]

REGIONS = [
    ("north", "BR"),
    ("northeast", "BR"),
    ("midwest", "BR"),
    ("southeast", "BR"),
    ("south", "BR"),
]

PRODUCT_CATEGORIES = [
    "electronics",
    "peripherals",
    "home",
    "accessories",
    "gaming",
    "software",
]

PRODUCT_NAMES = [
    "Mechanical Keyboard",
    "Gaming Mouse",
    "USB-C Dock",
    "Wireless Headset",
    "Laptop Stand",
    "Smart Lamp",
    "Portable SSD",
    "Streaming Camera",
    "Extended Mouse Pad",
    "Bluetooth Speaker",
]


def _random_user_id() -> str:
    return f"user_{random.randint(10000, 99999)}"


def _random_order_id() -> str:
    return f"ord_{random.randint(100000, 999999)}"


def _random_merchant_id() -> str:
    return f"merchant_{random.randint(1000, 9999)}"


def _random_channel() -> str:
    return random.choice(CHANNELS)


def _random_platform(channel: str) -> str:
    return random.choice(PLATFORMS[channel])


def _random_customer_tier() -> str | None:
    return random.choices(
        population=[None, "new", "returning", "vip"],
        weights=[0.15, 0.25, 0.45, 0.15],
        k=1,
    )[0]


def _random_payment_method(channel: str) -> str | None:
    if channel == "in_store":
        return random.choice(["cash", "debit_card", "credit_card", "pix"])
    return random.choice(PAYMENT_METHODS)


def _random_order_status() -> str:
    return random.choices(
        population=ORDER_STATUSES,
        weights=[10, 20, 18, 12, 10, 14, 7, 4, 5],
        k=1,
    )[0]


def _random_item() -> OrderItem:
    product_name = random.choice(PRODUCT_NAMES)
    category = random.choice(PRODUCT_CATEGORIES)
    quantity = random.randint(1, 3)
    unit_price = round(random.uniform(19.9, 499.9), 2)
    discount_amount = round(random.choice([0.0, 0.0, 0.0, 5.0, 10.0, 15.0]), 2)

    return OrderItem(
        item_id=f"item_{random.randint(100000, 999999)}",
        sku=f"sku_{fake.lexify(text='????').lower()}_{random.randint(100, 999)}",
        quantity=quantity,
        unit_price=unit_price,
        product_name=product_name,
        category=category,
        discount_amount=discount_amount,
    )


def _build_items() -> list[OrderItem]:
    item_count = random.randint(1, 4)
    return [_random_item() for _ in range(item_count)]


def _calculate_order_totals(items: list[OrderItem], fulfilment_type: str) -> dict[str, float]:
    gross = round(sum(item.unit_price * item.quantity for item in items), 2)
    discount = round(sum(item.discount_amount for item in items), 2)
    shipping = 0.0 if fulfilment_type in {"pickup", "digital"} else round(random.uniform(0.0, 29.9), 2)
    tax = 0.0
    net = round(max(gross - discount + shipping + tax, 0.0), 2)

    return {
        "gross": gross,
        "discount": discount,
        "shipping": shipping,
        "tax": tax,
        "net": net,
    }


def _derive_payment_status(order_status: str, payment_required: bool) -> str | None:
    if not payment_required:
        return None

    mapping = {
        "created": "pending",
        "submitted": "pending",
        "confirmed": "authorised",
        "packed": "captured",
        "shipped": "captured",
        "delivered": "captured",
        "cancelled": random.choice(["failed", "refunded"]),
        "returned": "refunded",
        "failed": "failed",
    }
    return mapping[order_status]


def _random_event_properties(channel: str, fulfilment_type: str) -> dict[str, str | int | float | bool | None]:
    props: dict[str, str | int | float | bool | None] = {
        "estimated_delivery_hours": random.randint(1, 72),
        "is_expedited": random.choice([True, False]),
    }

    if channel in {"web", "mobile_app"}:
        props["coupon_code"] = random.choice(
            [None, "WELCOME10", "SPRING15", "APPONLY", "VIP20"]
        )

    if fulfilment_type == "digital":
        props["delivery_mode"] = "instant"
    elif fulfilment_type == "pickup":
        props["pickup_store_id"] = f"store_{random.randint(100, 999)}"

    return props


def build_order_event(
    *,
    producer_version: str,
    schema_version: int,
    source_system: str,
) -> OrderEvent:
    order_id = _random_order_id()
    user_id = _random_user_id()
    merchant_id = _random_merchant_id()
    channel = _random_channel()
    platform = _random_platform(channel)
    region, country_code = random.choice(REGIONS)
    fulfilment_type = random.choice(FULFILMENT_TYPES)
    items = _build_items()
    totals = _calculate_order_totals(items, fulfilment_type)
    order_status = _random_order_status()
    payment_required = fulfilment_type != "digital" or random.random() < 0.9
    payment_method = _random_payment_method(channel) if payment_required else None

    base = build_base_envelope(
        event_type="order_event",
        partition_key=order_id,
        producer_version=producer_version,
        schema_version=schema_version,
        source_system=source_system,
    )

    return OrderEvent(
        **base,
        order_id=order_id,
        user_id=user_id,
        merchant_id=merchant_id,
        order_status=order_status,
        order_currency="BRL",
        order_amount=totals["gross"],
        item_count=sum(item.quantity for item in items),
        payment_required=payment_required,
        channel=channel,
        platform=platform,
        region=region,
        country_code=country_code,
        parent_order_id=None,
        discount_amount=totals["discount"],
        shipping_amount=totals["shipping"],
        tax_amount=totals["tax"],
        net_amount=totals["net"],
        payment_status=_derive_payment_status(order_status, payment_required),
        fulfilment_type=fulfilment_type,
        payment_method=payment_method,
        customer_tier=_random_customer_tier(),
        items=items,
        event_properties=_random_event_properties(channel, fulfilment_type),
    )


def generate_order_payload(
    *,
    producer_version: str,
    schema_version: int,
    source_system: str,
) -> dict[str, Any]:
    event = build_order_event(
        producer_version=producer_version,
        schema_version=schema_version,
        source_system=source_system,
    )
    payload = asdict(event)
    payload["items"] = [asdict(item) for item in event.items]
    return payload