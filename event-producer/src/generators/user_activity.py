from __future__ import annotations

import random
from dataclasses import asdict
from typing import Any

from faker import Faker

from event_producer.src.models.envelopes import (
    ExperimentAssignment,
    UserActivityEvent,
    build_base_envelope,
)

fake = Faker()

ACTION_TYPES = [
    "page_view",
    "click",
    "search",
    "login",
    "logout",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "purchase_complete",
    "form_submit",
    "video_play",
    "video_pause",
]

CHANNELS = [
    "web",
    "mobile_app",
    "email",
    "push_notification",
    "paid_media",
    "organic",
]

PLATFORMS = [
    "web",
    "android",
    "ios",
]

DEVICE_TYPES = {
    "web": ["desktop", "mobile", "tablet"],
    "android": ["mobile", "tablet"],
    "ios": ["mobile", "tablet"],
}

REGIONS = [
    ("north", "BR"),
    ("northeast", "BR"),
    ("midwest", "BR"),
    ("southeast", "BR"),
    ("south", "BR"),
]

PAGES = [
    "/",
    "/home",
    "/search",
    "/products",
    "/cart",
    "/checkout",
    "/profile",
    "/offers",
    "/help",
    "/video/tutorial",
]

LANGUAGES = [
    "pt-BR",
    "en-GB",
    "es-ES",
]

EXPERIMENT_IDS = [
    "exp_homepage_layout",
    "exp_checkout_button",
    "exp_search_ranking",
    "exp_video_autoplay",
]


def _random_user_id() -> str:
    return f"user_{random.randint(10000, 99999)}"


def _random_session_id() -> str:
    return f"sess_{random.randint(1000000, 9999999)}"


def _random_channel(platform: str) -> str:
    if platform == "web":
        return random.choice(["web", "email", "paid_media", "organic"])
    return random.choice(["mobile_app", "push_notification", "paid_media", "organic"])


def _random_device_type(platform: str) -> str:
    return random.choice(DEVICE_TYPES[platform])


def _random_page() -> str:
    base = random.choice(PAGES)
    if base == "/products":
        return f"/products/{fake.slug()}"
    if base == "/search":
        return f"/search?q={fake.word()}"
    if base == "/video/tutorial":
        return f"/video/{fake.slug()}"
    return base


def _random_referrer(page: str) -> str | None:
    if page in {"/", "/home"}:
        return None
    return random.choice(
        [
            "/",
            "/home",
            "/search",
            "/offers",
            f"https://{fake.domain_name()}/{fake.slug()}",
        ]
    )


def _random_campaign_id(channel: str) -> str | None:
    if channel in {"paid_media", "email", "push_notification"}:
        return f"camp_{fake.lexify(text='????').lower()}_{random.randint(1, 99):02d}"
    return None


def _random_experiments() -> list[ExperimentAssignment]:
    if random.random() < 0.6:
        return []

    assignments: list[ExperimentAssignment] = []
    for experiment_id in random.sample(EXPERIMENT_IDS, k=random.randint(1, min(2, len(EXPERIMENT_IDS)))):
        assignments.append(
            ExperimentAssignment(
                experiment_id=experiment_id,
                variant=random.choice(["A", "B", "C"]),
            )
        )
    return assignments


def _random_event_properties(action_type: str) -> dict[str, str | int | float | bool | None]:
    properties: dict[str, str | int | float | bool | None] = {
        "foreground": random.choice([True, False]),
        "scroll_depth_pct": random.randint(0, 100),
    }

    if action_type == "search":
        properties["search_term"] = fake.word()
        properties["results_count"] = random.randint(0, 250)

    if action_type in {"click", "form_submit"}:
        properties["button_name"] = fake.word()

    if action_type in {"video_play", "video_pause"}:
        properties["video_id"] = f"vid_{random.randint(1000, 9999)}"
        properties["playback_position_sec"] = random.randint(0, 1800)

    if action_type in {"add_to_cart", "remove_from_cart", "purchase_complete"}:
        properties["sku"] = f"sku_{fake.lexify(text='????').lower()}_{random.randint(100, 999)}"
        properties["quantity"] = random.randint(1, 5)

    return properties


def build_user_activity_event(
    *,
    producer_version: str,
    schema_version: int,
    source_system: str,
) -> UserActivityEvent:
    platform = random.choice(PLATFORMS)
    channel = _random_channel(platform)
    device_type = _random_device_type(platform)
    region, country_code = random.choice(REGIONS)
    user_id = _random_user_id()
    session_id = _random_session_id()
    action_type = random.choice(ACTION_TYPES)
    page = _random_page()
    is_authenticated = random.random() < 0.85
    language = random.choice(LANGUAGES)

    base = build_base_envelope(
        event_type="user_activity",
        partition_key=user_id,
        producer_version=producer_version,
        schema_version=schema_version,
        source_system=source_system,
    )

    return UserActivityEvent(
        **base,
        user_id=user_id,
        session_id=session_id,
        action_type=action_type,
        channel=channel,
        platform=platform,
        device_type=device_type,
        page=page,
        region=region,
        country_code=country_code,
        is_authenticated=is_authenticated,
        referrer=_random_referrer(page),
        campaign_id=_random_campaign_id(channel),
        language=language,
        experiment_assignments=_random_experiments(),
        event_properties=_random_event_properties(action_type),
    )


def generate_user_activity_payload(
    *,
    producer_version: str,
    schema_version: int,
    source_system: str,
) -> dict[str, Any]:
    event = build_user_activity_event(
        producer_version=producer_version,
        schema_version=schema_version,
        source_system=source_system,
    )
    payload = asdict(event)
    payload["experiment_assignments"] = [asdict(item) for item in event.experiment_assignments]
    return payload