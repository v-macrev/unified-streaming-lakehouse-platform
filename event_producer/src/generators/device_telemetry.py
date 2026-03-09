from __future__ import annotations

import random
from dataclasses import asdict
from typing import Any

from faker import Faker

from event_producer.src.models.envelopes import (
    DeviceTelemetryEvent,
    build_base_envelope,
)

fake = Faker()

METRIC_NAMES = [
    "cpu_usage_pct",
    "memory_usage_mb",
    "battery_level_pct",
    "network_latency_ms",
    "app_launch_time_ms",
    "frame_drop_count",
    "crash_count",
    "api_error_count",
    "disk_usage_pct",
    "temperature_c",
]

METRIC_TYPES = {
    "cpu_usage_pct": "gauge",
    "memory_usage_mb": "gauge",
    "battery_level_pct": "gauge",
    "network_latency_ms": "gauge",
    "app_launch_time_ms": "gauge",
    "frame_drop_count": "counter",
    "crash_count": "counter",
    "api_error_count": "counter",
    "disk_usage_pct": "gauge",
    "temperature_c": "gauge",
}

METRIC_UNITS = {
    "cpu_usage_pct": "pct",
    "memory_usage_mb": "mb",
    "battery_level_pct": "pct",
    "network_latency_ms": "ms",
    "app_launch_time_ms": "ms",
    "frame_drop_count": "count",
    "crash_count": "count",
    "api_error_count": "count",
    "disk_usage_pct": "pct",
    "temperature_c": "celsius",
}

PLATFORMS = ["android", "ios", "web", "desktop"]

DEVICE_TYPES_BY_PLATFORM = {
    "android": ["mobile", "tablet"],
    "ios": ["mobile", "tablet"],
    "web": ["desktop", "mobile", "tablet"],
    "desktop": ["desktop"],
}

OS_BY_PLATFORM = {
    "android": ["android"],
    "ios": ["ios"],
    "web": ["windows", "macos", "linux"],
    "desktop": ["windows", "macos", "linux"],
}

NETWORK_TYPES = ["wifi", "4g", "5g", "ethernet", "offline"]

REGIONS = [
    ("north", "BR"),
    ("northeast", "BR"),
    ("midwest", "BR"),
    ("southeast", "BR"),
    ("south", "BR"),
]

ANDROID_MODELS = [
    ("Samsung", "SM-S918B"),
    ("Motorola", "Edge 40"),
    ("Xiaomi", "Redmi Note 13"),
    ("Google", "Pixel 8"),
]

IOS_MODELS = [
    ("Apple", "iPhone 15"),
    ("Apple", "iPhone 14"),
    ("Apple", "iPad Air"),
]

DESKTOP_MODELS = [
    ("Dell", "XPS 15"),
    ("Lenovo", "ThinkPad T14"),
    ("Apple", "MacBook Pro"),
    ("HP", "EliteBook 840"),
]


def _random_device_id() -> str:
    return f"device_{random.randint(100000, 999999)}"


def _random_user_id() -> str | None:
    return None if random.random() < 0.25 else f"user_{random.randint(10000, 99999)}"


def _random_session_id() -> str | None:
    return None if random.random() < 0.20 else f"sess_{random.randint(1000000, 9999999)}"


def _random_platform() -> str:
    return random.choice(PLATFORMS)


def _random_device_type(platform: str) -> str:
    return random.choice(DEVICE_TYPES_BY_PLATFORM[platform])


def _random_os_name(platform: str) -> str:
    return random.choice(OS_BY_PLATFORM[platform])


def _random_os_version(os_name: str) -> str:
    if os_name == "android":
        return random.choice(["12", "13", "14", "15"])
    if os_name == "ios":
        return random.choice(["16.7", "17.4", "17.5", "18.0"])
    if os_name == "windows":
        return random.choice(["10", "11"])
    if os_name == "macos":
        return random.choice(["13.6", "14.4", "15.0"])
    return random.choice(["5.15", "6.1", "6.8"])


def _random_model(platform: str) -> tuple[str | None, str | None]:
    if platform == "android":
        return random.choice(ANDROID_MODELS)
    if platform == "ios":
        return random.choice(IOS_MODELS)
    return random.choice(DESKTOP_MODELS)


def _random_app_version() -> str:
    return f"{random.randint(1, 4)}.{random.randint(0, 9)}.{random.randint(0, 9)}"


def _random_metric_name() -> str:
    return random.choice(METRIC_NAMES)


def _random_metric_value(metric_name: str) -> float:
    if metric_name == "cpu_usage_pct":
        return round(random.uniform(5.0, 99.0), 2)
    if metric_name == "memory_usage_mb":
        return round(random.uniform(120.0, 4096.0), 2)
    if metric_name == "battery_level_pct":
        return round(random.uniform(1.0, 100.0), 2)
    if metric_name == "network_latency_ms":
        return round(random.uniform(15.0, 1200.0), 2)
    if metric_name == "app_launch_time_ms":
        return round(random.uniform(200.0, 5000.0), 2)
    if metric_name == "frame_drop_count":
        return float(random.randint(0, 250))
    if metric_name == "crash_count":
        return float(random.choices([0, 1, 2], weights=[88, 10, 2], k=1)[0])
    if metric_name == "api_error_count":
        return float(random.choices([0, 1, 2, 3], weights=[80, 12, 6, 2], k=1)[0])
    if metric_name == "disk_usage_pct":
        return round(random.uniform(10.0, 98.0), 2)
    if metric_name == "temperature_c":
        return round(random.uniform(24.0, 96.0), 2)
    return 0.0


def _derive_severity(metric_name: str, metric_value: float) -> str | None:
    if metric_name == "network_latency_ms":
        if metric_value >= 900:
            return "critical"
        if metric_value >= 450:
            return "warning"
        return "info"

    if metric_name == "app_launch_time_ms":
        if metric_value >= 3500:
            return "critical"
        if metric_value >= 1800:
            return "warning"
        return "info"

    if metric_name in {"cpu_usage_pct", "disk_usage_pct"}:
        if metric_value >= 92:
            return "critical"
        if metric_value >= 80:
            return "warning"
        return "info"

    if metric_name == "temperature_c":
        if metric_value >= 85:
            return "critical"
        if metric_value >= 70:
            return "warning"
        return "info"

    if metric_name in {"crash_count", "api_error_count", "frame_drop_count"}:
        if metric_value >= 5:
            return "critical"
        if metric_value >= 1:
            return "warning"
        return "info"

    return None


def _derive_threshold_breached(severity: str | None) -> bool:
    return severity in {"warning", "critical"}


def _random_network_type(platform: str) -> str | None:
    if platform in {"desktop", "web"}:
        return random.choice(["wifi", "ethernet", "offline", None])
    return random.choice(NETWORK_TYPES)


def _random_event_properties(metric_name: str) -> dict[str, str | int | float | bool | None]:
    properties: dict[str, str | int | float | bool | None] = {
        "foreground": random.choice([True, False]),
        "sdk_version": f"{random.randint(0, 2)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
        "sample_rate_pct": random.choice([10, 25, 50, 100]),
    }

    if metric_name == "network_latency_ms":
        properties["carrier"] = random.choice(
            ["carrier_br_01", "carrier_br_02", "carrier_br_03", None]
        )
        properties["request_path"] = random.choice(
            ["/api/search", "/api/checkout", "/api/profile", "/api/catalog"]
        )

    if metric_name == "app_launch_time_ms":
        properties["cold_start"] = random.choice([True, False])

    if metric_name in {"crash_count", "api_error_count"}:
        properties["error_domain"] = random.choice(
            ["network", "rendering", "backend", "storage", "unknown"]
        )

    return properties


def build_device_telemetry_event(
    *,
    producer_version: str,
    schema_version: int,
    source_system: str,
) -> DeviceTelemetryEvent:
    platform = _random_platform()
    device_type = _random_device_type(platform)
    os_name = _random_os_name(platform)
    os_version = _random_os_version(os_name)
    manufacturer, model = _random_model(platform)
    metric_name = _random_metric_name()
    metric_value = _random_metric_value(metric_name)
    severity = _derive_severity(metric_name, metric_value)
    region, country_code = random.choice(REGIONS)
    device_id = _random_device_id()

    base = build_base_envelope(
        event_type="device_telemetry",
        partition_key=device_id,
        producer_version=producer_version,
        schema_version=schema_version,
        source_system=source_system,
    )

    return DeviceTelemetryEvent(
        **base,
        device_id=device_id,
        metric_name=metric_name,
        metric_type=METRIC_TYPES[metric_name],
        metric_value=metric_value,
        metric_unit=METRIC_UNITS[metric_name],
        platform=platform,
        device_type=device_type,
        app_version=_random_app_version(),
        os_name=os_name,
        os_version=os_version,
        region=region,
        country_code=country_code,
        user_id=_random_user_id(),
        session_id=_random_session_id(),
        manufacturer=manufacturer,
        model=model,
        network_type=_random_network_type(platform),
        severity=severity,
        threshold_breached=_derive_threshold_breached(severity),
        event_properties=_random_event_properties(metric_name),
    )


def generate_device_telemetry_payload(
    *,
    producer_version: str,
    schema_version: int,
    source_system: str,
) -> dict[str, Any]:
    event = build_device_telemetry_event(
        producer_version=producer_version,
        schema_version=schema_version,
        source_system=source_system,
    )
    return asdict(event)