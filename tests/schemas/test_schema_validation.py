from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from jsonschema import Draft202012Validator

from event_producer.src.generators.device_telemetry import generate_device_telemetry_payload
from event_producer.src.generators.orders import generate_order_payload
from event_producer.src.generators.payments import generate_payment_payload
from event_producer.src.generators.user_activity import generate_user_activity_payload


TEST_PRODUCER_VERSION = "test-1.0.0"
TEST_SCHEMA_VERSION = 1
TEST_SOURCE_SYSTEM = "test-generator"


def _common_kwargs() -> dict[str, str | int]:
    return {
        "producer_version": TEST_PRODUCER_VERSION,
        "schema_version": TEST_SCHEMA_VERSION,
        "source_system": TEST_SOURCE_SYSTEM,
    }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_schema(filename: str) -> dict[str, Any]:
    schema_path = _repo_root() / "schemas" / filename
    with schema_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _validate_payload(payload: dict[str, Any], schema_filename: str) -> None:
    schema = _load_schema(schema_filename)
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda err: list(err.path))

    if errors:
        formatted_errors = "\n".join(
            f"- path={list(error.path)} message={error.message}"
            for error in errors
        )
        raise AssertionError(
            f"Payload failed schema validation for {schema_filename}:\n{formatted_errors}\n"
            f"Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}"
        )


def test_user_activity_payload_conforms_to_schema() -> None:
    payload = generate_user_activity_payload(**_common_kwargs())
    _validate_payload(payload, "user_activity.schema.json")


def test_order_payload_conforms_to_schema() -> None:
    payload = generate_order_payload(**_common_kwargs())
    _validate_payload(payload, "orders.schema.json")


def test_payment_payload_conforms_to_schema() -> None:
    payload = generate_payment_payload(**_common_kwargs())
    _validate_payload(payload, "payments.schema.json")


def test_device_telemetry_payload_conforms_to_schema() -> None:
    payload = generate_device_telemetry_payload(**_common_kwargs())
    _validate_payload(payload, "device_telemetry.schema.json")


def test_all_schema_files_are_loadable_as_valid_json() -> None:
    schema_dir = _repo_root() / "schemas"
    schema_files = sorted(schema_dir.glob("*.schema.json"))

    assert schema_files, "No schema files were found under schemas/."

    for schema_file in schema_files:
        with schema_file.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        assert isinstance(loaded, dict), f"Schema file {schema_file.name} did not load as a JSON object."


def test_multiple_generated_payloads_remain_schema_compliant() -> None:
    cases: list[tuple[str, Callable[..., dict[str, Any]], str]] = [
        ("user_activity", generate_user_activity_payload, "user_activity.schema.json"),
        ("orders", generate_order_payload, "orders.schema.json"),
        ("payments", generate_payment_payload, "payments.schema.json"),
        ("device_telemetry", generate_device_telemetry_payload, "device_telemetry.schema.json"),
    ]

    for name, generator, schema_filename in cases:
        for _ in range(10):
            payload = generator(**_common_kwargs())
            try:
                _validate_payload(payload, schema_filename)
            except AssertionError as exc:
                raise AssertionError(f"Generated payload for {name} failed repeated schema validation.") from exc