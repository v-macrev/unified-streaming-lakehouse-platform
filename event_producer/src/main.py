from __future__ import annotations

import logging
import random
import signal
import sys
import time
from collections.abc import Callable
from typing import Any

from dotenv import load_dotenv

from event_producer.src.config import AppConfig, load_config
from event_producer.src.generators.device_telemetry import generate_device_telemetry_payload
from event_producer.src.generators.orders import generate_order_payload
from event_producer.src.generators.payments import generate_payment_payload
from event_producer.src.generators.user_activity import generate_user_activity_payload
from event_producer.src.producer import EventProducer


PayloadGenerator = Callable[..., dict[str, Any]]


logger = logging.getLogger(__name__)
_SHOULD_STOP = False


def _configure_logging(config: AppConfig) -> None:
    log_level_name = config.runtime.log_level.upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _handle_shutdown_signal(signum: int, _frame: Any) -> None:
    global _SHOULD_STOP
    _SHOULD_STOP = True
    logger.info("Shutdown signal received", extra={"signal": signum})


def _register_signal_handlers() -> None:
    signal.signal(signal.SIGINT, _handle_shutdown_signal)
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)


def _generator_plan(config: AppConfig) -> list[tuple[str, PayloadGenerator]]:
    return [
        (config.topics.user_activity, generate_user_activity_payload),
        (config.topics.orders, generate_order_payload),
        (config.topics.payments, generate_payment_payload),
        (config.topics.device_telemetry, generate_device_telemetry_payload),
    ]


def _common_generator_kwargs(config: AppConfig) -> dict[str, str | int]:
    return {
        "producer_version": config.runtime.producer_version,
        "schema_version": config.runtime.schema_version,
        "source_system": config.runtime.source_system,
    }


def _run_publish_cycle(
    *,
    event_producer: EventProducer,
    config: AppConfig,
) -> int:
    published_count = 0
    generators = _generator_plan(config)
    generator_kwargs = _common_generator_kwargs(config)

    for _ in range(config.runtime.records_per_cycle):
        topic, generator = random.choice(generators)
        payload = generator(**generator_kwargs)
        event_producer.publish(topic, payload)
        published_count += 1

    return published_count


def run() -> int:
    load_dotenv()

    try:
        config = load_config()
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        logger.exception("Failed to load producer configuration")
        return 1

    _configure_logging(config)
    _register_signal_handlers()

    logger.info(
        "Starting synthetic event producer",
        extra={
            "app_name": config.runtime.app_name,
            "bootstrap_servers": config.kafka.bootstrap_servers,
            "records_per_cycle": config.runtime.records_per_cycle,
            "emit_interval_ms": config.runtime.emit_interval_ms,
        },
    )

    producer = EventProducer(config)
    total_published = 0
    sleep_seconds = config.runtime.emit_interval_ms / 1000.0

    try:
        while not _SHOULD_STOP:
            cycle_count = _run_publish_cycle(event_producer=producer, config=config)
            total_published += cycle_count

            logger.info(
                "Completed publish cycle",
                extra={
                    "cycle_published": cycle_count,
                    "total_published": total_published,
                },
            )

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        logger.info("Producer interrupted by keyboard signal")
    except Exception:
        logger.exception("Unhandled error in producer runtime")
        return 1
    finally:
        logger.info("Flushing producer before shutdown", extra={"total_published": total_published})
        producer.flush()

    logger.info("Producer shutdown complete", extra={"total_published": total_published})
    return 0


if __name__ == "__main__":
    sys.exit(run())