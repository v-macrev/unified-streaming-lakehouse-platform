#!/usr/bin/env bash

set -euo pipefail

echo "Initializing Kafka topics for Unified Streaming Lakehouse Platform"

BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}

PARTITIONS=${KAFKA_NUM_PARTITIONS:-3}
REPLICATION=${KAFKA_DEFAULT_REPLICATION_FACTOR:-1}

RETENTION_MS=${KAFKA_TOPIC_RETENTION_MS:-604800000}

create_topic () {
  local topic=$1

  echo "Creating topic: $topic"

  /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --replication-factor "$REPLICATION" \
    --partitions "$PARTITIONS" \
    --topic "$topic"

  /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --entity-type topics \
    --entity-name "$topic" \
    --alter \
    --add-config retention.ms="$RETENTION_MS"
}

create_topic "events.user_activity.v1"
create_topic "events.orders.v1"
create_topic "events.payments.v1"
create_topic "events.device_telemetry.v1"

echo "Kafka topic initialization complete"