#!/usr/bin/env bash

set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}"

PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
REPLICATION="${KAFKA_TOPIC_REPLICATION:-1}"

KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

create_topic () {
  local topic=$1

  echo "Ensuring topic exists: ${topic}"

  "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}"
}

echo "Kafka topic bootstrap starting..."

create_topic "events.user_activity.v1"
create_topic "events.orders.v1"
create_topic "events.payments.v1"
create_topic "events.device_telemetry.v1"

echo "Kafka topic bootstrap completed successfully."