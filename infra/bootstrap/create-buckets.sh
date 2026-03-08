#!/usr/bin/env bash

set -euo pipefail

echo "Initializing MinIO buckets for Unified Streaming Lakehouse Platform"

MINIO_ALIAS="${MINIO_ALIAS:-local}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minio}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minio123}"

LAKEHOUSE_BUCKET="${LAKEHOUSE_BUCKET:-lakehouse}"
CHECKPOINTS_BUCKET="${CHECKPOINTS_BUCKET:-checkpoints}"

BRONZE_PREFIX="${BRONZE_PREFIX:-warehouse/bronze}"
SILVER_PREFIX="${SILVER_PREFIX:-warehouse/silver}"
GOLD_PREFIX="${GOLD_PREFIX:-warehouse/gold}"

FLINK_CHECKPOINT_PREFIX="${FLINK_CHECKPOINT_PREFIX:-flink}"
FLINK_SAVEPOINT_PREFIX="${FLINK_SAVEPOINT_PREFIX:-savepoints}"

mc alias set "${MINIO_ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

echo "Creating required buckets"
mc mb --ignore-existing "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}"
mc mb --ignore-existing "${MINIO_ALIAS}/${CHECKPOINTS_BUCKET}"

echo "Applying private access policy to buckets"
mc anonymous set private "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}"
mc anonymous set private "${MINIO_ALIAS}/${CHECKPOINTS_BUCKET}"

echo "Creating lakehouse directory structure"
mc mb --ignore-existing "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}/${BRONZE_PREFIX}"
mc mb --ignore-existing "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}/${SILVER_PREFIX}"
mc mb --ignore-existing "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}/${GOLD_PREFIX}"

echo "Creating Flink state directory structure"
mc mb --ignore-existing "${MINIO_ALIAS}/${CHECKPOINTS_BUCKET}/${FLINK_CHECKPOINT_PREFIX}"
mc mb --ignore-existing "${MINIO_ALIAS}/${CHECKPOINTS_BUCKET}/${FLINK_SAVEPOINT_PREFIX}"

echo "MinIO bucket initialization complete"