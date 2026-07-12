#!/bin/bash
# =============================================================================
# create_topics.sh — Kafka Topic Provisioning Script
# Run from Bastion or Kafka Broker via SSM session
# Usage: ./create_topics.sh <broker_ip_1> <broker_ip_2>
# =============================================================================
set -euo pipefail

BROKER_1="${1:-10.0.2.119}"
BROKER_2="${2:-10.0.2.135}"
BOOTSTRAP="${BROKER_1}:9092,${BROKER_2}:9092"
KAFKA_BIN="/opt/kafka/bin"

echo "============================================="
echo "IoT Streaming Platform — Kafka Topic Setup"
echo "Bootstrap: $BOOTSTRAP"
echo "============================================="

create_topic() {
    local TOPIC=$1
    local PARTITIONS=$2
    local REPLICATION=$3
    local RETENTION_MS=$4
    local CLEANUP_POLICY="${5:-delete}"

    echo ""
    echo ">> Creating topic: $TOPIC"
    echo "   Partitions=$PARTITIONS | Replication=$REPLICATION | Retention=${RETENTION_MS}ms | Cleanup=$CLEANUP_POLICY"

    ${KAFKA_BIN}/kafka-topics.sh \
        --bootstrap-server "${BOOTSTRAP}" \
        --create \
        --if-not-exists \
        --topic "${TOPIC}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION}" \
        --config "retention.ms=${RETENTION_MS}" \
        --config "cleanup.policy=${CLEANUP_POLICY}" \
        --config "min.insync.replicas=1" \
        --config "compression.type=snappy"

    echo "   ✅ $TOPIC created (or already exists)"
}

# -----------------------------------------------------------------------------
# Core Topics
# -----------------------------------------------------------------------------

# iot-events: High-throughput raw sensor data from 100 devices
# 10 partitions for parallel consumers, 7-day retention
create_topic "iot-events"         10  2  604800000  "delete"

# iot-backup: Mirror of iot-events for S3 sink (separate consumer group)
# Matches iot-events partitioning for 1:1 connector mapping
create_topic "iot-backup"         10  2  604800000  "delete"

# iot-cdc: Debezium PostgreSQL change-data-capture events
# 6 partitions (one per table potential), 30-day retention for replay
create_topic "iot-cdc"            6   2  2592000000 "delete"

# iot-cdc.iot_streaming_db.public.iot_events: Debezium auto-creates this
# We pre-create it so retention & partitions are correct from the start
create_topic "iot-cdc.iot_streaming_db.public.iot_events" 6 2 2592000000 "delete"

# iot-cdc.iot_streaming_db.public.device_registry: Device metadata changes
create_topic "iot-cdc.iot_streaming_db.public.device_registry" 3 2 2592000000 "delete"

# dead-letter: Failed/invalid events from all connectors
# Compact + delete: keeps latest per key, 72hr window to investigate
create_topic "dead-letter"        3   2  259200000  "delete"

# __connect-offsets: Kafka Connect offset storage (internal)
create_topic "__connect-offsets"  25  2  -1         "compact"

# __connect-configs: Kafka Connect config storage (internal)
create_topic "__connect-configs"  1   2  -1         "compact"

# __connect-status: Kafka Connect status storage (internal)
create_topic "__connect-status"   5   2  -1         "compact"

# schema-changes.iot: Debezium schema history topic
create_topic "schema-changes.iot" 1   2  -1         "delete"

# -----------------------------------------------------------------------------
# Verify all topics
# -----------------------------------------------------------------------------
echo ""
echo "============================================="
echo "Listing all topics:"
echo "============================================="
${KAFKA_BIN}/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --list

echo ""
echo "============================================="
echo "Topic Descriptions:"
echo "============================================="
${KAFKA_BIN}/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --describe \
    --topic "iot-events,iot-backup,iot-cdc,dead-letter"

echo ""
echo "✅ All Kafka topics provisioned successfully!"
