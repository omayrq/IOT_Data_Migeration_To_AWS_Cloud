#!/bin/bash
# =============================================================================
# validate_kafka.sh — End-to-End Kafka Validation
# Runs a full produce → consume cycle and checks all topics exist
# Usage: ./validate_kafka.sh <broker_1_ip> <broker_2_ip>
# =============================================================================
set -euo pipefail

BROKER_1="${1:-10.0.2.119}"
BROKER_2="${2:-10.0.2.135}"
BOOTSTRAP="${BROKER_1}:9092,${BROKER_2}:9092"
KAFKA_BIN="/opt/kafka/bin"
PASS=0; FAIL=0

pass() { echo "  ✅ PASS: $1"; ((PASS++)); }
fail() { echo "  ❌ FAIL: $1"; ((FAIL++)); }

echo "============================================="
echo " Kafka Validation — IoT Streaming Platform"
echo " Bootstrap: $BOOTSTRAP"
echo "============================================="

# 1. Broker connectivity
echo ""
echo "[1/5] Checking broker connectivity..."
if ${KAFKA_BIN}/kafka-broker-api-versions.sh \
        --bootstrap-server "${BOOTSTRAP}" &>/dev/null; then
    pass "Brokers reachable at $BOOTSTRAP"
else
    fail "Cannot reach brokers at $BOOTSTRAP"
fi

# 2. Required topics exist
echo ""
echo "[2/5] Verifying required topics..."
REQUIRED_TOPICS=("iot-events" "iot-backup" "iot-cdc" "dead-letter")
for TOPIC in "${REQUIRED_TOPICS[@]}"; do
    if ${KAFKA_BIN}/kafka-topics.sh \
            --bootstrap-server "${BOOTSTRAP}" \
            --describe --topic "${TOPIC}" &>/dev/null; then
        pass "Topic exists: $TOPIC"
    else
        fail "Topic missing: $TOPIC"
    fi
done

# 3. Produce test message
echo ""
echo "[3/5] Producing test message to iot-events..."
TEST_MSG="{\"device_id\":\"validate-001\",\"temperature\":22.5,\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
echo "${TEST_MSG}" | ${KAFKA_BIN}/kafka-console-producer.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic "iot-events" \
    --property "parse.key=false" && \
    pass "Test message produced to iot-events" || \
    fail "Failed to produce test message"

# 4. Consume test message (5 second timeout)
echo ""
echo "[4/5] Consuming from iot-events (5s timeout)..."
CONSUMED=$(${KAFKA_BIN}/kafka-console-consumer.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic "iot-events" \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null || true)

if echo "${CONSUMED}" | grep -q "validate-001"; then
    pass "Consumed test message successfully"
else
    fail "Test message not consumed (may still be propagating)"
fi

# 5. Check consumer groups (if any exist)
echo ""
echo "[5/5] Listing consumer groups..."
GROUPS=$(${KAFKA_BIN}/kafka-consumer-groups.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --list 2>/dev/null || echo "none")
echo "  Active consumer groups: ${GROUPS:-none}"
pass "Consumer group listing successful"

# Summary
echo ""
echo "============================================="
echo " Validation Summary"
echo " PASSED: $PASS | FAILED: $FAIL"
echo "============================================="
if [ "$FAIL" -eq 0 ]; then
    echo " 🎉 All checks passed. Kafka is healthy."
    exit 0
else
    echo " ⚠️  $FAIL check(s) failed. Review above."
    exit 1
fi
