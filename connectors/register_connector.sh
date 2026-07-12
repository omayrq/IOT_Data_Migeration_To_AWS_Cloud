#!/bin/bash
# =============================================================================
# register_connector.sh — Register / update Kafka Connect connectors
# Usage: ./register_connector.sh [connector_json_file]
#        ./register_connector.sh all                     (register all)
# =============================================================================
set -euo pipefail

CONNECT_HOST="${CONNECT_HOST:-localhost}"
CONNECT_PORT="${CONNECT_PORT:-8083}"
BASE_URL="http://${CONNECT_HOST}:${CONNECT_PORT}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Helpers ──────────────────────────────────────────────────────────────────
wait_for_connect() {
    echo "⏳ Waiting for Kafka Connect REST API..."
    for i in $(seq 1 30); do
        if curl -sf "${BASE_URL}/connectors" >/dev/null 2>&1; then
            echo "✅ Kafka Connect is ready."
            return 0
        fi
        echo "   Attempt ${i}/30 — sleeping 5s..."
        sleep 5
    done
    echo "❌ Kafka Connect not reachable at ${BASE_URL}"
    exit 1
}

register_connector() {
    local FILE=$1
    local NAME
    NAME=$(python3 -c "import json,sys; print(json.load(open('${FILE}'))['name'])" 2>/dev/null \
           || jq -r '.name' "${FILE}")

    echo ""
    echo "── Registering: ${NAME} (from ${FILE})"

    # Check if connector already exists
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/connectors/${NAME}")

    if [ "${HTTP_CODE}" == "200" ]; then
        echo "   Connector exists — updating config..."
        PAYLOAD=$(python3 -c "import json,sys; d=json.load(open('${FILE}')); print(json.dumps({'config': d['config']}))" 2>/dev/null \
                  || jq '{config: .config}' "${FILE}")
        RESULT=$(curl -s -X PUT \
            -H "Content-Type: application/json" \
            --data "${PAYLOAD}" \
            "${BASE_URL}/connectors/${NAME}/config")
    else
        echo "   Connector not found — creating..."
        RESULT=$(curl -s -X POST \
            -H "Content-Type: application/json" \
            --data @"${FILE}" \
            "${BASE_URL}/connectors")
    fi

    # Show result
    echo "${RESULT}" | python3 -m json.tool 2>/dev/null || echo "${RESULT}"

    # Verify state
    sleep 2
    STATE=$(curl -s "${BASE_URL}/connectors/${NAME}/status" \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null \
        || echo "UNKNOWN")
    echo "   Connector state: ${STATE}"

    if [[ "${STATE}" == "RUNNING" ]]; then
        echo "   ✅ ${NAME} is RUNNING"
    else
        echo "   ⚠️  ${NAME} state is ${STATE} — check logs"
    fi
}

show_status() {
    echo ""
    echo "============================================="
    echo " Connector Status Summary"
    echo "============================================="
    CONNECTORS=$(curl -s "${BASE_URL}/connectors" \
        | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))" 2>/dev/null \
        || echo "none")

    for CONN in ${CONNECTORS}; do
        STATUS=$(curl -s "${BASE_URL}/connectors/${CONN}/status")
        STATE=$(echo "${STATUS}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null)
        TASKS=$(echo "${STATUS}" | python3 -c "import json,sys; d=json.load(sys.stdin); t=d.get('tasks',[]); ok=sum(1 for x in t if x['state']=='RUNNING'); print(f'{ok}/{len(t)} tasks running')" 2>/dev/null || echo "?")
        echo "  ${CONN}: ${STATE} (${TASKS})"
    done
}

# ── Main ─────────────────────────────────────────────────────────────────────
echo "============================================="
echo " Kafka Connect — Connector Registration"
echo " Target: ${BASE_URL}"
echo "============================================="

wait_for_connect

ARG="${1:-all}"

case "${ARG}" in
    all)
        for f in \
            "${SCRIPT_DIR}/jdbc-sink-connector.json" \
            "${SCRIPT_DIR}/s3-sink-connector.json" \
            "${SCRIPT_DIR}/snowflake-kafka-connector.json"; do
            # Debezium is registered separately (see debezium/)
            if [ -f "${f}" ]; then
                register_connector "${f}"
            else
                echo "   ⚠️  Skipping ${f} (not found)"
            fi
        done
        ;;
    *.json)
        register_connector "${SCRIPT_DIR}/${ARG}"
        ;;
    *)
        echo "Usage: $0 [connector.json | all]"
        exit 1
        ;;
esac

show_status
echo ""
echo "Done. Visit http://${CONNECT_HOST}:8083/connectors for full details."
