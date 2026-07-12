"""
timestream_writer.py — Lambda: Snowflake Gold → Amazon Timestream
=================================================================
Triggered every 5 minutes by EventBridge.
Reads latest device metrics from Snowflake Gold layer and writes
them to Amazon Timestream for Grafana time-series visualization.

Timestream structure:
  Database: IoTStreamingDB
  Tables:
    - device_telemetry  (temperature, humidity, battery, signal_strength)
    - device_health     (health_score, uptime_pct, active_hours)
    - anomaly_counts    (count per anomaly_type per region)
"""

import boto3
import json
import logging
import os
import time
from datetime import datetime, timezone

import snowflake.connector

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────────────────────
TIMESTREAM_DATABASE = os.environ.get("TIMESTREAM_DATABASE", "IoTStreamingDB")
TIMESTREAM_TELEMETRY_TABLE = os.environ.get(
    "TIMESTREAM_TELEMETRY_TABLE", "device_telemetry"
)
TIMESTREAM_HEALTH_TABLE = os.environ.get("TIMESTREAM_HEALTH_TABLE", "device_health")
TIMESTREAM_ANOMALY_TABLE = os.environ.get("TIMESTREAM_ANOMALY_TABLE", "anomaly_counts")

SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]


# ── Clients ───────────────────────────────────────────────────────────────────
timestream = boto3.client(
    "timestream-write", region_name=os.environ.get("AWS_REGION", "us-east-1")
)


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role="IOT_ANALYST_ROLE",
        warehouse="IOT_ANALYTICS_WH",
        database="IOT_GOLD",
        schema="KPI",
    )


def query_snowflake(conn, sql: str) -> list[dict]:
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute(sql)
    return cursor.fetchall()


def current_time_ms() -> str:
    return str(int(time.time() * 1000))


def build_telemetry_records(rows: list[dict]) -> list[dict]:
    """Convert Snowflake device summary rows → Timestream records."""
    records = []
    now_ms = current_time_ms()

    for row in rows:
        device_id = str(row.get("DEVICE_ID", "unknown"))
        region = str(row.get("REGION", "unknown"))

        dimensions = [
            {"Name": "device_id", "Value": device_id},
            {"Name": "region", "Value": region},
        ]

        metrics = {
            "avg_temperature": row.get("AVG_TEMPERATURE"),
            "max_temperature": row.get("MAX_TEMPERATURE"),
            "min_temperature": row.get("MIN_TEMPERATURE"),
            "avg_humidity": row.get("AVG_HUMIDITY"),
            "min_battery": row.get("MIN_BATTERY"),
            "battery_drain": row.get("BATTERY_DRAIN_TODAY"),
            "avg_signal": row.get("AVG_SIGNAL_STRENGTH"),
        }

        for measure_name, measure_value in metrics.items():
            if measure_value is not None:
                records.append(
                    {
                        "Dimensions": dimensions,
                        "MeasureName": measure_name,
                        "MeasureValue": str(float(measure_value)),
                        "MeasureValueType": "DOUBLE",
                        "Time": now_ms,
                        "TimeUnit": "MILLISECONDS",
                    }
                )

    return records


def build_health_records(rows: list[dict]) -> list[dict]:
    records = []
    now_ms = current_time_ms()

    for row in rows:
        device_id = str(row.get("DEVICE_ID", "unknown"))
        region = str(row.get("REGION", "unknown"))

        dimensions = [
            {"Name": "device_id", "Value": device_id},
            {"Name": "region", "Value": region},
        ]

        metrics = {
            "health_score": row.get("DEVICE_HEALTH_SCORE"),
            "uptime_pct": row.get("UPTIME_PCT"),
            "active_hours": row.get("ACTIVE_HOURS"),
            "total_events": row.get("TOTAL_EVENTS"),
        }

        for measure_name, measure_value in metrics.items():
            if measure_value is not None:
                records.append(
                    {
                        "Dimensions": dimensions,
                        "MeasureName": measure_name,
                        "MeasureValue": str(float(measure_value)),
                        "MeasureValueType": "DOUBLE",
                        "Time": now_ms,
                        "TimeUnit": "MILLISECONDS",
                    }
                )

    return records


def build_anomaly_records(rows: list[dict]) -> list[dict]:
    records = []
    now_ms = current_time_ms()

    for row in rows:
        anomaly_type = str(row.get("ANOMALY_TYPE", "UNKNOWN"))
        region = str(row.get("REGION", "unknown"))
        count = row.get("ANOMALY_COUNT", 0)

        records.append(
            {
                "Dimensions": [
                    {"Name": "anomaly_type", "Value": anomaly_type},
                    {"Name": "region", "Value": region},
                ],
                "MeasureName": "anomaly_count",
                "MeasureValue": str(int(count)),
                "MeasureValueType": "BIGINT",
                "Time": now_ms,
                "TimeUnit": "MILLISECONDS",
            }
        )

    return records


def write_to_timestream(table_name: str, records: list[dict]) -> int:
    """Write records in batches of 100 (Timestream API limit)."""
    if not records:
        return 0

    written = 0
    batch_size = 100

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        try:
            response = timestream.write_records(
                DatabaseName=TIMESTREAM_DATABASE,
                TableName=table_name,
                Records=batch,
                CommonAttributes={},
            )
            written += response.get("RecordsIngested", {}).get("Total", len(batch))
        except timestream.exceptions.RejectedRecordsException as e:
            logger.warning(
                f"Rejected {len(e.response['RejectedRecords'])} records in {table_name}"
            )
            written += len(batch) - len(e.response["RejectedRecords"])
        except Exception as e:
            logger.error(f"Timestream write error for {table_name}: {e}")

    return written


def ensure_timestream_resources():
    """Create Timestream database and tables if they don't exist."""
    try:
        timestream.create_database(DatabaseName=TIMESTREAM_DATABASE)
        logger.info(f"Created Timestream database: {TIMESTREAM_DATABASE}")
    except timestream.exceptions.ConflictException:
        pass  # Already exists

    table_configs = [
        (
            TIMESTREAM_TELEMETRY_TABLE,
            24 * 3600,
            365 * 24 * 3600,
        ),  # 1 day memory, 1 year magnetic
        (TIMESTREAM_HEALTH_TABLE, 24 * 3600, 365 * 24 * 3600),
        (TIMESTREAM_ANOMALY_TABLE, 24 * 3600, 90 * 24 * 3600),
    ]

    for table_name, memory_hours, magnetic_hours in table_configs:
        try:
            timestream.create_table(
                DatabaseName=TIMESTREAM_DATABASE,
                TableName=table_name,
                RetentionProperties={
                    "MemoryStoreRetentionPeriodInHours": memory_hours // 3600,
                    "MagneticStoreRetentionPeriodInDays": magnetic_hours // 86400,
                },
            )
            logger.info(f"Created Timestream table: {table_name}")
        except timestream.exceptions.ConflictException:
            pass  # Already exists


def lambda_handler(event, context):
    logger.info("IoT Timestream Writer invoked")
    start_time = time.time()

    ensure_timestream_resources()

    conn = get_snowflake_conn()
    total_written = 0

    try:
        # 1. Device telemetry & health (today's summary)
        telemetry_rows = query_snowflake(
            conn,
            """
            SELECT DEVICE_ID, REGION, AVG_TEMPERATURE, MAX_TEMPERATURE, MIN_TEMPERATURE,
                   AVG_HUMIDITY, MIN_BATTERY, BATTERY_DRAIN_TODAY, AVG_SIGNAL_STRENGTH,
                   DEVICE_HEALTH_SCORE, UPTIME_PCT, ACTIVE_HOURS, TOTAL_EVENTS
            FROM IOT_GOLD.KPI.IOT_DEVICE_SUMMARY
            WHERE EVENT_DATE = CURRENT_DATE()
            LIMIT 200
        """,
        )

        telemetry_records = build_telemetry_records(telemetry_rows)
        health_records = build_health_records(telemetry_rows)

        total_written += write_to_timestream(
            TIMESTREAM_TELEMETRY_TABLE, telemetry_records
        )
        total_written += write_to_timestream(TIMESTREAM_HEALTH_TABLE, health_records)
        logger.info(f"Wrote {len(telemetry_rows)} device metrics to Timestream")

        # 2. Anomaly counts (last hour, by type + region)
        anomaly_rows = query_snowflake(
            conn,
            """
            SELECT ANOMALY_TYPE, REGION, COUNT(*) AS ANOMALY_COUNT
            FROM IOT_GOLD.KPI.IOT_ANOMALIES
            WHERE EVENT_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
            GROUP BY ANOMALY_TYPE, REGION
        """,
        )

        anomaly_records = build_anomaly_records(anomaly_rows)
        total_written += write_to_timestream(TIMESTREAM_ANOMALY_TABLE, anomaly_records)
        logger.info(f"Wrote {len(anomaly_rows)} anomaly metrics to Timestream")

    finally:
        conn.close()

    elapsed = round(time.time() - start_time, 2)
    logger.info(f"Done. Total records written: {total_written} in {elapsed}s")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "records_written": total_written,
                "elapsed_seconds": elapsed,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ),
    }
