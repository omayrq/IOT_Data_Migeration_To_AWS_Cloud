"""
IoT Device Simulator (Task 1.2 stand-in for local development)
================================================================
In AWS, real telemetry flows:
    AWS IoT Device Simulator -> MQTT -> AWS IoT Core -> MSK (iot-events)
        -> Kafka Connect JDBC Sink -> PostgreSQL EC2

For local development/demo we simulate the *end result* of that chain by
writing rows straight into Postgres with psycopg2. This is enough to drive
the Phase 2 CDC pipeline (Debezium reads the WAL regardless of how the row
got there), and the AWS CDK stacks in /phase1-aws-cdk stand up the real
managed-service version of the ingestion path.

Run:
    pip install psycopg2-binary
    python simulate_devices.py --devices 5 --interval 2
"""
import argparse
import random
import time
from datetime import datetime, timezone

import psycopg2

# Base coordinates: O2 Arena, London (per hackathon spec)
BASE_LAT, BASE_LON = 51.5030, -0.0032

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "iot_db",
    "user": "iot_admin",
    "password": "iot_password",
}


def classify_severity(aqi: float) -> str:
    if aqi >= 100:
        return "critical"
    if aqi >= 60:
        return "warning"
    return "normal"


def generate_reading(device_id: str) -> dict:
    aqi = max(0, random.gauss(55, 25))
    return {
        "device_id": device_id,
        "latitude": BASE_LAT + random.uniform(-0.01, 0.01),
        "longitude": BASE_LON + random.uniform(-0.01, 0.01),
        "aqi": round(aqi, 2),
        "temperature_c": round(random.uniform(15, 30), 2),
        "battery_pct": round(random.uniform(20, 100), 2),
        "severity": classify_severity(aqi),
        "event_ts": datetime.now(timezone.utc),
    }


def main(num_devices: int, interval: float, iterations: int | None):
    device_ids = [f"device-{i:03d}" for i in range(1, num_devices + 1)]
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO public.iot_events
            (device_id, latitude, longitude, aqi, temperature_c, battery_pct, severity, event_ts)
        VALUES (%(device_id)s, %(latitude)s, %(longitude)s, %(aqi)s,
                %(temperature_c)s, %(battery_pct)s, %(severity)s, %(event_ts)s)
    """

    print(f"Streaming telemetry for {num_devices} devices every {interval}s. Ctrl+C to stop.")
    count = 0
    try:
        while iterations is None or count < iterations:
            for device_id in device_ids:
                reading = generate_reading(device_id)
                cur.execute(insert_sql, reading)
                print(f"[{reading['event_ts'].isoformat()}] {device_id} "
                      f"aqi={reading['aqi']} severity={reading['severity']}")
            count += 1
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--devices", type=int, default=5, help="number of virtual devices")
    parser.add_argument("--interval", type=float, default=2.0, help="seconds between rounds")
    parser.add_argument("--iterations", type=int, default=None, help="rounds to run (default: infinite)")
    args = parser.parse_args()
    main(args.devices, args.interval, args.iterations)
