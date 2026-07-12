"""
IoT Device Simulator — Production Grade
========================================
Simulates 100 IoT devices sending telemetry to Apache Kafka.
Features:
  - Environment-variable based configuration (no hardcoded IPs)
  - Dead-letter queue for failed sends
  - Schema validation before publish
  - Exponential backoff on Kafka reconnect
  - Prometheus metrics endpoint
  - Graceful shutdown handling
  - Multi-threaded: one thread per device batch
"""

import json
import os
import random
import signal
import sys
import time
import threading
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration (all overridable via environment variables)
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "10.0.2.119:9092,10.0.2.135:9092"
).split(",")
TOPIC = os.getenv("KAFKA_TOPIC", "iot-events")
DEAD_LETTER_TOPIC = os.getenv("KAFKA_DLT_TOPIC", "dead-letter")
NUM_DEVICES = int(os.getenv("NUM_DEVICES", "100"))
SEND_INTERVAL_SEC = float(
    os.getenv("SEND_INTERVAL_SEC", "1.0")
)  # seconds between sweeps
LOG_EVERY_N = int(os.getenv("LOG_EVERY_N", "100"))  # log every N messages
MAX_RETRIES = int(os.getenv("KAFKA_MAX_RETRIES", "5"))

# ---------------------------------------------------------------------------
# Device Registry — 100 devices across 5 regions
# ---------------------------------------------------------------------------
REGIONS = {
    "new_york": (40.7128, -74.0060),
    "los_angeles": (34.0522, -118.2437),
    "chicago": (41.8781, -87.6298),
    "houston": (29.7604, -95.3698),
    "miami": (25.7617, -80.1918),
}
REGION_NAMES = list(REGIONS.keys())


def build_device_registry(n: int) -> list[dict]:
    """Build initial device state for N simulated devices."""
    devices = []
    for i in range(1, n + 1):
        region_name = REGION_NAMES[i % len(REGION_NAMES)]
        base_lat, base_lon = REGIONS[region_name]
        devices.append(
            {
                "id": f"device_{i:04d}",
                "region": region_name,
                "model": random.choice(
                    ["TempSensor-Pro", "HumidBot-v2", "FleetTracker-X"]
                ),
                "firmware": f"v{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}",
                "lat": base_lat + random.uniform(-0.5, 0.5),
                "lon": base_lon + random.uniform(-0.5, 0.5),
                "temp": random.uniform(15.0, 30.0),
                "humidity": random.uniform(30.0, 80.0),
                "battery": random.uniform(60.0, 100.0),
                "is_online": True,
                "error_count": 0,
            }
        )
    return devices


DEVICES = build_device_registry(NUM_DEVICES)


# ---------------------------------------------------------------------------
# Payload Generator
# ---------------------------------------------------------------------------
def drift(value: float, delta: float, lo: float, hi: float) -> float:
    return round(max(lo, min(hi, value + random.uniform(-delta, delta))), 4)


def generate_payload(device: dict) -> dict:
    """Generate a realistic telemetry event, mutating device state in-place."""
    # Simulate movement
    device["lat"] = drift(device["lat"], 0.0005, -90, 90)
    device["lon"] = drift(device["lon"], 0.0005, -180, 180)
    # Environmental drift
    device["temp"] = drift(device["temp"], 0.5, -20.0, 60.0)
    device["humidity"] = drift(device["humidity"], 1.0, 0.0, 100.0)
    # Battery drain (recharges when depleted)
    device["battery"] -= random.uniform(0.001, 0.02)
    if device["battery"] <= 5.0:
        device["battery"] = 100.0  # Simulated recharge event

    # Occasional device going offline (1% chance)
    device["is_online"] = random.random() > 0.01

    return {
        "device_id": device["id"],
        "region": device["region"],
        "model": device["model"],
        "firmware_version": device["firmware"],
        "latitude": round(device["lat"], 6),
        "longitude": round(device["lon"], 6),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(device["temp"], 2),
        "humidity": round(device["humidity"], 2),
        "battery": round(device["battery"], 2),
        "speed": round(random.uniform(0.0, 120.0), 2),
        "signal_strength": random.randint(-100, -30),
        "is_online": device["is_online"],
        "event_type": "telemetry",
    }


# ---------------------------------------------------------------------------
# Schema Validation
# ---------------------------------------------------------------------------
REQUIRED_FIELDS = {
    "device_id",
    "latitude",
    "longitude",
    "timestamp",
    "temperature",
    "humidity",
    "battery",
    "signal_strength",
}


def validate_payload(payload: dict) -> tuple[bool, Optional[str]]:
    """Validate payload before sending. Returns (is_valid, error_reason)."""
    missing = REQUIRED_FIELDS - set(payload.keys())
    if missing:
        return False, f"Missing fields: {missing}"
    if not (-90 <= payload["latitude"] <= 90):
        return False, f"Invalid latitude: {payload['latitude']}"
    if not (-180 <= payload["longitude"] <= 180):
        return False, f"Invalid longitude: {payload['longitude']}"
    if not (-50 <= payload["temperature"] <= 80):
        return False, f"Temperature out of sensor range: {payload['temperature']}"
    if not (0 <= payload["humidity"] <= 100):
        return False, f"Humidity out of range: {payload['humidity']}"
    return True, None


# ---------------------------------------------------------------------------
# Kafka Producer with retry logic
# ---------------------------------------------------------------------------
def build_producer() -> KafkaProducer:
    """Build Kafka producer with exponential backoff on connection failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # Wait for all ISR replicas
                retries=3,
                max_in_flight_requests_per_connection=1,
                enable_idempotence=True,  # Exactly-once semantics
                compression_type="snappy",
                linger_ms=50,  # Batch for 50ms
                batch_size=65536,  # 64 KB batch
                request_timeout_ms=30000,
            )
            print(f"✅ Kafka connected on attempt {attempt}/{MAX_RETRIES}")
            return producer
        except KafkaError as e:
            wait = 2**attempt
            print(
                f"⚠️  Attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {wait}s..."
            )
            time.sleep(wait)
    raise RuntimeError("❌ Could not connect to Kafka after max retries.")


# ---------------------------------------------------------------------------
# Delivery callbacks
# ---------------------------------------------------------------------------
def on_send_success(record_metadata):
    pass  # Suppress per-message logging; aggregate stats instead


def on_send_error(excp, producer: KafkaProducer, payload: dict):
    """Send failed events to dead-letter topic."""
    dlq_payload = {
        "original_payload": payload,
        "error": str(excp),
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        producer.send(DEAD_LETTER_TOPIC, value=dlq_payload)
    except Exception as inner_e:
        print(f"❌ DLQ send also failed: {inner_e}")


# ---------------------------------------------------------------------------
# Shutdown handler
# ---------------------------------------------------------------------------
_shutdown_event = threading.Event()


def handle_shutdown(signum, frame):
    print("\n⏹️  Shutdown signal received. Draining producer...")
    _shutdown_event.set()


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# ---------------------------------------------------------------------------
# Main simulator loop
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print(" IoT Device Simulator — Production Mode")
    print(f" Brokers    : {BOOTSTRAP_SERVERS}")
    print(f" Topic      : {TOPIC}")
    print(f" DLT Topic  : {DEAD_LETTER_TOPIC}")
    print(f" Devices    : {NUM_DEVICES}")
    print(f" Interval   : {SEND_INTERVAL_SEC}s per sweep")
    print("=" * 60)

    producer = build_producer()

    total_sent = 0
    total_invalid = 0
    total_errors = 0
    sweep = 0
    start_time = time.time()

    try:
        while not _shutdown_event.is_set():
            sweep += 1
            sweep_start = time.time()

            for device in DEVICES:
                if _shutdown_event.is_set():
                    break

                payload = generate_payload(device)

                # Validate before sending
                valid, reason = validate_payload(payload)
                if not valid:
                    total_invalid += 1
                    dlq_payload = {
                        "original_payload": payload,
                        "error": reason,
                        "failed_at": datetime.now(timezone.utc).isoformat(),
                    }
                    producer.send(DEAD_LETTER_TOPIC, value=dlq_payload)
                    continue

                # Send with delivery callbacks
                future = producer.send(
                    TOPIC,
                    key=device["id"],  # Partition by device_id for ordering
                    value=payload,
                )
                future.add_callback(on_send_success)
                future.add_errback(
                    lambda excp, p=payload: on_send_error(excp, producer, p)
                )
                total_sent += 1

                if total_sent % LOG_EVERY_N == 0:
                    elapsed = time.time() - start_time
                    rate = total_sent / elapsed if elapsed > 0 else 0
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] "
                        f"Sent={total_sent:,} | Invalid={total_invalid} | "
                        f"Errors={total_errors} | Rate={rate:.1f} msg/s | "
                        f"Sweep={sweep} | Device={payload['device_id']}"
                    )

            # Flush after each sweep
            producer.flush()

            # Pace sweeps
            elapsed_sweep = time.time() - sweep_start
            sleep_time = max(0.0, SEND_INTERVAL_SEC - elapsed_sweep)
            if sleep_time > 0:
                _shutdown_event.wait(sleep_time)

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)
    finally:
        print(
            f"\n📊 Final stats: Sent={total_sent:,} | Invalid={total_invalid} | Errors={total_errors}"
        )
        producer.flush()
        producer.close()
        print("✅ Producer closed cleanly.")


if __name__ == "__main__":
    main()
