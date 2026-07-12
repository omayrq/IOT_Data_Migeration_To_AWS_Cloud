# IoT Streaming Platform — Operational Runbook

This runbook describes the standard operating procedures, administration scripts, and troubleshooting workflows for the **IoT Streaming Platform**.

---

## 🚀 Deployment & Initialization Order

To spin up the platform from scratch, follow this sequence:

1. **Deploy AWS Infrastructure**:
   ```bash
   cd infrastructure
   npm install -g aws-cdk
   pip install -r requirements.txt
   cdk deploy SecretsStack
   cdk deploy InfrastructureStack
   ```

2. **Initialize Database Schema**:
   Connect via the SSM Bastion host to RDS PostgreSQL and run:
   ```bash
   psql -h <rds-endpoint> -U postgres -d postgres -f postgres/setup_db.sql
   ```

3. **Provision Kafka Topics**:
   Log into a Kafka broker EC2 instance and execute:
   ```bash
   ./kafka/create_topics.sh
   ```

4. **Register Kafka Connectors**:
   Deploy the Docker Compose stack on the Kafka Connect host:
   ```bash
   cd connectors
   docker-compose up -d
   ./register_connector.sh
   ```

5. **Start IoT Simulator**:
   ```bash
   cd producer
   pip install -r requirements.txt
   python iot_simulator.py
   ```

6. **Trigger dbt Model Transformations**:
   Execute dbt transformations:
   ```bash
   cd dbt
   dbt deps
   dbt run --target prod
   dbt test --target prod
   ```

---

## 🔧 Cluster Administration

### Kafka Topic Operations

* **List Topics**:
  ```bash
  kafka-topics.sh --bootstrap-server localhost:9092 --list
  ```

* **Inspect Consumer Group Lag**:
  ```bash
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group connect-jdbc-sink
  ```

### Kafka Connect Administration

* **Check Connector Status**:
  ```bash
  curl -s http://localhost:8083/connectors/jdbc-sink-connector/status | jq
  ```

* **Restart Failed Connector Task**:
  ```bash
  curl -X POST http://localhost:8083/connectors/jdbc-sink-connector/tasks/0/restart
  ```

---

## 🚨 Troubleshooting & Alerts Runbook

### Alert: `High Kafka Consumer Group Lag`
* **Symptom**: CloudWatch alarm triggered for `connect-jdbc-sink` consumer group lag.
* **Diagnosis**:
  1. Inspect Kafka Connect logs: `docker-compose logs -f kafka-connect`.
  2. Verify network path to RDS database.
  3. Look for poison pill messages (e.g. invalid bytes) blocking the sink tasks.
* **Mitigation**:
  * Scale up the connector tasks using `"tasks.max": 4` in the connector JSON configuration.
  * Route broken records using Dead Letter Queue policies.

### Alert: `PostgreSQL Logical Replication Slot Growth`
* **Symptom**: Disk space depleting on RDS instance due to active WAL files.
* **Diagnosis**:
  ```sql
  SELECT slot_name, active, wal_status FROM pg_replication_slots;
  ```
* **Mitigation**:
  * If the Debezium connector has stopped, restart it to consume WAL segments.
  * If the replication slot is orphaned, drop it manually to release storage:
    ```sql
    SELECT pg_drop_replication_slot('debezium_slot');
    ```

### Alert: `Data Quality Quarantine Spike`
* **Symptom**: Number of rows in `IOT_QUARANTINE` exceeds safety thresholds.
* **Diagnosis**:
  Identify the most frequent rejection reasons:
  ```sql
  SELECT rejection_reason, COUNT(*)
  FROM IOT_SILVER.CLEAN.IOT_QUARANTINE
  WHERE quarantined_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  GROUP BY 1;
  ```
* **Mitigation**:
  * If `TEMPERATURE_OUT_OF_RANGE`: Check if simulator configuration needs sensor recalibration.
  * If `NULL_DEVICE_ID`: Investigate upstream message format schema changes.
