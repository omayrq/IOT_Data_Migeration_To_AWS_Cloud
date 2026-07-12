# IoT Streaming Platform — Production Architecture

This document details the real-time production-ready architecture of the **IoT Streaming Platform**. The platform processes telemetry data from simulated devices, performs ingestion, stream processing, long-term warehousing, and serves it to dashboards with data quality validations and monitoring.

## System Architecture Diagram

```mermaid
flowchart TB
    %% Ingestion Layer
    subgraph IoT_Telemetry ["Telemetry Ingestion"]
        Simulator["IoT Device Simulator (Python)"]
        IoTCore["AWS IoT Core Core Rule"]
    end

    %% Streaming Layer
    subgraph Kafka_Cluster ["Kafka Streaming Platform"]
        Kafka1["Kafka Broker 1 (KRaft)"]
        Kafka2["Kafka Broker 2 (KRaft)"]
        Connect["Kafka Connect Engine"]
        S3Sink["S3 Sink Connector"]
        JDBCSink["JDBC Sink Connector"]
        Debezium["Debezium PG CDC Connector"]
    end

    %% Relational Storage Layer
    subgraph DB_Layer ["Relational Storage Tier"]
        RDS["Multi-AZ RDS PostgreSQL Instance"]
    end

    %% Storage & Analytics Warehousing
    subgraph S3_Backup ["Object Storage"]
        S3Bucket["KMS Encrypted S3 Bucket"]
    end

    subgraph Snowflake_Warehouse ["Snowflake Medallion Architecture"]
        SF_Bronze["RAW Database (Bronze Layer)"]
        SF_Silver["SILVER Database (Deduplicated & Cleaned)"]
        SF_Gold["GOLD Database (Daily KPI summaries & Anomalies)"]
        SF_Quarantine["Silver Quarantine (Failed DQ Checks)"]
    end

    %% Monitoring, Orchestration & UI
    subgraph Analytics_UI ["Analytics & Visualisation"]
        Streamlit["Streamlit Interactive Dashboard"]
        Lambda["Lambda Timestream Writer"]
        Timestream["Amazon Timestream Time-Series DB"]
        Grafana["Grafana Visualisation Service"]
        Prometheus["Prometheus JMX Scraper"]
    end

    %% Flow links
    Simulator -->|Sends Telemetry| Kafka1 & Kafka2
    IoTCore -->|Triggers Alert| SNS_Alerts["AWS SNS Alert Topic"]
    
    Kafka1 & Kafka2 -->|Raw Streams| Connect
    Connect -->|S3 Sink| S3Bucket
    Connect -->|JDBC Sink| RDS
    RDS -->|WAL CDC Events| Debezium
    Debezium -->|CDC Logs to Kafka| Kafka1 & Kafka2

    %% Snowflake integration
    Kafka1 & Kafka2 -->|Snowflake Kafka Connector| SF_Bronze
    SF_Bronze -->|dbt model: stg_iot_raw| SF_Silver
    SF_Silver -->|Deduplicated & Cleaned: check_data_quality| SF_Silver
    SF_Silver -->|Route Invalid Rows| SF_Quarantine
    SF_Silver -->|dbt model: iot_device_summary| SF_Gold
    SF_Silver -->|dbt model: iot_anomalies| SF_Gold

    %% Data Consumers
    SF_Gold -->|Reads KPIs| Streamlit
    SF_Gold -->|Scheduled reads every 5 mins| Lambda
    Lambda -->|Writes metrics| Timestream
    Timestream -->|Queries| Grafana
    Prometheus -->|Scrapes JMX Port 7071/7072| Kafka1 & Kafka2
```

## Architectural Stages and Ingestion Flow

1. **Stage 1 (AWS Foundation)**: Deploys core security services, including **AWS Key Management Service (KMS)** and **SNS Alert Topics** for notifying engineers.
2. **Stage 2 (Networking & Security)**: Implements a 3-tier Private VPC layout restricting RDS & Kafka Connect instances to private subnets without public internet ingress.
3. **Stage 3 (Infrastructure as Code)**: Declares all components in standard python AWS CDK for reproducible deployments.
4. **Stage 4 & 5 (Kafka & IoT)**: Configures a 2-broker Kraft cluster processing incoming payload from the simulator generating telemetry.
5. **Stage 6 (PostgreSQL)**: Handles relational metadata registry and processing logs with optimized query performance.
6. **Stage 7 & 8 (Kafka Connect & S3)**: Moves raw message streams to AWS S3 storage under Hive partitions and sinks to PostgreSQL database.
7. **Stage 9 (Debezium CDC)**: Monitors PostgreSQL write-ahead logs (WAL) to emit change events back to Kafka.
8. **Stage 10, 11 & 12 (Snowflake & dbt & DQ)**: Transforms raw JSON events using medallion modeling (Bronze -> Silver -> Gold). Reusable data quality rules isolate bad rows into the Quarantine table.
9. **Stage 13 & 14 (Streamlit, Lambda & Grafana)**: Builds high-performance charts, maps, and Grafana dashboards for user consumption.
10. **Stage 15 & 16 (Monitoring & Hardening)**: Leverages Prometheus and Alertmanager to scrape JMX JRE process metrics alongside automated GitHub Actions pipelines.
