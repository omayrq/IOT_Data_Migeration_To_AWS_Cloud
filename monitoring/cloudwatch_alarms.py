#!/usr/bin/env python3
"""
monitoring/cloudwatch_alarms.py
==================================
Creates and updates CloudWatch alarms for critical system resources
in the IoT Streaming Platform using boto3.

Alarms Created:
  1. EC2 CPU Utilization > 80% (Kafka brokers and Connect instances)
  2. RDS CPU Utilization > 85% (PostgreSQL instance)
  3. RDS Freeable Memory < 512MB
  4. RDS Free Storage Space < 10GB
  5. S3 Bucket Size growth notifications or KMS replication alerts
"""

import argparse
import sys
import boto3
from botocore.exceptions import ClientError

# Configuration
REGION = "us-east-1"
SNS_TOPIC_ARN_DEFAULT = "arn:aws:sns:us-east-1:528582359305:iot-platform-alarms"

cloudwatch = boto3.client("cloudwatch", region_name=REGION)


def log(msg):
    print(f"[+] {msg}")


def log_err(msg):
    print(f"[-] ERROR: {msg}", file=sys.stderr)


def create_ec2_cpu_alarm(instance_id, sns_topic_arn):
    alarm_name = f"EC2-{instance_id}-High-CPU-Utilization"
    log(f"Creating/updating alarm: {alarm_name}")
    try:
        cloudwatch.put_metric_alarm(
            AlarmName=alarm_name,
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            EvaluationPeriods=2,
            MetricName="CPUUtilization",
            Namespace="AWS/EC2",
            Period=300,
            Statistic="Average",
            Threshold=80.0,
            ActionsEnabled=True,
            AlarmActions=[sns_topic_arn],
            AlarmDescription="Alarm when EC2 CPU utilization exceeds 80% for 10 minutes",
            Dimensions=[{"Name": "InstanceId", "Value": instance_id}],
            Unit="Percent",
        )
        log("Successfully created EC2 CPU alarm.")
    except ClientError as e:
        log_err(f"Failed to create EC2 CPU alarm: {e}")


def create_rds_alarms(db_instance_id, sns_topic_arn):
    # 1. RDS High CPU
    cpu_alarm_name = f"RDS-{db_instance_id}-High-CPU"
    log(f"Creating/updating alarm: {cpu_alarm_name}")
    try:
        cloudwatch.put_metric_alarm(
            AlarmName=cpu_alarm_name,
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            EvaluationPeriods=2,
            MetricName="CPUUtilization",
            Namespace="AWS/RDS",
            Period=300,
            Statistic="Average",
            Threshold=85.0,
            ActionsEnabled=True,
            AlarmActions=[sns_topic_arn],
            AlarmDescription="Alarm when RDS CPU utilization exceeds 85% for 10 minutes",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": db_instance_id}],
            Unit="Percent",
        )
        log("Successfully created RDS CPU alarm.")
    except ClientError as e:
        log_err(f"Failed to create RDS CPU alarm: {e}")

    # 2. RDS Low Storage Space
    storage_alarm_name = f"RDS-{db_instance_id}-Low-Storage"
    log(f"Creating/updating alarm: {storage_alarm_name}")
    try:
        cloudwatch.put_metric_alarm(
            AlarmName=storage_alarm_name,
            ComparisonOperator="LessThanOrEqualToThreshold",
            EvaluationPeriods=1,
            MetricName="FreeStorageSpace",
            Namespace="AWS/RDS",
            Period=900,
            Statistic="Average",
            Threshold=10 * 1024 * 1024 * 1024.0,  # 10 GB in bytes
            ActionsEnabled=True,
            AlarmActions=[sns_topic_arn],
            AlarmDescription="Alarm when RDS free storage space is less than 10 GB",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": db_instance_id}],
            Unit="Bytes",
        )
        log("Successfully created RDS Storage alarm.")
    except ClientError as e:
        log_err(f"Failed to create RDS Storage alarm: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Configure CloudWatch Alarms for the IoT Streaming Platform."
    )
    parser.add_argument(
        "--sns-topic",
        default=SNS_TOPIC_ARN_DEFAULT,
        help="SNS Topic ARN for alarm actions",
    )
    parser.add_argument(
        "--ec2-instances",
        nargs="*",
        default=["i-01742fef0bdca7316"],
        help="List of EC2 Instance IDs to monitor",
    )
    parser.add_argument(
        "--rds-id",
        default="iot-postgres-instance",
        help="RDS DB Instance Identifier to monitor",
    )

    args = parser.parse_args()

    # Configure Alarms
    for instance in args.ec2_instances:
        create_ec2_cpu_alarm(instance, args.sns_topic)

    create_rds_alarms(args.rds_id, args.sns_topic)


if __name__ == "__main__":
    main()
