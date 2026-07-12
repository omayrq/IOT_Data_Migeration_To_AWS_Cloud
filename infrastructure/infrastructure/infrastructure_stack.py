# pyrefly: ignore [missing-import]
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_iam as iam,
    aws_s3 as s3,
    aws_kms as kms,
    aws_secretsmanager as secretsmanager,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_iot as iot,
    aws_logs as logs,
)

# pyrefly: ignore [missing-import]
from constructs import Construct


class InfrastructureStack(Stack):
    """
    Production-ready IoT Streaming Platform infrastructure.
    Provisions: VPC, Bastion, PostgreSQL RDS (multi-AZ), 2x Kafka EC2 brokers,
    Kafka Connect EC2, S3 backup bucket, KMS encryption, CloudWatch alarms,
    SNS alerting, IAM roles, and Security Groups with least-privilege rules.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # =========================================================================
        # 1. KMS KEY — Created first, used by everything below
        # =========================================================================
        key = kms.Key(
            self,
            "IoTKey",
            description="Master KMS key for IoT Streaming Platform",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        key.add_alias("alias/iot-streaming-platform")

        # =========================================================================
        # 2. SNS ALERT TOPIC — Central alerting for all CloudWatch alarms
        # =========================================================================
        alert_topic = sns.Topic(
            self,
            "IoTAlertTopic",
            display_name="IoT Streaming Platform Alerts",
            topic_name="iot-streaming-alerts",
        )
        # Uncomment and set your email to receive alerts:
        # alert_topic.add_subscription(subs.EmailSubscription("ops-team@example.com"))

        # =========================================================================
        # 3. VPC — 3-tier network: Public / Private-Egress / Private-Isolated
        # =========================================================================
        self.vpc = ec2.Vpc(
            self,
            "IoTStreamingVPC",
            vpc_name="iot-streaming-vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=2,
            nat_gateways=1,  # One NAT GW in first AZ (cost-optimized for portfolio)
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,  # 10.0.0.0/24 & 10.0.1.0/24
                ),
                ec2.SubnetConfiguration(
                    name="Private-Data-A",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,  # Has NAT GW access
                    cidr_mask=24,  # 10.0.2.0/24 & 10.0.3.0/24
                ),
                ec2.SubnetConfiguration(
                    name="Private-Data-B",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,  # No internet, pure DB tier
                    cidr_mask=24,  # 10.0.4.0/24 & 10.0.5.0/24
                ),
            ],
        )

        # VPC Flow Logs for security auditing
        self.vpc.add_flow_log(
            "IoTVPCFlowLogs",
            destination=ec2.FlowLogDestination.to_cloud_watch_logs(
                log_group=logs.LogGroup(
                    self,
                    "VPCFlowLogGroup",
                    log_group_name="/iot-streaming/vpc-flow-logs",
                    retention=logs.RetentionDays.ONE_MONTH,
                    removal_policy=RemovalPolicy.DESTROY,
                )
            ),
        )

        # =========================================================================
        # 4. SECURITY GROUPS — Least-privilege, ordered to avoid forward-refs
        # =========================================================================

        # Bastion: SSH from anywhere (tighten to your IP in production)
        bastion_sg = ec2.SecurityGroup(
            self,
            "BastionSG",
            vpc=self.vpc,
            description="Bastion Host — SSH ingress",
            security_group_name="iot-bastion-sg",
            allow_all_outbound=True,
        )
        bastion_sg.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(22),
            description="SSH from internet (restrict to your IP in production)",
        )

        # PostgreSQL RDS
        postgres_sg = ec2.SecurityGroup(
            self,
            "PostgresSG",
            vpc=self.vpc,
            description="PostgreSQL RDS — controlled ingress",
            security_group_name="iot-postgres-sg",
            allow_all_outbound=True,
        )

        # Kafka Brokers (self-referencing for inter-broker communication)
        kafka_sg = ec2.SecurityGroup(
            self,
            "KafkaSG",
            vpc=self.vpc,
            description="Kafka Brokers — cluster + client ingress",
            security_group_name="iot-kafka-sg",
            allow_all_outbound=True,
        )

        # Kafka Connect (separate SG — Debezium + JDBC + S3)
        connect_sg = ec2.SecurityGroup(
            self,
            "KafkaConnectSG",
            vpc=self.vpc,
            description="Kafka Connect — talks to brokers, RDS, S3",
            security_group_name="iot-connect-sg",
            allow_all_outbound=True,
        )

        # ---- Inter-SG Rules ----
        # Bastion → PostgreSQL (management)
        postgres_sg.add_ingress_rule(
            bastion_sg, ec2.Port.tcp(5432), "Bastion manages PostgreSQL"
        )
        # Bastion → Kafka (admin / CLI)
        kafka_sg.add_ingress_rule(
            bastion_sg, ec2.Port.tcp(9092), "Bastion Kafka CLI plaintext"
        )
        kafka_sg.add_ingress_rule(
            bastion_sg, ec2.Port.tcp(9093), "Bastion Kafka controller port"
        )
        kafka_sg.add_ingress_rule(
            bastion_sg, ec2.Port.tcp(22), "Bastion SSH to Kafka brokers"
        )
        # Kafka internal cluster sync (self-reference)
        kafka_sg.add_ingress_rule(
            kafka_sg, ec2.Port.all_tcp(), "Kafka internal broker communication"
        )
        # Kafka Connect → Kafka Brokers
        kafka_sg.add_ingress_rule(
            connect_sg, ec2.Port.tcp(9092), "Kafka Connect to brokers"
        )
        # Kafka Connect → PostgreSQL (for Debezium CDC + JDBC sink)
        postgres_sg.add_ingress_rule(
            connect_sg, ec2.Port.tcp(5432), "Kafka Connect CDC + JDBC sink"
        )
        # Bastion → Kafka Connect (REST API admin)
        connect_sg.add_ingress_rule(
            bastion_sg, ec2.Port.tcp(8083), "Bastion to Connect REST API"
        )

        # =========================================================================
        # 5. S3 BUCKET — Declared early so roles can reference it below
        # =========================================================================
        backup_bucket = s3.Bucket(
            self,
            "IoTBackupBucket",
            bucket_name=f"iot-streaming-backup-{self.account}",
            removal_policy=RemovalPolicy.RETAIN,  # RETAIN in production — never auto-delete!
            encryption=s3.BucketEncryption.KMS,
            encryption_key=key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ArchiveRawAfter90Days",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(90),
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(365),
                        ),
                    ],
                )
            ],
            server_access_logs_prefix="access-logs/",
        )

        # =========================================================================
        # 6. IAM ROLES
        # =========================================================================

        # Shared SSM role for Bastion and Kafka EC2 (no key-based SSH required)
        ssm_role = iam.Role(
            self,
            "EC2SSMRole",
            role_name="iot-ec2-ssm-role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
        )

        # Kafka Connect role: SSM + Secrets Manager + S3
        connect_role = iam.Role(
            self,
            "KafkaConnectRole",
            role_name="iot-kafka-connect-role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
        )
        # Allow Connect to read Postgres credentials from Secrets Manager
        connect_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadPostgresSecret",
                actions=[
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret",
                ],
                resources=["arn:aws:secretsmanager:*:*:secret:*PostgreSQL*"],
            )
        )
        # Allow Connect to write/read S3 backup bucket (now declared above)
        connect_role.add_to_policy(
            iam.PolicyStatement(
                sid="S3BackupAccess",
                actions=[
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:DeleteObject",
                ],
                resources=[backup_bucket.bucket_arn, f"{backup_bucket.bucket_arn}/*"],
            )
        )
        # Allow Connect to use KMS for S3 encryption
        connect_role.add_to_policy(
            iam.PolicyStatement(
                sid="KMSAccess",
                actions=["kms:GenerateDataKey", "kms:Decrypt"],
                resources=[key.key_arn],
            )
        )

        # IoT Producer role: publish to Kafka via IoT Core
        iot_role = iam.Role(
            self,
            "IoTProducerRole",
            role_name="iot-producer-role",
            assumed_by=iam.ServicePrincipal("iot.amazonaws.com"),
        )
        iot_role.add_to_policy(
            iam.PolicyStatement(
                sid="IoTPublish",
                actions=["iot:Connect", "iot:Publish", "iot:Subscribe", "iot:Receive"],
                resources=["*"],
            )
        )
        iot_role.add_to_policy(
            iam.PolicyStatement(
                sid="SNSPublish",
                actions=["sns:Publish"],
                resources=[alert_topic.topic_arn],
            )
        )

        # AWS IoT Core Topic Rule for Temperature & Battery Alerts -> SNS
        self.iot_sns_rule = iot.CfnTopicRule(
            self,
            "IoTSNSTopicRule",
            rule_name="IoT_Critical_Alerts_Rule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql="SELECT * FROM 'iot/telemetry' WHERE temperature > 50 OR battery < 10",
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        sns=iot.CfnTopicRule.SnsActionProperty(
                            target_arn=alert_topic.topic_arn,
                            role_arn=iot_role.role_arn,
                            message_format="RAW",
                        )
                    )
                ],
                description="Routes critical IoT telemetry alerts directly to SNS Topic",
                rule_disabled=False,
            ),
        )

        # Lambda role for Timestream writer
        lambda_role = iam.Role(
            self,
            "LambdaTimestreamRole",
            role_name="iot-lambda-timestream-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="TimestreamWrite",
                actions=[
                    "timestream:WriteRecords",
                    "timestream:DescribeEndpoints",
                    "timestream:CreateTable",
                    "timestream:CreateDatabase",
                ],
                resources=["*"],
            )
        )

        # =========================================================================
        # 7. BASTION HOST
        # =========================================================================
        bastion = ec2.BastionHostLinux(
            self,
            "BastionHost",
            vpc=self.vpc,
            security_group=bastion_sg,
            subnet_selection=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            instance_name="iot-bastion",
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3, ec2.InstanceSize.MICRO
            ),
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        20,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        encrypted=True,
                    ),
                )
            ],
        )
        # Bastion uses SSM role (no key pair needed — use Session Manager)
        backup_bucket.grant_read_write(bastion)

        # =========================================================================
        # 8. POSTGRESQL RDS — Multi-AZ, encrypted, logical replication enabled
        # =========================================================================
        postgres_params = rds.ParameterGroup(
            self,
            "PostgresParams",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15
            ),
            description="IoT Streaming — enable logical replication for Debezium CDC",
            parameters={
                "rds.logical_replication": "1",
                "max_replication_slots": "10",
                "max_wal_senders": "10",
                "wal_keep_size": "512",
            },
        )

        postgres_db = rds.DatabaseInstance(
            self,
            "PostgreSQLInstance",
            instance_identifier="iot-streaming-postgres",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.LARGE
            ),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_group_name="Private-Data-A"),
            security_groups=[postgres_sg],
            allocated_storage=100,
            max_allocated_storage=500,  # Auto-scaling storage up to 500 GB
            storage_encrypted=True,
            storage_encryption_key=key,
            backup_retention=Duration.days(7),
            preferred_backup_window="02:00-03:00",
            preferred_maintenance_window="Sun:03:00-Sun:04:00",
            multi_az=True,
            database_name="iot_streaming_db",
            deletion_protection=True,  # Protect production DB
            removal_policy=RemovalPolicy.RETAIN,
            parameter_group=postgres_params,
            cloudwatch_logs_exports=["postgresql", "upgrade"],
            monitoring_interval=Duration.seconds(60),  # Enhanced monitoring
            enable_performance_insights=True,
            performance_insight_retention=rds.PerformanceInsightRetention.DEFAULT,
        )

        # =========================================================================
        # 9. KAFKA BROKER EC2 INSTANCES — KRaft mode (no ZooKeeper)
        # =========================================================================
        kafka_user_data = ec2.UserData.for_linux()
        kafka_user_data.add_commands(
            "yum update -y",
            "yum install -y java-17-amazon-corretto-headless aws-cli",
            # CloudWatch agent
            "yum install -y amazon-cloudwatch-agent",
        )

        # KeyPair reference for EC2 instances
        key_pair = ec2.KeyPair.from_key_pair_name(self, "KeyPair", "mua-dev")

        kafka_broker_a = ec2.Instance(
            self,
            "KafkaBrokerA",
            instance_name="iot-kafka-broker-a",
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.M5, ec2.InstanceSize.LARGE
            ),
            machine_image=ec2.MachineImage.latest_amazon_linux2(),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_group_name="Private-Data-A"),
            security_group=kafka_sg,
            role=ssm_role,
            key_pair=key_pair,
            user_data=kafka_user_data,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        100,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        encrypted=True,
                    ),
                )
            ],
        )

        kafka_broker_b = ec2.Instance(
            self,
            "KafkaBrokerB",
            instance_name="iot-kafka-broker-b",
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.M5, ec2.InstanceSize.LARGE
            ),
            machine_image=ec2.MachineImage.latest_amazon_linux2(),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_group_name="Private-Data-A"),
            security_group=kafka_sg,
            role=ssm_role,
            key_pair=key_pair,
            user_data=kafka_user_data,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        100,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        encrypted=True,
                    ),
                )
            ],
        )

        # =========================================================================
        # 10. KAFKA CONNECT EC2 — Runs Debezium, JDBC, S3, Snowflake connectors
        # =========================================================================
        connect_user_data = ec2.UserData.for_linux()
        connect_user_data.add_commands(
            "yum update -y",
            "yum install -y java-17-amazon-corretto-headless docker aws-cli",
            "systemctl enable docker",
            "systemctl start docker",
            "usermod -aG docker ec2-user",
            # Install Docker Compose
            'curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose',
            "chmod +x /usr/local/bin/docker-compose",
            "yum install -y amazon-cloudwatch-agent",
        )

        kafka_connect = ec2.Instance(
            self,
            "KafkaConnectInstance",
            instance_name="iot-kafka-connect",
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.M5, ec2.InstanceSize.XLARGE
            ),
            machine_image=ec2.MachineImage.latest_amazon_linux2(),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_group_name="Private-Data-A"),
            security_group=connect_sg,
            role=connect_role,
            key_pair=key_pair,
            user_data=connect_user_data,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        100,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        encrypted=True,
                    ),
                )
            ],
        )

        # =========================================================================
        # 11. CLOUDWATCH ALARMS — Monitor RDS, Kafka EC2s, Connect
        # =========================================================================
        alarm_action = cw_actions.SnsAction(alert_topic)

        # RDS CPU > 80%
        rds_cpu_alarm = cloudwatch.Alarm(
            self,
            "RDSCPUAlarm",
            alarm_name="iot-rds-cpu-high",
            alarm_description="PostgreSQL CPU utilization > 80% for 10 minutes",
            metric=postgres_db.metric_cpu_utilization(),
            threshold=80,
            evaluation_periods=2,
            datapoints_to_alarm=2,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.BREACHING,
        )
        rds_cpu_alarm.add_alarm_action(alarm_action)

        # RDS storage < 10 GB remaining
        rds_storage_alarm = cloudwatch.Alarm(
            self,
            "RDSStorageAlarm",
            alarm_name="iot-rds-storage-low",
            alarm_description="PostgreSQL free storage < 10 GB",
            metric=postgres_db.metric_free_storage_space(),
            threshold=10 * 1024 * 1024 * 1024,  # 10 GB in bytes
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
        )
        rds_storage_alarm.add_alarm_action(alarm_action)

        # Kafka Broker A CPU > 70%
        kafka_cpu_alarm = cloudwatch.Alarm(
            self,
            "KafkaBrokerACPUAlarm",
            alarm_name="iot-kafka-broker-a-cpu-high",
            alarm_description="Kafka Broker A CPU > 70%",
            metric=cloudwatch.Metric(
                namespace="AWS/EC2",
                metric_name="CPUUtilization",
                dimensions_map={"InstanceId": kafka_broker_a.instance_id},
                period=Duration.minutes(5),
                statistic="Average",
            ),
            threshold=70,
            evaluation_periods=3,
        )
        kafka_cpu_alarm.add_alarm_action(alarm_action)

        # =========================================================================
        # 12. CFNOUTPUTS — All resource IDs for scripts and documentation
        # =========================================================================
        CfnOutput(
            self,
            "VPCId",
            export_name="IoT-VPCId",
            value=self.vpc.vpc_id,
            description="VPC ID for IoT Streaming Platform",
        )

        CfnOutput(
            self,
            "BastionInstanceId",
            export_name="IoT-BastionInstanceId",
            value=bastion.instance_id,
            description="Use: aws ssm start-session --target <this-value>",
        )

        CfnOutput(
            self,
            "PostgresEndpoint",
            export_name="IoT-PostgresEndpoint",
            value=postgres_db.db_instance_endpoint_address,
            description="PostgreSQL RDS endpoint (private)",
        )

        CfnOutput(
            self,
            "PostgresSecretArn",
            export_name="IoT-PostgresSecretArn",
            value=postgres_db.secret.secret_arn if postgres_db.secret else "N/A",
            description="Secrets Manager ARN for PostgreSQL credentials",
        )

        CfnOutput(
            self,
            "BackupBucketName",
            export_name="IoT-BackupBucketName",
            value=backup_bucket.bucket_name,
            description="S3 bucket for raw IoT event backups",
        )

        CfnOutput(
            self,
            "KafkaBrokerAId",
            export_name="IoT-KafkaBrokerAId",
            value=kafka_broker_a.instance_id,
            description="Kafka Broker A EC2 instance ID",
        )

        CfnOutput(
            self,
            "KafkaBrokerBId",
            export_name="IoT-KafkaBrokerBId",
            value=kafka_broker_b.instance_id,
            description="Kafka Broker B EC2 instance ID",
        )

        CfnOutput(
            self,
            "KafkaConnectId",
            export_name="IoT-KafkaConnectId",
            value=kafka_connect.instance_id,
            description="Kafka Connect EC2 instance ID",
        )

        CfnOutput(
            self,
            "KMSKeyArn",
            export_name="IoT-KMSKeyArn",
            value=key.key_arn,
            description="KMS key used for all encryption",
        )

        CfnOutput(
            self,
            "AlertTopicArn",
            export_name="IoT-AlertTopicArn",
            value=alert_topic.topic_arn,
            description="SNS topic for CloudWatch alarms",
        )

        CfnOutput(
            self,
            "LambdaRoleArn",
            export_name="IoT-LambdaRoleArn",
            value=lambda_role.role_arn,
            description="IAM role ARN for Lambda Timestream writer",
        )
