from aws_cdk import Stack, Duration, RemovalPolicy
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct


class KafkaConnectStack(Stack):
    """
    - PostgreSQL EC2 in a private subnet (no public IP) — Task 1.4
    - Bastion EC2 reachable only via SSM Session Manager — Task 1.4
    - Kafka Connect EC2 worker running Debezium / JDBC / Snowflake / S3 plugins
    - Secrets Manager secret holding Postgres credentials — Task 1.3
    """

    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.Vpc,
                 postgres_sg: ec2.SecurityGroup, bastion_sg: ec2.SecurityGroup,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ---- Secrets Manager: Postgres credentials -------------------------
        self.pg_secret = secretsmanager.Secret(
            self, "PostgresCredentials",
            secret_name="hackathon/iot/postgres",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"iot_admin"}',
                generate_string_key="password",
                exclude_characters='"@/\\',
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ---- IAM role shared by EC2 instances (SSM + Secrets read) --------
        role = iam.Role(
            self, "IotEc2Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                )
            ],
        )
        self.pg_secret.grant_read(role)

        amzn_linux = ec2.MachineImage.latest_amazon_linux2023()

        # ---- PostgreSQL EC2 (private subnet, no public IP) ----------------
        pg_user_data = ec2.UserData.for_linux()
        pg_user_data.add_commands(
            "dnf install -y postgresql15-server",
            "postgresql-setup --initdb",
            "sed -i \"s/^#wal_level.*/wal_level = logical/\" /var/lib/pgsql/data/postgresql.conf",
            "systemctl enable postgresql",
            "systemctl start postgresql",
        )

        self.postgres_instance = ec2.Instance(
            self, "PostgresEc2",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MEDIUM),
            machine_image=amzn_linux,
            security_group=postgres_sg,
            role=role,
            user_data=pg_user_data,
        )

        # ---- Bastion host (SSM only, no SSH keypair, no public IP needed) -
        self.bastion = ec2.Instance(
            self, "BastionHost",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
            machine_image=amzn_linux,
            security_group=bastion_sg,
            role=role,
        )

        # ---- Kafka Connect worker EC2 --------------------------------------
        connect_sg = ec2.SecurityGroup(
            self, "ConnectSG", vpc=vpc, description="Kafka Connect worker",
        )
        connect_sg.add_ingress_rule(
            ec2.Peer.ipv4(vpc.vpc_cidr_block), ec2.Port.tcp(8083), "Connect REST API"
        )

        connect_user_data = ec2.UserData.for_linux()
        connect_user_data.add_commands(
            "dnf install -y java-17-amazon-corretto wget",
            "mkdir -p /opt/kafka-connect/plugins",
            # Drop connector plugin jars: Debezium, Confluent JDBC, Confluent S3,
            # Snowflake Kafka Connector — see README 'Connector plugin jars'.
            "echo 'Place connector plugin jars in /opt/kafka-connect/plugins' > /opt/kafka-connect/README.txt",
        )

        self.connect_instance = ec2.Instance(
            self, "KafkaConnectEc2",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.LARGE),
            machine_image=amzn_linux,
            security_group=connect_sg,
            role=role,
            user_data=connect_user_data,
        )
