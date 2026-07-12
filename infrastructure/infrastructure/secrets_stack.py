# pyrefly: ignore [missing-import]
from aws_cdk import (
    Stack,
    RemovalPolicy,
    CfnOutput,
    aws_secretsmanager as secretsmanager,
    aws_kms as kms,
)

# pyrefly: ignore [missing-import]
from constructs import Construct


class SecretsStack(Stack):
    """
    SecretsStack handles all sensitive passwords, access keys, and connection strings.
    This separates security credentials from the core infrastructure resources
    to prevent accidental credential exposure.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Retrieve or create KMS key for secrets encryption
        secrets_key = kms.Key(
            self,
            "SecretsKMSKey",
            description="KMS Key for IoT Streaming Platform Secrets",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        secrets_key.add_alias("alias/iot-secrets-key")

        # 1. Snowflake Credentials Secret
        self.snowflake_secret = secretsmanager.Secret(
            self,
            "SnowflakeCredentials",
            secret_name="iot-platform/snowflake-credentials",
            description="Snowflake credentials for dbt and Streamlit connector",
            encryption_key=secrets_key,
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"account": "your-account-id", "username": "DBT_USER", "role": "IOT_DBT_ROLE", "warehouse": "IOT_TRANSFORM_WH"}',
                generate_string_key="private_key_passphrase",
                exclude_punctuation=True,
                password_length=32,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # 2. Grafana and Admin Console Admin Password Secret
        self.admin_credentials_secret = secretsmanager.Secret(
            self,
            "AdminCredentials",
            secret_name="iot-platform/admin-credentials",
            description="Admin credentials for Grafana, Prometheus, and Postgres master user",
            encryption_key=secrets_key,
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"grafana_username": "admin", "postgres_username": "postgres"}',
                generate_string_key="admin_password",
                exclude_punctuation=True,
                password_length=24,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # 3. SMTP Alertmanager Config Credentials Secret
        self.smtp_credentials_secret = secretsmanager.Secret(
            self,
            "SMTPCredentials",
            secret_name="iot-platform/smtp-credentials",
            description="SMTP server details for sending notifications to Alertmanager",
            encryption_key=secrets_key,
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"smtp_host": "smtp.example.com", "smtp_port": "587", "smtp_user": "alertmanager@company.com"}',
                generate_string_key="smtp_password",
                exclude_punctuation=True,
                password_length=32,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # CfnOutputs
        CfnOutput(self, "SnowflakeSecretArn", value=self.snowflake_secret.secret_arn)
        CfnOutput(
            self, "AdminSecretArn", value=self.admin_credentials_secret.secret_arn
        )
        CfnOutput(self, "SMTPSecretArn", value=self.smtp_credentials_secret.secret_arn)
