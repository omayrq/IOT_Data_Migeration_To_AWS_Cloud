from aws_cdk import Stack, RemovalPolicy, Duration
from aws_cdk import aws_s3 as s3
from constructs import Construct


class S3BackupStack(Stack):
    """S3 bucket used by the optional Kafka S3 Sink Connector backup path."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bucket = s3.Bucket(
            self, "IotBackupBucket",
            bucket_name=f"hackathon-iot-backup-{Stack.of(self).account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="transition-to-ia",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30),
                        )
                    ],
                )
            ],
            removal_policy=RemovalPolicy.RETAIN,
        )
