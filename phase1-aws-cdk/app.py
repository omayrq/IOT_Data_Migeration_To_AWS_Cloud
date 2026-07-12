#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.vpc_stack import VpcStack
from stacks.msk_stack import MskStack
from stacks.kafka_connect_stack import KafkaConnectStack
from stacks.s3_backup_stack import S3BackupStack

app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "eu-west-2",
)

vpc_stack = VpcStack(app, "IotHackathon-Vpc", env=env)

msk_stack = MskStack(app, "IotHackathon-Msk", vpc=vpc_stack.vpc, env=env)
msk_stack.add_dependency(vpc_stack)

connect_stack = KafkaConnectStack(
    app, "IotHackathon-KafkaConnect",
    vpc=vpc_stack.vpc,
    postgres_sg=vpc_stack.postgres_sg,
    bastion_sg=vpc_stack.bastion_sg,
    env=env,
)
connect_stack.add_dependency(msk_stack)

s3_stack = S3BackupStack(app, "IotHackathon-S3Backup", env=env)

app.synth()

# Deploy all stacks:
#   cdk deploy --all --context region=eu-west-2 --context account=<your-account-id>
