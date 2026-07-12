#!/usr/bin/env python3
import sys
import os

# Add outer directory to path to ensure the inner package can be imported from any location
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import aws_cdk as cdk

from infrastructure.infrastructure_stack import InfrastructureStack
from infrastructure.secrets_stack import SecretsStack

app = cdk.App()

# 1. Provision Secrets Stack (Security & Credentials)
secrets = SecretsStack(app, "SecretsStack")

# 2. Provision Core Infrastructure Stack (VPC, RDS, EC2, MSK)
# Can reference secrets stack if needed
InfrastructureStack(app, "InfrastructureStack")

app.synth()
