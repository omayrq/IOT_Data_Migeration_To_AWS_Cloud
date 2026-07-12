from aws_cdk import Stack
from aws_cdk import aws_ec2 as ec2
from constructs import Construct


class VpcStack(Stack):
    """
    VPC with:
      - Public subnet  -> Bastion host (SSM-only, no inbound SSH)
      - Private subnet -> PostgreSQL EC2 (no public IP), MSK, Kafka Connect
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(
            self, "IotHackathonVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="private-app",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # Security group for the Postgres EC2 instance: only reachable from
        # within the VPC (Kafka Connect / bastion), never from the internet.
        self.postgres_sg = ec2.SecurityGroup(
            self, "PostgresSG", vpc=self.vpc, description="Postgres EC2 (on-prem sim)",
            allow_all_outbound=True,
        )
        self.postgres_sg.add_ingress_rule(
            ec2.Peer.ipv4(self.vpc.vpc_cidr_block), ec2.Port.tcp(5432),
            "Allow Postgres access from within the VPC only",
        )

        # Bastion security group: no inbound rules at all — access is via
        # AWS SSM Session Manager, not SSH.
        self.bastion_sg = ec2.SecurityGroup(
            self, "BastionSG", vpc=self.vpc, description="Bastion (SSM only)",
            allow_all_outbound=True,
        )
