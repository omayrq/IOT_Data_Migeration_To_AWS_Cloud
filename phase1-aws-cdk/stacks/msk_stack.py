from aws_cdk import Stack
from aws_cdk import aws_msk as msk
from aws_cdk import aws_ec2 as ec2
from constructs import Construct


class MskStack(Stack):
    """
    AWS MSK cluster that carries both:
      - iot-events (Phase 1: device telemetry from IoT Core)
      - cdc.public.iot_events (Phase 2: Debezium CDC change events)
    """

    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.Vpc, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        msk_sg = ec2.SecurityGroup(
            self, "MskSG", vpc=vpc, description="MSK broker security group",
            allow_all_outbound=True,
        )
        msk_sg.add_ingress_rule(
            ec2.Peer.ipv4(vpc.vpc_cidr_block), ec2.Port.tcp(9092), "Kafka plaintext"
        )
        msk_sg.add_ingress_rule(
            ec2.Peer.ipv4(vpc.vpc_cidr_block), ec2.Port.tcp(9094), "Kafka TLS"
        )

        private_subnet_ids = [s.subnet_id for s in vpc.private_subnets]

        self.cluster = msk.CfnCluster(
            self, "IotMskCluster",
            cluster_name="hackathon-iot-msk",
            kafka_version="3.6.0",
            number_of_broker_nodes=2,
            broker_node_group_info=msk.CfnCluster.BrokerNodeGroupInfoProperty(
                instance_type="kafka.t3.small",
                client_subnets=private_subnet_ids,
                security_groups=[msk_sg.security_group_id],
                storage_info=msk.CfnCluster.StorageInfoProperty(
                    ebs_storage_info=msk.CfnCluster.EBSStorageInfoProperty(volume_size=100)
                ),
            ),
            encryption_info=msk.CfnCluster.EncryptionInfoProperty(
                encryption_in_transit=msk.CfnCluster.EncryptionInTransitProperty(
                    client_broker="TLS_PLAINTEXT", in_cluster=True
                )
            ),
        )
