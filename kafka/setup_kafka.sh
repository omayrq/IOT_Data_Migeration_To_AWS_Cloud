#!/bin/bash
# setup_kafka.sh
# Usage: ./setup_kafka.sh <node_id> <private_ip> <other_broker_ip>

set -e

NODE_ID=$1
PRIVATE_IP=$2
OTHER_IP=$3

if [[ -z "$NODE_ID" || -z "$PRIVATE_IP" || -z "$OTHER_IP" ]]; then
    echo "Usage: $0 <node_id> <private_ip> <other_broker_ip>"
    exit 1
fi

echo "========================================="
echo "Setting up Kafka Broker Node ID: $NODE_ID"
echo "Private IP: $PRIVATE_IP"
echo "Other IP: $OTHER_IP"
echo "========================================="

# 1. Install Java 17
echo "Installing Java 17..."
sudo yum update -y
sudo yum install -y java-17-amazon-corretto-headless

# 2. Download and extract Apache Kafka
KAFKA_VERSION="3.7.0"
SCALA_VERSION="2.13"
KAFKA_TAR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
DOWNLOAD_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TAR}"

if [ ! -d "/opt/kafka" ]; then
    echo "Downloading Kafka $KAFKA_VERSION..."
    curl -sS -o /tmp/${KAFKA_TAR} ${DOWNLOAD_URL}
    echo "Extracting Kafka..."
    sudo tar -xzf /tmp/${KAFKA_TAR} -C /opt
    sudo ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka
    rm -f /tmp/${KAFKA_TAR}
fi

# 3. Create Kafka log directory
sudo mkdir -p /var/lib/kafka/data
sudo chown -R ec2-user:ec2-user /var/lib/kafka /opt/kafka /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}

# 4. Create custom KRaft server properties
echo "Configuring kraft/server.properties..."
CONFIG_FILE="/opt/kafka/config/kraft/server.properties"

cat << EOF > /tmp/server.properties
# Kafka KRaft Configuration
process.roles=broker,controller
node.id=${NODE_ID}

# Controller Quorum Voters
controller.quorum.voters=1@10.0.2.119:9093,2@10.0.4.85:9093

# Listeners
listeners=PLAINTEXT://${PRIVATE_IP}:9092,CONTROLLER://${PRIVATE_IP}:9093
advertised.listeners=PLAINTEXT://${PRIVATE_IP}:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# Logs
log.dirs=/var/lib/kafka/data

# Partitions & Replication
num.partitions=3
default.replication.factor=2
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=2

# Log Retention
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
EOF

sudo mv /tmp/server.properties ${CONFIG_FILE}
sudo chown ec2-user:ec2-user ${CONFIG_FILE}

# 5. Format storage directory (using a static cluster UUID)
CLUSTER_ID="4L62Cnn2TZa2YfG3H2xP1g"
echo "Formatting Kafka log directory with Cluster ID: ${CLUSTER_ID}..."
/opt/kafka/bin/kafka-storage.sh format -t ${CLUSTER_ID} -c ${CONFIG_FILE} --ignore-formatted || true

# 6. Create systemd unit file
echo "Creating systemd unit file..."
cat << EOF | sudo tee /etc/systemd/system/kafka.service
[Unit]
Description=Apache Kafka Distributed Message Broker (KRaft)
Documentation=http://kafka.apache.org/documentation.html
After=network.target

[Service]
Type=simple
User=ec2-user
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

# 7. Start & enable Kafka service
echo "Starting Kafka service..."
sudo systemctl daemon-reload
sudo systemctl enable kafka
sudo systemctl start kafka

echo "Kafka Broker Node ID $NODE_ID setup complete and started!"
