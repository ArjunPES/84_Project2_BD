#!/bin/bash
# Stop Kafka Broker

echo "Stopping Kafka Broker..."
cd ~/kafka_2.13-3.6.0
bin/kafka-server-stop.sh

echo "Stopping ZooKeeper..."
bin/zookeeper-server-stop.sh

echo "Kafka stopped"