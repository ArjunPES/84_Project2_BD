#!/bin/bash
# Start Kafka Broker

echo "Starting ZooKeeper..."
cd ~/kafka_2.13-3.6.0
bin/zookeeper-server-start.sh config/zookeeper.properties &

sleep 5

echo "Starting Kafka Broker..."
bin/kafka-server-start.sh config/server.properties &

echo "Kafka is starting... Check logs for status"
echo "Logs: ~/kafka_2.13-3.6.0/logs/"