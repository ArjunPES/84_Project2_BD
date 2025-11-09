#!/bin/bash
# List all Kafka topics

cd ~/kafka_2.13-3.6.0
bin/kafka-topics.sh --bootstrap-server 10.241.219.151:9092 --list