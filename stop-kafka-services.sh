#!/bin/bash

set -e

echo "stop Kafka 2"
curl -X POST http://localhost:8101/stopKafka/2

sleep 10

echo "stop Kafka 1"
curl -X POST http://localhost:8101/stopKafka/1

sleep 10

echo "stop Kafka"
curl -X POST http://localhost:8101/stopKafka

sleep 10

echo "stop zookeeper"
curl -X POST http://localhost:8101/stopZookeeper

