#!/bin/bash

set -e

echo "start zookeeper"
curl -X POST http://localhost:8101/startZookeeper

sleep 5

echo "start Kafka"
curl -X POST http://localhost:8101/startKafka

sleep 10

echo "start Kafka 1"
curl -X POST http://localhost:8101/startKafka/1

sleep 10

echo "start Kafka 2"
curl -X POST http://localhost:8101/startKafka/2

