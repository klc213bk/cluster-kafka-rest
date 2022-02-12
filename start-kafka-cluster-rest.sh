#!/bin/bash

export KAFKA_HEAP_OPTS="-Xmx200M -Xms100M"

java -jar -Dspring.profiles.active=prod target/cluster-kafka-rest-1.0.jar --spring.config.location=file:config/ & 
