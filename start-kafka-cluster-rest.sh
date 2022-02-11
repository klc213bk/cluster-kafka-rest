#!/bin/bash

java -jar -Dspring.profiles.active=prod target/cluster-kafka-rest-1.0.jar --spring.config.location=file:config/ & 
