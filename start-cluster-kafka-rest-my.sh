#!/bin/bash

java -jar -Dspring.profiles.active=my target/cluster-kafka-rest-1.0.jar --spring.config.location=file:config/ & 