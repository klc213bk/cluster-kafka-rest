#!/bin/bash

KAFKA_HOME=/opt/kafka_2.13-3.1.0 
${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/kraft/serverA.properties &

sleep 5

${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/kraft/serverB.properties &

