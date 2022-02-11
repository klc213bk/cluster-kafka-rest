#!/bin/bash

set -e

KAFKA_HOME=/opt/kafka_2.13-3.1.0

export KAFKA_HEAP_OPTS="-Xmx200M -Xms100M"

cd ${KAFKA_HOME}

#./bin/kafka-server-start.sh -daemon ./config/kraft/serverA.properties
#./bin/kafka-server-start.sh -daemon ./config/kraft/serverB.properties

./bin/kafka-server-start.sh ./config/kraft/serverA.properties &
./bin/kafka-server-start.sh ./config/kraft/serverB.properties &