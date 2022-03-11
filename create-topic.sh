#!/bin/bash

curl -X POST http://localhost:8101/createTopic -H 'Content-Type: application/json' -d '{"topic":"EBAOPRD1.TGLMINER.TM2_HEARTBEAT","numPartitions":"1","replicationFactor":2}'
