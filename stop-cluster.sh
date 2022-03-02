#!/bin/bash

kill -9 $(lsof -t -i:9093 -sTCP:LISTEN)

sleep 5

kill -9 $(lsof -t -i:9092 -sTCP:LISTEN)
