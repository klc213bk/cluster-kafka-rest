# cluster-kafka-rest

# start rest server
$ ./start-kafka-cluster-rest.sh

# start kafka cluster
curl -X POST http://localhost:8101/startCluster


# stop kafka cluster
curl -X POST http://localhost:8101/stopCluster


# stop rest server
$ ./stop-kafka-cluster-rest.sh


# check port
$ netstat -plten | grep java


###############################################################
### create folders
/data/v2/kafka/serverA
/data/v2/kafka/serverB
/data/v2/kafka/connectors
/data/v2/kafka/health/connect
/data/v2/kafka/health/logs