rand=`/opt/kafka_2.13-3.1.0/bin/kafka-storage.sh random-uuid`
echo $rand
/opt/kafka_2.13-3.1.0/bin/kafka-storage.sh format -t $rand -c /opt/kafka_2.13-3.1.0/config/kraft/serverA.properties
/opt/kafka_2.13-3.1.0/bin/kafka-storage.sh format -t $rand -c /opt/kafka_2.13-3.1.0/config/kraft/serverB.properties
