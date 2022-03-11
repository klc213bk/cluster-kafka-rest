package com.transglobe.streamingetl.cluster.kafka.rest.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.cluster.kafka.rest.common.CreateTopic;
import com.transglobe.streamingetl.cluster.kafka.rest.common.LastLogminerScn;

@Service
public class KafkaService {
	static final Logger LOGGER = LoggerFactory.getLogger(KafkaService.class);

	@Value("${kafka.server.home}")
	private String kafkaServerHome;

	//	@Value("${kafka.start.script}")
	//	private String kafkaStartScript;
	//	
	//	@Value("${kafka.cluster.prop.A}")
	//	private String kafkaClusterPropA;
	//	
	//	@Value("${kafka.cluster.prop.B}")
	//	private String kafkaClusterPropB;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;

	//	private Process clusterStartProcessA;
	//	private ExecutorService clusterStartExecutorA;
	//	
	//	private Process clusterStartProcessB;
	//	private ExecutorService clusterStartExecutorB;

	//	public void startCluster() throws Exception {
	//		LOGGER.info(">>>>>>>>>>>> KafkaService.startCluster starting");
	//		try {
	//			if (clusterStartProcessA == null || !clusterStartProcessA.isAlive()) {
	//				LOGGER.info(">>>>>>>>>>>> clusterStartProcessA.isAlive={} ", (clusterStartProcessA == null)? null : clusterStartProcessA.isAlive());
	//				//				kafkaStartFinished0.set(false);
	//				ProcessBuilder builder = new ProcessBuilder();
	//				//	String script = "./bin/zookeeper-server-start.sh";
	//				//builder.command("sh", "-c", script);
	//				builder.command(kafkaStartScript, kafkaClusterPropA);
	//
	//				builder.directory(new File(kafkaServerHome));
	//				clusterStartProcessA = builder.start();
	//
	//				clusterStartExecutorA = Executors.newSingleThreadExecutor();
	//				clusterStartExecutorA.submit(new Runnable() {
	//
	//					@Override
	//					public void run() {
	//						BufferedReader reader = new BufferedReader(new InputStreamReader(clusterStartProcessA.getInputStream()));
	//						reader.lines().forEach(line -> {
	//							LOGGER.info(line);
	//
	//						});
	//					}
	//
	//				});
	//				
	//				ProcessBuilder builder2 = new ProcessBuilder();
	//				//	String script = "./bin/zookeeper-server-start.sh";
	//				//builder.command("sh", "-c", script);
	//				builder2.command(kafkaStartScript, kafkaClusterPropB);
	//
	//				builder2.directory(new File(kafkaServerHome));
	//				clusterStartProcessB = builder2.start();
	//
	//				clusterStartExecutorB = Executors.newSingleThreadExecutor();
	//				clusterStartExecutorB.submit(new Runnable() {
	//
	//					@Override
	//					public void run() {
	//						BufferedReader reader = new BufferedReader(new InputStreamReader(clusterStartProcessB.getInputStream()));
	//						reader.lines().forEach(line -> {
	//							LOGGER.info(line);
	//
	//						});
	//					}
	//
	//				});
	//				
	//				while (true) {
	//					try {
	//						listTopics();
	//						break;
	//					} catch (Exception e) {
	//						LOGGER.info(">>>> listTopics error occurred. Sleep for 1 second");;
	//						Thread.sleep(1000);
	//						continue;
	//					}
	//				}
	//				LOGGER.info(">>>>>>>>>>>> clusterStartProcessA.isAlive={} ", (clusterStartProcessA == null)? null : clusterStartProcessA.isAlive());
	//				LOGGER.info(">>>>>>>>>>>> clusterStartProcessB.isAlive={} ", (clusterStartProcessB == null)? null : clusterStartProcessB.isAlive());
	//				
	//				
	//				LOGGER.info(">>>>>>>>>>>> KafkaService.startCluster End");
	//			} else {
	//				LOGGER.warn(" >>> clusterStartProcess is currently Running.");
	//			}
	//		} catch (IOException e) {
	//			LOGGER.error(">>> Error!!!, startCluster, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
	//			throw e;
	//		} 
	//	}
	//	
	//	public void stopCluster() throws Exception {
	//		LOGGER.info(">>>>>>>>>>>> KafkaService.stopCluster starting...");
	//		try {
	//			LOGGER.info(">>>>>>>>>>>> clusterStartProcessA.isAlive={} ", (clusterStartProcessA == null)? null : clusterStartProcessA.isAlive());
	//
	//			LOGGER.info(">>>>>>>>>>>> clusterStartProcessB.isAlive={} ", (clusterStartProcessB == null)? null : clusterStartProcessB.isAlive());
	//			
	//			if (clusterStartProcessB != null && clusterStartProcessB.isAlive()) {
	//				//				
	//				clusterStartProcessB.destroy();
	//
	//
	//				LOGGER.info(">>>>>>>>>>>> KafkaService.stopCluster End");
	//			} else {
	//				LOGGER.warn(" >>> clusterStartProcessB IS NOT ALIVE.");
	//			}
	//			
	//			if (clusterStartProcessA != null && clusterStartProcessA.isAlive()) {
	//				//				
	//				clusterStartProcessA.destroy();
	//
	//
	//				LOGGER.info(">>>>>>>>>>>> KafkaService.stopCluster End");
	//			} else {
	//				LOGGER.warn(" >>> clusterStartProcessA IS NOT ALIVE.");
	//			}
	//
	//			if (!clusterStartExecutorB.isTerminated()) {
	//				if (!clusterStartExecutorB.isShutdown()) {
	//					clusterStartExecutorB.shutdown();
	//				}
	//				while (!clusterStartExecutorB.isShutdown()) {
	//					Thread.sleep(1000);
	//					LOGGER.info(">>>> waiting for executor shuttung down.");;
	//				}
	//
	//				while (!clusterStartExecutorB.isTerminated()) {
	//					Thread.sleep(1000);
	//					LOGGER.info(">>>> waiting for executor termainting.");;
	//				}
	//			} 
	//			if (clusterStartExecutorB.isTerminated()) {
	//				LOGGER.info(">>>> clusterStartExecutorB is Terminated!!!!!");
	//			} 
	//			
	//			if (!clusterStartExecutorA.isTerminated()) {
	//				if (!clusterStartExecutorA.isShutdown()) {
	//					clusterStartExecutorA.shutdown();
	//				}
	//				while (!clusterStartExecutorA.isShutdown()) {
	//					Thread.sleep(1000);
	//					LOGGER.info(">>>> waiting for executor shuttung down.");;
	//				}
	//
	//				while (!clusterStartExecutorA.isTerminated()) {
	//					Thread.sleep(1000);
	//					LOGGER.info(">>>> waiting for executor termainting.");;
	//				}
	//			} 
	//			if (clusterStartExecutorA.isTerminated()) {
	//				LOGGER.info(">>>> clusterStartExecutorA is Terminated!!!!!");
	//			} 
	//
	//		} catch (Exception e) {
	//			LOGGER.error(">>> Error!!!, stopCluster, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
	//			throw e;
	//		} 
	//	}

	public Set<String> listTopics() throws Exception {
		LOGGER.info(">>>>>>>>>>>> listTopics ");
		try {

			AdminClient adminClient = createAdminClient();
			ListTopicsResult listTopicsResult = adminClient.listTopics();
			KafkaFuture<Set<String>> future = listTopicsResult.names();
			Set<String> topicNames = future.get();

			adminClient.close();

			return topicNames;

		} catch (Exception e) {
			LOGGER.error(">>> Error!!!, listTopics, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 

	}
	public void createTopic(CreateTopic createTopic) throws Exception {
		LOGGER.info(">>>>>>>>>>>> createTopic topic=={}", ToStringBuilder.reflectionToString(createTopic));
		try {
			Set<NewTopic> topicSet = new HashSet<>();
			NewTopic newTopic = new NewTopic(
					createTopic.getTopic(), 
					createTopic.getNumPartitions(),
					createTopic.getReplicationFactor());
			topicSet.add(newTopic);

			AdminClient adminClient = createAdminClient();
			CreateTopicsResult result = adminClient.createTopics(topicSet);
			KafkaFuture<Void> futTopic = result.all();
			futTopic.get();

			adminClient.close();

		} catch (Exception e) {
			LOGGER.error(">>> Error!!!, createTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		}
	}
	//	public void createTopic0(String topic, Integer numPartitions, Short replicationFactor) throws Exception {
	//		LOGGER.info(">>>>>>>>>>>> createTopic topic=={}", topic);
	//		try {
	//			Set<NewTopic> topicSet = new HashSet<>();
	//			NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
	//			topicSet.add(newTopic);
	//			
	//			AdminClient adminClient = createAdminClient();
	//			CreateTopicsResult result = adminClient.createTopics(topicSet);
	//			KafkaFuture<Void> futTopic = result.all();
	//			futTopic.get();
	//			
	//			adminClient.close();
	//
	//		} catch (Exception e) {
	//			LOGGER.error(">>> Error!!!, createTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
	//			throw e;
	//		}
	//	}
	public void deleteTopic(String topic) throws Exception {
		LOGGER.info(">>>>>>>>>>>> deleteTopic topic=={}", topic);
		try {

			Set<String> topicSet = new HashSet<>();
			topicSet.add(topic);

			AdminClient adminClient = createAdminClient();
			DeleteTopicsResult result = adminClient.deleteTopics(topicSet);
			KafkaFuture<Void> futTopic = result.all();
			futTopic.get();

			adminClient.close();

		} catch (Exception e) {
			LOGGER.error(">>> Error!!!, deleteTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		}
	}
	public Optional<LastLogminerScn> lastLogminerScn(String topic) {
		LOGGER.info(">>>>>kafkaBootstrapServer={}",kafkaBootstrapServer);
		Consumer<String, String> consumer = createConsumer(kafkaBootstrapServer, "lastLogminerScn");

		LastLogminerScn lastLogminer = null;
		List<TopicPartition> tps = new ArrayList<>();
		Map<String, List<PartitionInfo>> map = consumer.listTopics();
		for (String t : map.keySet()) {	
			//	if (topic.startsWith("EBAOPRD1")) {
			for (PartitionInfo pi : map.get(t)) {
				if (StringUtils.equals(pi.topic(), topic)) {
					tps.add(new TopicPartition(pi.topic(), pi.partition()));
					LOGGER.info(">>>>>topic={}, partition={}", pi.topic(), pi.partition());
				}
			}

			//		}
		}
		consumer.assign( tps);

		String selectedTopic = "";
		Integer partition = null;
		Long offset = null;
		long lastScn = 0L;
		long lastCommitScn = 0L;
		String rowId = null;
		Long timestamp =  null;
		Map<TopicPartition, Long> offsetMap = consumer.endOffsets(tps);
		for (TopicPartition tp : offsetMap.keySet()) {
			//		for (TopicPartition tp : tps) {
			//			long position = consumer.position(tp);
			offset = offsetMap.get(tp);

			if (offset == 0) {
				continue;
			}

			LOGGER.info("topic:{}, partition:{},offset:{}", tp.topic(), tp.partition(), offset);
			consumer.seek(tp, offset- 1);

			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			System.out.println("record count:" + consumerRecords.count());

			for (ConsumerRecord<String, String> record : consumerRecords) {
				LOGGER.info("record key:{}, topic:{}, partition:{},offset:{}, timestamp:{}",
						record.key(), record.topic(), record.partition(), record.offset(), record.timestamp());
				//				System.out.println("Record Key " + record.key());
				//				System.out.println("Record value " + record.value());
				//				System.out.println("Record topic " + record.topic() + " partition " + record.partition());
				//				System.out.println("Record offset " + record.offset() + " timestamp " + record.timestamp());

				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

				try {
					JsonNode jsonNode = objectMapper.readTree(record.value());
					JsonNode payload = jsonNode.get("payload");
					//	payloadStr = payload.toString();

					String operation = payload.get("OPERATION").asText();
					String tableName = payload.get("TABLE_NAME").asText();
					Long scn = Long.valueOf(payload.get("SCN").asText());
					Long commitScn = Long.valueOf(payload.get("COMMIT_SCN").asText());

					LOGGER.info("operation:{},tableName:{},scn:{}, commitScn:{}, rowId:{},timestamp",operation,tableName,scn,commitScn,rowId,timestamp);

					if (scn.longValue() > lastScn) {
						lastScn = scn.longValue();
						rowId = payload.get("ROW_ID").asText();
						timestamp = Long.valueOf(payload.get("TIMESTAMP").asText());
						selectedTopic = tp.topic();
						partition = tp.partition();
					}
					if (commitScn.longValue() > lastCommitScn) {
						lastCommitScn = commitScn.longValue();
					}
				} catch(Exception e) {
					e.printStackTrace();
				} 
			}

		}
		consumer.close();

		lastLogminer = new LastLogminerScn(selectedTopic, partition, offset, lastScn, lastCommitScn, rowId, timestamp);
		return Optional.of(lastLogminer);
	}
	public Optional<LastLogminerScn> lastLogminerScnByConnector(String connector) {
		LOGGER.info(">>>>>kafkaBootstrapServer={}",kafkaBootstrapServer);
		Consumer<String, String> consumer = createConsumer(kafkaBootstrapServer, "lastLogminerScn");

		LastLogminerScn lastLogminer = null;
		List<TopicPartition> tps = new ArrayList<>();
		Map<String, List<PartitionInfo>> map = consumer.listTopics();
		for (String t : map.keySet()) {	
			//	if (topic.startsWith("EBAOPRD1")) {
			for (PartitionInfo pi : map.get(t)) {
				tps.add(new TopicPartition(pi.topic(), pi.partition()));
				//				LOGGER.info(">>>>>topic={}, partition={}", pi.topic(), pi.partition());
			}

			//		}
		}
		consumer.assign( tps);

		String selectedTopic = "";
		Integer partition = null;
		long scn = 0L;
		long lastScn = 0L;
		long commitScn = 0L;
		long lastCommitScn = 0L;
		Long offset = null;
		String rowId = null;
		Long timestamp =  null;
		Map<TopicPartition, Long> offsetMap = consumer.endOffsets(tps);
		for (TopicPartition tp : offsetMap.keySet()) {
			//		for (TopicPartition tp : tps) {
			//			long position = consumer.position(tp);
			offset = offsetMap.get(tp);

			if (offset == 0) {
				continue;
			} else if (StringUtils.equals("__consumer_offsets", tp.topic())){
				continue;
			}
			

			LOGGER.info(">>>>>topic:{}, partition:{},offset:{}", tp.topic(), tp.partition(), offset);
			consumer.seek(tp, offset- 1);

			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			System.out.println(">>>>>record count:" + consumerRecords.count());

			for (ConsumerRecord<String, String> record : consumerRecords) {
				LOGGER.info(">>>>>record key:{}, value={}, topic:{}, partition:{},offset:{}, timestamp:{}",
						record.key(), record.value(), record.topic(), record.partition(), record.offset(), record.timestamp());
								
				if (record.value() == null) {
					continue;
				}
								
				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

				try {
					JsonNode jsonNode = objectMapper.readTree(record.value());
					JsonNode payload = jsonNode.get("payload");
					//	payloadStr = payload.toString();

					String srcConnector = (payload.get("CONNECTOR") == null)? "" : payload.get("CONNECTOR").asText();

					if (StringUtils.equals(srcConnector, connector)) {
						scn = Long.valueOf(payload.get("SCN").asText());
						
						if (scn > lastScn) {
							commitScn = Long.valueOf(payload.get("COMMIT_SCN").asText());
							rowId = payload.get("ROW_ID").asText();
							timestamp = Long.valueOf(payload.get("TIMESTAMP").asText());
							selectedTopic = tp.topic();
							partition = tp.partition();
							
							lastScn = scn;
							lastCommitScn = commitScn;
						}
						LOGGER.info(">>>>>scn:{}, commitScn:{}, rowId:{},timestamp",scn,commitScn,rowId,timestamp);

					}
				} catch(Exception e) {
					e.printStackTrace();
					continue;
				} 
			}

		}
		consumer.close();

		lastLogminer = new LastLogminerScn(selectedTopic, partition, offset, scn, commitScn, rowId, timestamp);
		return Optional.of(lastLogminer);
	}
	private AdminClient createAdminClient() {
		Properties props = new Properties();
		props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
		return  AdminClient.create(props);
	}
	private Consumer<String, String> createConsumer(String kafkaBootstrapServer, String kafkaGroupId) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		Consumer<String, String> consumer = new KafkaConsumer<>(props);

		return consumer;
	}
}

