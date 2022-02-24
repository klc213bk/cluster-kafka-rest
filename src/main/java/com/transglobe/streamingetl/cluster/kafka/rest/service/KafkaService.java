package com.transglobe.streamingetl.cluster.kafka.rest.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
	static final Logger LOGGER = LoggerFactory.getLogger(KafkaService.class);
	
	@Value("${kafka.server.home}")
	private String kafkaServerHome;

	@Value("${kafka.start.script}")
	private String kafkaStartScript;
	
	@Value("${kafka.cluster.prop.A}")
	private String kafkaClusterPropA;
	
	@Value("${kafka.cluster.prop.B}")
	private String kafkaClusterPropB;

	private Process clusterStartProcessA;
	private ExecutorService clusterStartExecutorA;
	
	private Process clusterStartProcessB;
	private ExecutorService clusterStartExecutorB;

	public void startCluster() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.startCluster starting");
		try {
			if (clusterStartProcessA == null || !clusterStartProcessA.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> clusterStartProcessA.isAlive={} ", (clusterStartProcessA == null)? null : clusterStartProcessA.isAlive());
				//				kafkaStartFinished0.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(kafkaStartScript, kafkaClusterPropA);

				builder.directory(new File(kafkaServerHome));
				clusterStartProcessA = builder.start();

				clusterStartExecutorA = Executors.newSingleThreadExecutor();
				clusterStartExecutorA.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(clusterStartProcessA.getInputStream()));
						reader.lines().forEach(line -> {
							LOGGER.info(line);

						});
					}

				});
				
				ProcessBuilder builder2 = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder2.command(kafkaStartScript, kafkaClusterPropB);

				builder2.directory(new File(kafkaServerHome));
				clusterStartProcessB = builder2.start();

				clusterStartExecutorB = Executors.newSingleThreadExecutor();
				clusterStartExecutorB.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(clusterStartProcessB.getInputStream()));
						reader.lines().forEach(line -> {
							LOGGER.info(line);

						});
					}

				});
				
				while (true) {
					try {
						listTopics();
						break;
					} catch (Exception e) {
						LOGGER.info(">>>> listTopics error occurred. Sleep for 1 second");;
						Thread.sleep(1000);
						continue;
					}
				}
				LOGGER.info(">>>>>>>>>>>> clusterStartProcessA.isAlive={} ", (clusterStartProcessA == null)? null : clusterStartProcessA.isAlive());
				LOGGER.info(">>>>>>>>>>>> clusterStartProcessB.isAlive={} ", (clusterStartProcessB == null)? null : clusterStartProcessB.isAlive());
				
				
				LOGGER.info(">>>>>>>>>>>> KafkaService.startCluster End");
			} else {
				LOGGER.warn(" >>> clusterStartProcess is currently Running.");
			}
		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, startCluster, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	
	public void stopCluster() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.stopCluster starting...");
		try {
			LOGGER.info(">>>>>>>>>>>> clusterStartProcessA.isAlive={} ", (clusterStartProcessA == null)? null : clusterStartProcessA.isAlive());

			LOGGER.info(">>>>>>>>>>>> clusterStartProcessB.isAlive={} ", (clusterStartProcessB == null)? null : clusterStartProcessB.isAlive());
			
			if (clusterStartProcessB != null && clusterStartProcessB.isAlive()) {
				//				
				clusterStartProcessB.destroy();


				LOGGER.info(">>>>>>>>>>>> KafkaService.stopCluster End");
			} else {
				LOGGER.warn(" >>> clusterStartProcessB IS NOT ALIVE.");
			}
			
			if (clusterStartProcessA != null && clusterStartProcessA.isAlive()) {
				//				
				clusterStartProcessA.destroy();


				LOGGER.info(">>>>>>>>>>>> KafkaService.stopCluster End");
			} else {
				LOGGER.warn(" >>> clusterStartProcessA IS NOT ALIVE.");
			}

			if (!clusterStartExecutorB.isTerminated()) {
				if (!clusterStartExecutorB.isShutdown()) {
					clusterStartExecutorB.shutdown();
				}
				while (!clusterStartExecutorB.isShutdown()) {
					Thread.sleep(1000);
					LOGGER.info(">>>> waiting for executor shuttung down.");;
				}

				while (!clusterStartExecutorB.isTerminated()) {
					Thread.sleep(1000);
					LOGGER.info(">>>> waiting for executor termainting.");;
				}
			} 
			if (clusterStartExecutorB.isTerminated()) {
				LOGGER.info(">>>> clusterStartExecutorB is Terminated!!!!!");
			} 
			
			if (!clusterStartExecutorA.isTerminated()) {
				if (!clusterStartExecutorA.isShutdown()) {
					clusterStartExecutorA.shutdown();
				}
				while (!clusterStartExecutorA.isShutdown()) {
					Thread.sleep(1000);
					LOGGER.info(">>>> waiting for executor shuttung down.");;
				}

				while (!clusterStartExecutorA.isTerminated()) {
					Thread.sleep(1000);
					LOGGER.info(">>>> waiting for executor termainting.");;
				}
			} 
			if (clusterStartExecutorA.isTerminated()) {
				LOGGER.info(">>>> clusterStartExecutorA is Terminated!!!!!");
			} 

		} catch (Exception e) {
			LOGGER.error(">>> Error!!!, stopCluster, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	
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
	public void createTopic(String topic, Integer numPartitions, Short replicationFactor) throws Exception {
		LOGGER.info(">>>>>>>>>>>> createTopic topic=={}", topic);
		try {
			Set<NewTopic> topicSet = new HashSet<>();
			NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
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
	private AdminClient createAdminClient() {
		Properties props = new Properties();
		props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		return  AdminClient.create(props);
	}

}

