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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
	static final Logger LOGGER = LoggerFactory.getLogger(KafkaService.class);

	@Value("${cluster.start.script}")
	private String clusterStartScript;
	
	@Value("${kafka.server.home}")
	private String kafkaServerHome;

	@Value("${kafka.server.port}")
	private String kafkaServerPortStr;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;

	private Process clusterStartProcess;
	private ExecutorService clusterStartExecutor;

	//	private Process kafkaStopProcess;
	//	private Process kafkaStopProcessOne;
	//	private Process kafkaStopProcessTwo;

	public void startCluster() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.startCluster starting");
		try {
			if (clusterStartProcess == null || !clusterStartProcess.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> clusterStartProcess.isAlive={} ", (clusterStartProcess == null)? null : clusterStartProcess.isAlive());
				//				kafkaStartFinished0.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(clusterStartScript);

				builder.directory(new File("."));
				clusterStartProcess = builder.start();

				clusterStartExecutor = Executors.newSingleThreadExecutor();
				clusterStartExecutor.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(clusterStartProcess.getInputStream()));
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
			if (clusterStartProcess != null && clusterStartProcess.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> clusterStartProcess.isAlive={} ", (clusterStartProcess == null)? null : clusterStartProcess.isAlive());
				//				
				clusterStartProcess.destroy();

				int kafkaServerPort = Integer.valueOf(kafkaServerPortStr);
				while (checkPortListening(kafkaServerPort)) {
					Thread.sleep(10000);
					LOGGER.info(">>>> Sleep for 10 second");;
				}

				LOGGER.info(">>>>>>>>>>>> KafkaService.stopCluster End");
			} else {
				LOGGER.warn(" >>> clusterStartProcess IS NOT ALIVE.");
			}

			if (!clusterStartExecutor.isTerminated()) {
				if (!clusterStartExecutor.isShutdown()) {
					clusterStartExecutor.shutdown();
				}
				while (!clusterStartExecutor.isShutdown()) {
					Thread.sleep(1000);
					LOGGER.info(">>>> waiting for executor shuttung down.");;
				}

				while (!clusterStartExecutor.isTerminated()) {
					Thread.sleep(1000);
					LOGGER.info(">>>> waiting for executor termainting.");;
				}
			} 
			if (clusterStartExecutor.isTerminated()) {
				LOGGER.info(">>>> clusterStartExecutor is Terminated!!!!!");
			} 

		} catch (IOException e) {
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
			
//			ProcessBuilder builder = new ProcessBuilder();
//			String script = "./bin/kafka-topics.sh" + " --list --bootstrap-server " + kafkaBootstrapServer;
//			builder.command("sh", "-c", script);
//			//				builder.command(kafkaTopicsScript + " --list --bootstrap-server " + kafkaBootstrapServer);
//
//			//				builder.command(kafkaTopicsScript, "--list", "--bootstrap-server", kafkaBootstrapServer);
//
//			builder.directory(new File(kafkaServerHome));
//			Process listTopicsProcess = builder.start();
//
//			ExecutorService listTopicsExecutor = Executors.newSingleThreadExecutor();
//			listTopicsExecutor.submit(new Runnable() {
//
//				@Override
//				public void run() {
//					BufferedReader reader = new BufferedReader(new InputStreamReader(listTopicsProcess.getInputStream()));
//					reader.lines().forEach(topic -> topics.add(topic));
//				}
//
//			});
//			int exitVal = listTopicsProcess.waitFor();
//			if (exitVal == 0) {
//
//				LOGGER.info(">>> Success!!! listTopics, exitVal={}", exitVal);
//			} else {
//				LOGGER.error(">>> Error!!! listTopics, exitcode={}", exitVal);
//				String errStr = (topics.size() > 0)? topics.get(0) : "";
//				throw new Exception(errStr);
//			}



		} catch (Exception e) {
			LOGGER.error(">>> Error!!!, listTopics, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 

	}
	public void createTopic(String topic, Integer replicationFactor, Integer numPartitions) throws Exception {
		LOGGER.info(">>>>>>>>>>>> createTopic topic=={}", topic);
		try {

			ProcessBuilder builder = new ProcessBuilder();
			String script = "./bin/kafka-topics.sh --create --bootstrap-server " + kafkaBootstrapServer + " --replication-factor " + replicationFactor + " --partitions " + numPartitions + " --topic " + topic;
			builder.command("sh", "-c", script);

			builder.directory(new File(kafkaServerHome));
			Process createTopicProcess = builder.start();

			int exitVal = createTopicProcess.waitFor();
			if (exitVal == 0) {
				LOGGER.info(">>> Success!!! createTopic:{}, exitcode={}", topic, exitVal);
			} else {
				LOGGER.error(">>> Error!!! createTopic:{}, exitcode={}", topic, exitVal);
			}
			LOGGER.info(">>> createTopicProcess isalive={}", createTopicProcess.isAlive());
			if (!createTopicProcess.isAlive()) {
				createTopicProcess.destroy();
			}


		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, createTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} catch (InterruptedException e) {
			LOGGER.error(">>> Error!!!, createTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void deleteTopic(String topic) throws Exception {
		LOGGER.info(">>>>>>>>>>>> deleteTopic topic=={}", topic);
		try {

			ProcessBuilder builder = new ProcessBuilder();
			String script = "./bin/kafka-topics.sh --delete --bootstrap-server " + kafkaBootstrapServer + " --topic " + topic;
			builder.command("sh", "-c", script);

			builder.directory(new File(kafkaServerHome));
			Process deleteTopicProcess = builder.start();

			int exitVal = deleteTopicProcess.waitFor();
			if (exitVal == 0) {
				LOGGER.info(">>> Success!!! deleteTopic:{}, exitcode={}", topic, exitVal);
			} else {
				LOGGER.error(">>> Error!!! deleteTopic:{}, exitcode={}", topic, exitVal);
			}
			LOGGER.info(">>> deleteTopicProcess isalive={}", deleteTopicProcess.isAlive());
			if (!deleteTopicProcess.isAlive()) {
				deleteTopicProcess.destroy();
			}

		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, deleteTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} catch (InterruptedException e) {
			LOGGER.error(">>> Error!!!, deleteTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	private AdminClient createAdminClient() {
		Properties props = new Properties();
		props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		return  AdminClient.create(props);
	}
	private boolean checkPortListening(int port) throws Exception {
		LOGGER.info(">>>>>>>>>>>> checkPortListening:{} ", port);

		BufferedReader reader = null;
		try {
			ProcessBuilder builder = new ProcessBuilder();
			String script = "netstat -tnlp | grep :" + port;
			builder.command("bash", "-c", script);
			//				builder.command(kafkaTopicsScript + " --list --bootstrap-server " + kafkaBootstrapServer);

			//				builder.command(kafkaTopicsScript, "--list", "--bootstrap-server", kafkaBootstrapServer);

			builder.directory(new File("."));
			Process checkPortProcess = builder.start();

			AtomicBoolean portRunning = new AtomicBoolean(false);


			int exitVal = checkPortProcess.waitFor();
			if (exitVal == 0) {
				reader = new BufferedReader(new InputStreamReader(checkPortProcess.getInputStream()));
				reader.lines().forEach(line -> {
					if (StringUtils.contains(line, "LISTEN")) {
						portRunning.set(true);
						LOGGER.info(">>> Success!!! portRunning.set(true)");
					}
				});
				reader.close();

				LOGGER.info(">>> Success!!! portRunning={}", portRunning.get());
			} else {
				LOGGER.error(">>> Error!!!  exitcode={}", exitVal);


			}
			if (checkPortProcess.isAlive()) {
				checkPortProcess.destroy();
			}

			return portRunning.get();
		} finally {
			if (reader != null) reader.close();
		}

	}
}

