package com.transglobe.streamingetl.cluster.kafka.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
	static final Logger LOGGER = LoggerFactory.getLogger(KafkaService.class);

	@Value("${kafka.server.home}")
	private String kafkaServerHome;

	@Value("${zookeeper.start.script}")
	private String zookeeperStartScript;

	@Value("${zookeeper.start.properties}")
	private String zookeeperStartProperties;
	
	@Value("${zookeeper.server.port}")
	private String zookeeperServerPortStr;
	
	@Value("${zookeeper.stop.script}")
	private String zookeeperStopScript;
	
	@Value("${kafka.start.script}")
	private String kafkaStartScript;

	@Value("${kafka.start.properties}")
	private String kafkaStartProperties;
	
	@Value("${kafka.start.properties.one}")
	private String kafkaStartPropertiesOne;
	
	@Value("${kafka.start.properties.two}")
	private String kafkaStartPropertiesTwo;

	@Value("${kafka.stop.script}")
	private String kafkaStopScript;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;
	
	@Value("${kafka.server.port}")
	private String kafkaServerPortStr;
	
	@Value("${kafka.server.port.one}")
	private String kafkaServerPortOneStr;
	
	@Value("${kafka.server.port.two}")
	private String kafkaServerPortTwoStr;
	
	private Process zookeeperStartProcess;
//	private ExecutorService zookeeperStartExecutor;
//	private AtomicBoolean zookeeperStartFinished = new AtomicBoolean(false);
//	private AtomicBoolean zookeeperStopFinished = new AtomicBoolean(false);
	private Process zookeeperStopProcess;
	
	private Process kafkaStartProcess;
	private Process kafkaStartProcessOne;
	private Process kafkaStartProcessTwo;
//	private Process kafkaStopProcess;
//	private Process kafkaStopProcessOne;
//	private Process kafkaStopProcessTwo;
	
	public void startZookeeper() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.startZookeeper starting");
		try {
			if (zookeeperStartProcess == null || !zookeeperStartProcess.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> zookeeperStartProcess.isAlive={} ", (zookeeperStartProcess == null)? null : zookeeperStartProcess.isAlive());
//				zookeeperStartFinished.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(zookeeperStartScript, zookeeperStartProperties);

				builder.directory(new File(kafkaServerHome));
				zookeeperStartProcess = builder.start();

//				zookeeperStartExecutor = Executors.newSingleThreadExecutor();
//				zookeeperStartExecutor.submit(new Runnable() {
//
//					@Override
//					public void run() {
//						BufferedReader reader = new BufferedReader(new InputStreamReader(zookeeperStartProcess.getInputStream()));
//						reader.lines().forEach(line -> {
//							LOGGER.info("********"+line);
//						
//						});
//					}
//
//				});

				int zookeeperServerPort = Integer.valueOf(zookeeperServerPortStr);
				while (!checkPortListening(zookeeperServerPort)) {
					Thread.sleep(1000);
					LOGGER.info(">>>> Sleep for 1 second");;
				}

				LOGGER.info(">>>>>>>>>>>> KafkaService.startZookeeper End");
			} else {
				LOGGER.warn(" >>> zookeeperStartProcess is currently Running.");
			}
		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, startZookeeper, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void stopZookeeper() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.stopZookeeper starting...");
		try {
			if (zookeeperStopProcess == null || !zookeeperStopProcess.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> zookeeperStopProcess.isAlive={} ", (zookeeperStopProcess == null)? null : zookeeperStopProcess.isAlive());
//				zookeeperStopFinished.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(zookeeperStopScript);

				builder.directory(new File(kafkaServerHome));
				zookeeperStopProcess = builder.start();

				int exitVal = zookeeperStopProcess.waitFor();
				if (exitVal == 0) {
//					zookeeperStopFinished.set(true);
					LOGGER.info(">>> Success!!! stopZookeeper, exitVal={}", exitVal);
				} else {
					LOGGER.error(">>> Error!!! stopZookeeper, exitcode={}", exitVal);
//					zookeeperStopFinished.set(true);
				}
				int zookeeperServerPort = Integer.valueOf(zookeeperServerPortStr);
				while (checkPortListening(zookeeperServerPort)) {
					Thread.sleep(1000);
					LOGGER.info(">>>> Sleep for 1 second");;
				}

				if (!zookeeperStopProcess.isAlive()) {
					zookeeperStopProcess.destroy();
				}

				LOGGER.info(">>>>>>>>>>>> KafkaService.stopZookeeper End");
			} else {
				LOGGER.warn(" >>> zookeeperStopProcess is currently Running.");
			}
		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, stopZookeeper, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void startKafka() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.startKafka starting");
		try {
			if (kafkaStartProcess == null || !kafkaStartProcess.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> kafkaStartProcess.isAlive={} ", (kafkaStartProcess == null)? null : kafkaStartProcess.isAlive());
//				kafkaStartFinished0.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(kafkaStartScript, kafkaStartProperties);

				builder.directory(new File(kafkaServerHome));
				kafkaStartProcess = builder.start();

//				kafkaStartExecutor = Executors.newSingleThreadExecutor();
//				kafkaStartExecutor.submit(new Runnable() {
//
//					@Override
//					public void run() {
//						BufferedReader reader = new BufferedReader(new InputStreamReader(kafkaStartProcess.getInputStream()));
//						reader.lines().forEach(line -> {
//							LOGGER.info(line);
//							
//						});
//					}
//
//				});
				int kafkaServerPort = Integer.valueOf(kafkaServerPortStr);
				while (!checkPortListening(kafkaServerPort)) {
					Thread.sleep(1000);
					LOGGER.info(">>>> Sleep for 1 second");;
				}
				Thread.sleep(15000);
				LOGGER.info(">>>>>>>>>>>> KafkaService.startKafka End");
			} else {
				LOGGER.warn(" >>> kafkaStartProcess is currently Running.");
			}
		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, startKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void startKafkaOne() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.startKafkaOne starting");
		try {
			if (kafkaStartProcessOne == null || !kafkaStartProcessOne.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> kafkaStartProcessOne.isAlive={} ", (kafkaStartProcessOne == null)? null : kafkaStartProcessOne.isAlive());
//				kafkaStartFinished0.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(kafkaStartScript, kafkaStartPropertiesOne);

				builder.directory(new File(kafkaServerHome));
				kafkaStartProcessOne = builder.start();

//				kafkaStartExecutor = Executors.newSingleThreadExecutor();
//				kafkaStartExecutor.submit(new Runnable() {
//
//					@Override
//					public void run() {
//						BufferedReader reader = new BufferedReader(new InputStreamReader(kafkaStartProcess.getInputStream()));
//						reader.lines().forEach(line -> {
//							LOGGER.info(line);
//							
//						});
//					}
//
//				});
				int kafkaServerPortOne = Integer.valueOf(kafkaServerPortOneStr);
				while (!checkPortListening(kafkaServerPortOne)) {
					Thread.sleep(1000);
					LOGGER.info(">>>> Sleep for 1 second");;
				}
				Thread.sleep(15000);
				LOGGER.info(">>>>>>>>>>>> KafkaService.startKafkaOne End");
			} else {
				LOGGER.warn(" >>> kafkaStartProcessOne is currently Running.");
			}
		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, startKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void startKafkaTwo() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.startKafkaTwo starting");
		try {
			if (kafkaStartProcessTwo == null || !kafkaStartProcessTwo.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> kafkaStartProcessTwo.isAlive={} ", (kafkaStartProcessOne == null)? null : kafkaStartProcessTwo.isAlive());
//				kafkaStartFinished0.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./bin/zookeeper-server-start.sh";
				//builder.command("sh", "-c", script);
				builder.command(kafkaStartScript, kafkaStartPropertiesTwo);

				builder.directory(new File(kafkaServerHome));
				kafkaStartProcessTwo = builder.start();

//				kafkaStartExecutor = Executors.newSingleThreadExecutor();
//				kafkaStartExecutor.submit(new Runnable() {
//
//					@Override
//					public void run() {
//						BufferedReader reader = new BufferedReader(new InputStreamReader(kafkaStartProcess.getInputStream()));
//						reader.lines().forEach(line -> {
//							LOGGER.info(line);
//							
//						});
//					}
//
//				});
				int kafkaServerPortTwo = Integer.valueOf(kafkaServerPortTwoStr);
				while (!checkPortListening(kafkaServerPortTwo)) {
					Thread.sleep(1000);
					LOGGER.info(">>>> Sleep for 1 second");;
				}
				Thread.sleep(15000);
				LOGGER.info(">>>>>>>>>>>> KafkaService.startKafkaTwo End");
			} else {
				LOGGER.warn(" >>> kafkaStartProcessTwo is currently Running.");
			}
		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, startKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void stopKafka() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.stopKafka starting...");
		try {
			if (kafkaStartProcess != null && kafkaStartProcess.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> kafkaStartProcess.isAlive={} ", (kafkaStartProcess == null)? null : kafkaStartProcess.isAlive());
//				
				kafkaStartProcess.destroy();
				
				int kafkaServerPort = Integer.valueOf(kafkaServerPortStr);
				while (checkPortListening(kafkaServerPort)) {
					Thread.sleep(10000);
					LOGGER.info(">>>> Sleep for 10 second");;
				}

				LOGGER.info(">>>>>>>>>>>> KafkaService.stopKafka End");
			} else {
				LOGGER.warn(" >>> kafkaStartProcess IS NOT ALIVE.");
			}
		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, stopKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void stopKafkaOne() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.stopKafkaOne starting...");
		try {
			if (kafkaStartProcessOne != null && kafkaStartProcessOne.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> kafkaStartProcessOne.isAlive={} ", (kafkaStartProcessOne == null)? null : kafkaStartProcessOne.isAlive());
//				
				kafkaStartProcessOne.destroy();
				
				int kafkaServerPortOne = Integer.valueOf(kafkaServerPortOneStr);
				while (checkPortListening(kafkaServerPortOne)) {
					Thread.sleep(10000);
					LOGGER.info(">>>> Sleep for 10 second");;
				}

				LOGGER.info(">>>>>>>>>>>> KafkaService.stopKafkaOne End");
			} else {
				LOGGER.warn(" >>> kafkaStartProcessOne IS NOT ALIVE.");
			}
		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, stopKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void stopKafkaTwo() throws Exception {
		LOGGER.info(">>>>>>>>>>>> KafkaService.stopKafkaTwo starting...");
		try {
			if (kafkaStartProcessTwo != null && kafkaStartProcessTwo.isAlive()) {
				LOGGER.info(">>>>>>>>>>>> kafkaStartProcessTwo.isAlive={} ", (kafkaStartProcessTwo == null)? null : kafkaStartProcessOne.isAlive());
//				
				kafkaStartProcessTwo.destroy();
				
				int kafkaServerPortOne = Integer.valueOf(kafkaServerPortTwoStr);
				while (checkPortListening(kafkaServerPortOne)) {
					Thread.sleep(10000);
					LOGGER.info(">>>> Sleep for 10 second");;
				}

				LOGGER.info(">>>>>>>>>>>> KafkaService.stopKafkaTwo End");
			} else {
				LOGGER.warn(" >>> kafkaStartProcessTwo IS NOT ALIVE.");
			}
		} catch (IOException e) {
			LOGGER.error(">>> Error!!!, stopKafka, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
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

