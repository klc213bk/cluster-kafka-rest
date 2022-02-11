package com.transglobe.streamingetl.cluster.kafka.rest.controller;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.transglobe.streamingetl.cluster.kafka.rest.service.KafkaService;


@RestController
@RequestMapping("/")
public class KafkaController {
	static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

	@Autowired
	private KafkaService kafkaService;
	
	@Autowired
	private ObjectMapper mapper;

	@GetMapping(path="/ok")
	@ResponseBody
	public ResponseEntity<String> ok() {
		logger.info(">>>>controller ok is called");
		
		
		
		logger.info(">>>>controller ok finished ");
		
		return new ResponseEntity<String>("OK", HttpStatus.OK);
	}
	@PostMapping(path="/startCluster", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> startCluster() {
		logger.info(">>>>controller startCluster is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			kafkaService.startCluster();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller startCluster finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/stopCluster", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> stopCluster() {
		logger.info(">>>>controller stopCluster is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			kafkaService.stopCluster();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller stopCluster finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	
	@GetMapping(path="/listTopics", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> listTopics() {
		logger.info(">>>>controller listTopics is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
	
		try {
			Set<String> topics = kafkaService.listTopics();
			List<String> topicList = new ArrayList<>();
			for (String t : topics) {
				topicList.add(t);
			}
			
			final ByteArrayOutputStream out = new ByteArrayOutputStream();
		    final ObjectMapper mapper = new ObjectMapper();

		    mapper.writeValue(out, topicList);

		    final byte[] data = out.toByteArray();
		    
		    String jsonStr =  new String(data);

			objectNode.put("returnCode", "0000");
			objectNode.put("topics", jsonStr);
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		logger.info(">>>>controller listTopics finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/createTopic/{topic}/rf/{replicationFactor}/np/{numPartitions}", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> createTopic(@PathVariable("topic") String topic, @PathVariable("replicationFactor") Integer replicationFactor, @PathVariable("numPartitions") Integer numPartitions) {
		logger.info(">>>>controller createTopic is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
	
		try {
			kafkaService.createTopic(topic, replicationFactor, numPartitions);
			
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		logger.info(">>>>controller createTopic finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/deleteTopic/{topic}", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> deleteTopic(@PathVariable("topic") String topic) {
		logger.info(">>>>controller deleteTopic is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
	
		try {
			kafkaService.deleteTopic(topic);
			
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		logger.info(">>>>controller deleteTopic finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
//	@PostMapping(path="/deleteAllTopics", produces=MediaType.APPLICATION_JSON_VALUE)
//	@ResponseBody
//	public ResponseEntity<Object> deleteAllTopics() {
//		logger.info(">>>>controller deleteAllTopics is called");
//		
//		ObjectNode objectNode = mapper.createObjectNode();
//	
//		try {
//			kafkaService.deleteAllTopics();
//			
//			objectNode.put("returnCode", "0000");
//		} catch (Exception e) {
//			String errMsg = ExceptionUtils.getMessage(e);
//			String stackTrace = ExceptionUtils.getStackTrace(e);
//			objectNode.put("returnCode", "-9999");
//			objectNode.put("errMsg", errMsg);
//			objectNode.put("returnCode", stackTrace);
//			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
//		}
//		
//		logger.info(">>>>controller deleteAllTopics finished ");
//		
//		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
//	}
//	@GetMapping(value="/lastLogminerScn")
//	@ResponseBody
//	public ResponseEntity<LastLogminerScn> getEbaoKafkaLastLogminerScn(){
//		logger.info(">>>>getKafkaLastLogminerScn begin");
//		long t0 = System.currentTimeMillis();
//		String errMsg = null;
//		String returnCode = "0000";
//		Optional<LastLogminerScn> logminerLastScn = null;
//		try {
//			logminerLastScn = kafkaService.getEbaoKafkaLastLogminerScn();
//			logger.info("    >>>>getKafkaLastLogminerScn finished.");
//
//			if (logminerLastScn.isPresent()) {
//				return new ResponseEntity<>(logminerLastScn.get(), HttpStatus.OK);
//			} else {
//				throw new ResponseStatusException(HttpStatus.NOT_FOUND);
//			}
//		} catch (Exception e) {
//			returnCode = "-9999";
//			errMsg = ExceptionUtils.getMessage(e);
//			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
//		}
//
//		long t1 = System.currentTimeMillis();
//
//		logger.info(">>>>getKafkaLastLogminerScn finished returnCode={}, span={}", returnCode, (t1 - t0));
//
//		return ResponseEntity.status(HttpStatus.OK).body(logminerLastScn.get());
//
//	}

}
