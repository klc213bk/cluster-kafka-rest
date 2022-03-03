package com.transglobe.streamingetl.cluster.kafka.rest.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LastLogminerScn {

	@JsonProperty("topic")
	private String topic;
	
	@JsonProperty("partition")
	private Integer partition;
	
	@JsonProperty("lastScn")
	private Long lastScn;
	
	@JsonProperty("lastCommitScn")
	private Long lastCommitScn;
	
	@JsonProperty("rowId")
	private String rowId;
	
	@JsonProperty("timestamp")
	private Long timestamp;

	public LastLogminerScn() { }
	
	public LastLogminerScn(String topic, Integer partition, Long lastScn, Long lastCommitScn, String rowId, Long timestamp) {
		this.topic = topic;
		this.partition = partition;
		this.lastScn = lastScn;
		this.lastCommitScn = lastCommitScn;
		this.rowId = rowId;
		this.timestamp = timestamp;
		
	}
	
	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Integer getPartition() {
		return partition;
	}

	public void setPartition(Integer partition) {
		this.partition = partition;
	}

	public Long getLastScn() {
		return lastScn;
	}

	public void setLastScn(Long lastScn) {
		this.lastScn = lastScn;
	}

	public Long getLastCommitScn() {
		return lastCommitScn;
	}

	public void setLastCommitScn(Long lastCommitScn) {
		this.lastCommitScn = lastCommitScn;
	}

	public String getRowId() {
		return rowId;
	}

	public void setRowId(String rowId) {
		this.rowId = rowId;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	
	
	
}
