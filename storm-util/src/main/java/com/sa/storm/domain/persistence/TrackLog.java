package com.sa.storm.domain.persistence;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sa.storm.domain.tuple.BaseTuple;

public class TrackLog extends BasePersistenceObject {

	private static final int FIELD_MAX_LENGTH = 30;

	private String requestId;

	private String parentRequestId;

	private String sourceType;

	private Integer priority;

	private Integer resultCode;

	private Integer type;

	private String params;

	private String contexts;

	private String histories;

	private Long createTime;

	public TrackLog() {

	}

	public TrackLog(BaseTuple tuple) {
		this.requestId = tuple.getRequestId();
		this.parentRequestId = tuple.getParentRequestId();
		this.sourceType = tuple.getSourceType();
		this.priority = tuple.getPriority();
		this.resultCode = tuple.getResultCode();
		this.type = tuple.getType();

		StringBuffer params = new StringBuffer();
		for (Map.Entry<String, String> pair : tuple.getParams().entrySet()) {
			params.append(pair.getKey()).append(":").append(capString(pair.getValue(), FIELD_MAX_LENGTH)).append("; ");
		}
		this.params = params.toString();

		StringBuffer contexts = new StringBuffer();
		for (Map.Entry<String, String> pair : tuple.getContexts().entrySet()) {
			contexts.append(pair.getKey()).append(":").append(capString(pair.getValue(), FIELD_MAX_LENGTH)).append("; ");
		}
		this.contexts = contexts.toString();

		StringBuffer histories = new StringBuffer();
		for (String history : tuple.getHistories()) {
			histories.append(history).append("\n");
		}
		this.histories = histories.toString();
	}

	@JsonProperty("requestId")
	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	@JsonProperty("parentRequestId")
	public String getParentRequestId() {
		return parentRequestId;
	}

	public void setParentRequestId(String parentRequestId) {
		this.parentRequestId = parentRequestId;
	}

	@JsonProperty("sourceType")
	public String getSourceType() {
		return sourceType;
	}

	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}

	@JsonProperty("priority")
	public Integer getPriority() {
		return priority;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	@JsonProperty("resultCode")
	public Integer getResultCode() {
		return resultCode;
	}

	public void setResultCode(Integer resultCode) {
		this.resultCode = resultCode;
	}

	@JsonProperty("type")
	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

	@JsonProperty("params")
	public String getParams() {
		return params;
	}

	public void setParams(String params) {
		this.params = params;
	}

	@JsonProperty("contexts")
	public String getContexts() {
		return contexts;
	}

	public void setContexts(String contexts) {
		this.contexts = contexts;
	}

	@JsonProperty("histories")
	public String getHistories() {
		return histories;
	}

	public void setHistories(String histories) {
		this.histories = histories;
	}

	@JsonProperty("createTime")
	public Long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Long createTime) {
		this.createTime = createTime;
	}

	@Override
	public String toString() {
		StringBuffer output = new StringBuffer();
		output.append("Request ID: ").append(requestId).append("\n");
		output.append("Parent Request ID: ").append(parentRequestId).append("\n");
		output.append("Source type: ").append(sourceType).append("\n");
		output.append("Priority: ").append(priority).append("\n");
		output.append("Result code: ").append(resultCode).append("\n");
		output.append("Type: ").append(type).append("\n");
		output.append("Params: ").append(params).append("\n");
		output.append("Contexts: ").append(contexts).append("\n");
		output.append("Histories:\n").append(histories).append("\n");

		return output.toString();
	}

	private String capString(String value, int maxLength) {
		if (value != null && value.length() > maxLength) {
			return value.substring(0, maxLength) + "...";
		}
		return value;
	}
}
