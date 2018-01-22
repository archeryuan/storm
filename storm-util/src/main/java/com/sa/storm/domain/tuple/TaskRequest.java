package com.sa.storm.domain.tuple;

import java.util.HashMap;
import java.util.Map;

import com.sa.crawler.message.TaskRequestMessage;

public class TaskRequest extends BaseTuple {

	private static final long serialVersionUID = 1L;

	public TaskRequest(TaskRequestMessage message) {
		super(message.getId(), message.getParentId(), message.getType(), message.getSourceType(), message.getPriority(), message
				.getParams());
		addAllContexts(message.getStates());
	}

	public TaskRequest() {
		super();
	}

	public TaskRequest(BaseTuple original) {
		super(original);
	}

	public TaskRequest(String requestId, BaseTuple original) {
		super(requestId, original);
	}

	public TaskRequest(String requestId, String parentRequestId, Integer type, String sourceType, Map<String, String> params) {
		super(requestId, parentRequestId, type, sourceType, params);
	}

	public TaskRequest(String requestId, String parentRequestId, Integer type, String sourceType, Integer priority,
			Map<String, String> params) {
		super(requestId, parentRequestId, type, sourceType, priority, params);
	}

	public TaskRequestMessage toMessage() {
		TaskRequestMessage message = new TaskRequestMessage();
		message.setId(getRequestId());
		message.setParentId(getParentRequestId());
		message.setParams(new HashMap<String, String>(getParams()));
		message.setSourceType(getSourceType());
		message.setPriority(getPriority());
		message.setStates(new HashMap<String, String>(getContexts()));
		message.setType(getType());

		return message;
	}
}
