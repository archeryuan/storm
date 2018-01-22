package com.sa.storm.domain.tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sa.redis.definition.RedisDefinition.Priority;
import com.sa.storm.definition.TupleDefinition;

/**
 * A basic tuple which contain basic contextual information
 * 
 * @author Kelvin Wong
 * 
 */
public class BaseTuple implements Serializable {

	private static final long serialVersionUID = 1L;

	private String requestId;

	private String parentRequestId;

	private Integer type;

	private String sourceType;

	private Integer priority;

	private Integer resultCode;

	private Map<String, String> params;

	private Map<String, String> contexts;

	private List<String> histories;

	public BaseTuple() {

	}

	public BaseTuple(String requestId, Integer type, String sourceType, Map<String, String> params) {
		this.requestId = requestId;
		this.parentRequestId = null;
		this.type = type;
		this.sourceType = sourceType;
		this.priority = Priority.NORMAL.getCode();
		this.params = new HashMap<String, String>(params);
		this.contexts = new HashMap<String, String>();
		this.histories = new ArrayList<String>();
	}

	public BaseTuple(String requestId, String parentRequestId, Integer type, String sourceType, Map<String, String> params) {
		this(requestId, type, sourceType, params);
		this.parentRequestId = parentRequestId;
	}

	public BaseTuple(String requestId, String parentRequestId, Integer type, String sourceType, Integer priority, Map<String, String> params) {
		this(requestId, parentRequestId, type, sourceType, params);
		this.priority = priority;
	}

	/**
	 * Copy constructor that create a clone of the original object
	 * 
	 * @param original
	 */
	public BaseTuple(BaseTuple original) {
		this(original.requestId, original.parentRequestId, original.type, original.sourceType, original.priority, original.params);
		this.contexts = new HashMap<String, String>(original.contexts);
		this.histories = new ArrayList<String>(original.histories);
	}

	/**
	 * Copy constructor that accept a new request ID
	 * 
	 * @param requestId
	 * @param original
	 */
	public BaseTuple(String requestId, BaseTuple original) {
		this(original);
		setRequestId(requestId);
	}

	public String getRequestId() {
		return requestId;
	}

	protected void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getParentRequestId() {
		return parentRequestId;
	}

	public void setParentRequestId(String parentRequestId) {
		this.parentRequestId = parentRequestId;
	}

	public Integer getType() {
		return type;
	}

	protected void setType(Integer type) {
		this.type = type;
	}

	public String getSourceType() {
		return sourceType;
	}

	protected void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}

	public Integer getPriority() {
		return priority;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	public Integer getResultCode() {
		return resultCode;
	}

	public void setResultCode(Integer resultCode) {
		this.resultCode = resultCode;
	}

	/**
	 * Return immutable Map of params to prevent external modification
	 * 
	 * @return
	 */
	public Map<String, String> getParams() {
		return Collections.unmodifiableMap(params);
	}

	public String getParamByKey(TupleDefinition.Param key) {
		if (params != null) {
			return params.get(key.getCode());
		}

		return null;
	}

	public void addParam(TupleDefinition.Param key, String value) {
		if (params == null) {
			params = new HashMap<String, String>();
		}

		params.put(key.getCode(), value);
	}

	/**
	 * Return immutable Map of contexts to prevent external modification
	 * 
	 * @return
	 */
	public Map<String, String> getContexts() {
		return Collections.unmodifiableMap(contexts);
	}

	public String getContextByKey(TupleDefinition.Context key) {
		if (contexts != null) {
			return contexts.get(key.getCode());
		}

		return null;
	}

	public int getIntegerContextByKey(TupleDefinition.Context key, int defaultValue) {
		String value = getContextByKey(key);
		if (value != null && !value.isEmpty()) {
			return Integer.parseInt(value);
		} else {
			return defaultValue;
		}
	}

	public long getLongContextByKey(TupleDefinition.Context key, long defaultValue) {
		String value = getContextByKey(key);
		if (value != null && !value.isEmpty()) {
			return Long.parseLong(value);
		} else {
			return defaultValue;
		}
	}

	public void addContext(TupleDefinition.Context key, String value) {
		if (contexts == null) {
			contexts = new HashMap<String, String>();
		}
		contexts.put(key.getCode(), value);
	}

	public void addAllContexts(Map<String, String> contexts) {
		if (contexts != null) {
			if (this.contexts == null) {
				this.contexts = new HashMap<String, String>();
			}
			this.contexts.putAll(contexts);
		}
	}

	/**
	 * Return immutable List of histories to prevent external modification
	 * 
	 * @return
	 */
	public List<String> getHistories() {
		return Collections.unmodifiableList(histories);
	}

	public void addHistory(String history) {
		if (histories == null) {
			histories = new ArrayList<String>();
		}

		histories.add(history);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj instanceof BaseTuple) {
			BaseTuple tuple = (BaseTuple) obj;
			if (tuple.requestId.equals(this.requestId)) {
				return true;
			}
		}

		return false;
	}
}
