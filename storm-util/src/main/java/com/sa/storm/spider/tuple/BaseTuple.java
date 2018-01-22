package com.sa.storm.spider.tuple;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sa.storm.definition.TupleDefinition.Context;

public abstract class BaseTuple implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6372104732740110925L;

	/**
	 * Refer to com.sa.crawler.definition.TaskType
	 */
	private int taskType;

	/**
	 * Refer to RedisDefinition.Priority
	 */
	private int priority;

	/**
	 * Tuple creation date<BR>
	 * Field is null-able for compatibility issue.
	 */
	private Long createDate;

	private Map<String, String> params;
	private Map<String, Object> contexts;

	/**
	 * 
	 */
	public BaseTuple() {
		createDate = System.currentTimeMillis();
	}

	/**
	 * @param taskType
	 * @param priority
	 */
	public BaseTuple(int taskType, int priority) {
		this.taskType = taskType;
		this.priority = priority;
		createDate = System.currentTimeMillis();
	}

	/**
	 * @param taskType
	 */
	public BaseTuple(BaseTuple tuple) {
		this.taskType = tuple.getTaskType();
		this.priority = tuple.getPriority();
		this.params = tuple.getParams();
		this.contexts = tuple.getContexts();
		this.createDate = tuple.getCreateDate();
	}

	/**
	 * @return the taskType
	 */
	public int getTaskType() {
		return taskType;
	}

	/**
	 * @return the priority
	 */
	public int getPriority() {
		return priority;
	}

	/**
	 * Task Type<BR>
	 * Refer to definition com.sa.crawler.definition.TaskType
	 * 
	 * @param taskType
	 *            the taskType to set
	 */
	public void setTaskType(int taskType) {
		this.taskType = taskType;
	}

	/**
	 * Refer to RedisDefinition.Priority
	 * 
	 * @param priority
	 *            the priority to set
	 */
	public void setPriority(int priority) {
		this.priority = priority;
	}

	public void addParam(String key, String object) {
		getParams().put(key, object);
	}

	public String getParam(String key) {
		String obj = getParams().get(key);
		if (obj != null) {
			return obj;
		}
		return null;
	}

	/**
	 * @return the params
	 */
	public Map<String, String> getParams() {
		if (params == null) {
			params = new HashMap<String, String>();
		}
		return params;
	}

	/**
	 * @param params
	 *            the params to set
	 */
	public void setParams(Map<String, String> params) {
		this.params = params;
	}

	public void setContext(String key, Object value) {
		if (contexts == null) {
			contexts = new HashMap<String, Object>();
		}
		contexts.put(key, value);
	}

	public Integer getContextInt(String key) {
		if (getContexts() == null)
			return null;
		return (Integer) getContexts().get(key);
	}

	public String getContextStr(String key) {
		if (getContexts() == null)
			return null;
		return (String) getContexts().get(key);
	}

	/**
	 * @return the contexts
	 */
	public Map<String, Object> getContexts() {
		return contexts;
	}

	/**
	 * @param contexts
	 *            the contexts to set
	 */
	public void setContexts(Map<String, Object> contexts) {
		this.contexts = contexts;
	}

	public void setRetryCount(Integer count) {
		if (count != null)
			setContext(Context.RETRY_COUNT.getCode(), count);
	}

	public Integer getRetryCount() {
		return getContextInt(Context.RETRY_COUNT.getCode());
	}

	/**
	 * @return the createDate
	 */
	@JsonProperty("cDate")
	public Long getCreateDate() {
		return createDate;
	}

	/**
	 * @param createDate
	 *            the createDate to set
	 */
	@JsonProperty("cDate")
	public void setCreateDate(Long createDate) {
		this.createDate = createDate;
	}

	/**
	 * Add retry count by 1
	 */
	@JsonIgnore
	public void addRetryCount() {
		Integer count = getRetryCount();
		if (count == null) {
			count = 1;
		} else {
			count++;
		}
		setContext(Context.RETRY_COUNT.getCode(), count);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((createDate == null) ? 0 : createDate.hashCode());
		result = prime * result + ((params == null) ? 0 : params.hashCode());
		result = prime * result + priority;
		result = prime * result + taskType;
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BaseTuple other = (BaseTuple) obj;
		if (createDate == null) {
			if (other.createDate != null)
				return false;
		} else if (!createDate.equals(other.createDate))
			return false;
		if (params == null) {
			if (other.params != null)
				return false;
		} else if (!params.equals(other.params))
			return false;
		if (priority != other.priority)
			return false;
		if (taskType != other.taskType)
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
