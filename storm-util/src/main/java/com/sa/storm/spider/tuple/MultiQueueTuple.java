/**
 * 
 */
package com.sa.storm.spider.tuple;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * @author lewis
 * 
 */
public abstract class MultiQueueTuple extends BaseTuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = -54005343798566398L;
	private String url;
	private String domain;

	public MultiQueueTuple() {
	}

	public MultiQueueTuple(MultiQueueTuple tuple) {
		super(tuple);
		this.url = tuple.getUrl();
		this.domain = tuple.getDomain();
	}

	/**
	 * @param taskType
	 * @param priority
	 */
	public MultiQueueTuple(int taskType, int priority, String url, String domain) {
		super(taskType, priority);
		this.url = url;
		this.domain = domain;
	}

	/**
	 * @param url
	 * @param domain
	 */
	public MultiQueueTuple(BaseTuple tuple, String url, String domain) {
		super(tuple);
		this.url = url;
		this.domain = domain;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @return the domain
	 */
	public String getDomain() {
		return domain;
	}

	/**
	 * @param url
	 *            the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @param domain
	 *            the domain to set
	 */
	public void setDomain(String domain) {
		this.domain = domain;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((domain == null) ? 0 : domain.hashCode());
		result = prime * result + ((url == null) ? 0 : url.hashCode());
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
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		MultiQueueTuple other = (MultiQueueTuple) obj;
		if (domain == null) {
			if (other.domain != null)
				return false;
		} else if (!domain.equals(other.domain))
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}

	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}
