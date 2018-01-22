/**
 * 
 */
package com.sa.storm.spider.tuple;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * @author lewis
 * 
 */
public class SpiderJobTuple extends MultiQueueTuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5374673162730916352L;

	/**
	 * Field for object detection while in json string form
	 */
	private String version = "*#01";

	private long seedId;

	/**
	 * 
	 */
	public SpiderJobTuple() {
	}

	/**
	 * @param tuple
	 * @param url
	 * @param domain
	 */
	public SpiderJobTuple(BaseTuple tuple, String url, String domain) {
		super(tuple, url, domain);
	}

	/**
	 * @param taskType
	 * @param priority
	 * @param url
	 * @param domain
	 */
	public SpiderJobTuple(int taskType, int priority, String url, String domain, long seedId) {
		super(taskType, priority, url, domain);
		this.seedId = seedId;
	}

	public SpiderJobTuple(SpiderJobTuple tuple) {
		super(tuple);
		this.seedId = tuple.getSeedId();
		this.version = tuple.getVersion();
	}

	/**
	 * @param tuple
	 */
	public SpiderJobTuple(MultiQueueTuple tuple) {
		super(tuple);

		if (tuple != null && tuple instanceof SpiderJobTuple) {
			SpiderJobTuple t = (SpiderJobTuple) tuple;
			this.seedId = t.getSeedId();
			this.version = t.getVersion();
		}
	}

	/**
	 * @return the seedId
	 */
	public long getSeedId() {
		return seedId;
	}

	/**
	 * @param seedId
	 *            the seedId to set
	 */
	public void setSeedId(long seedId) {
		this.seedId = seedId;
	}

	/**
	 * @return the version
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * @param version
	 *            the version to set
	 */
	public void setVersion(String version) {
		this.version = version;
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
		result = prime * result + (int) (seedId ^ (seedId >>> 32));
		result = prime * result + ((version == null) ? 0 : version.hashCode());
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
		SpiderJobTuple other = (SpiderJobTuple) obj;
		if (seedId != other.seedId)
			return false;
		if (version == null) {
			if (other.version != null)
				return false;
		} else if (!version.equals(other.version))
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
