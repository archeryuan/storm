/**
 * 
 */
package com.sa.storm.spider.tuple;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.StringUtils;

/**
 * @author lewis
 * 
 */
public class OtherPageTuple extends BaseTuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7245026008920592063L;
	private List<String> urls;
	private String rId;
	private long seedId;
	private String keyword;
	private long regularId;

	/**
	 * 
	 */
	public OtherPageTuple() {
	}

	/**
	 * @param tuple
	 */
	public OtherPageTuple(BaseTuple tuple) {
		super(tuple);
	}

	/**
	 * @param taskType
	 * @param priority
	 */
	public OtherPageTuple(int taskType, int priority) {
		super(taskType, priority);
	}

	public void addUrl(String url) {
		if (urls == null) {
			urls = new ArrayList<String>();
		}
		if (!StringUtils.isBlank(url))
			urls.add(url);
	}

	/**
	 * @return the urls
	 */
	public List<String> getUrls() {
		return urls;
	}

	/**
	 * @param urls
	 *            the urls to set
	 */
	public void setUrls(List<String> urls) {
		this.urls = urls;
	}

	/**
	 * @return the rId
	 */
	public String getrId() {
		return rId;
	}

	/**
	 * @param rId
	 *            the rId to set
	 */
	public void setrId(String rId) {
		this.rId = rId;
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
	 * @return the keyword
	 */
	public String getKeyword() {
		return keyword;
	}

	/**
	 * @param keyword
	 *            the keyword to set
	 */
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}

	/**
	 * @return the regularId
	 */
	public long getRegularId() {
		return regularId;
	}

	/**
	 * @param regularId
	 *            the regularId to set
	 */
	public void setRegularId(long regularId) {
		this.regularId = regularId;
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
		result = prime * result + ((keyword == null) ? 0 : keyword.hashCode());
		result = prime * result + ((rId == null) ? 0 : rId.hashCode());
		result = prime * result + (int) (regularId ^ (regularId >>> 32));
		result = prime * result + (int) (seedId ^ (seedId >>> 32));
		result = prime * result + ((urls == null) ? 0 : urls.hashCode());
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
		OtherPageTuple other = (OtherPageTuple) obj;
		if (keyword == null) {
			if (other.keyword != null)
				return false;
		} else if (!keyword.equals(other.keyword))
			return false;
		if (rId == null) {
			if (other.rId != null)
				return false;
		} else if (!rId.equals(other.rId))
			return false;
		if (regularId != other.regularId)
			return false;
		if (seedId != other.seedId)
			return false;
		if (urls == null) {
			if (other.urls != null)
				return false;
		} else if (!urls.equals(other.urls))
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
