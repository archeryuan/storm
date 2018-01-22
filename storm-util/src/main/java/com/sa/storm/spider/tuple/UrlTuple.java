/**
 * 
 */
package com.sa.storm.spider.tuple;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.sa.common.domain.SpiderRegular;
import com.sa.common.domain.SpiderSeed;

/**
 * @author lewis
 * 
 */
public class UrlTuple extends MultiQueueTuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6863759016524557378L;
	private SpiderSeed spiderSeed;
	private SpiderRegular spiderRegular;

	/**
	 * Deprecated, use createDate in BaseTuple instead
	 */
	@Deprecated
	private Long ts;

	/**
	 * 
	 */
	public UrlTuple() {
	}

	/**
	 * 
	 */
	public UrlTuple(MultiQueueTuple tuple) {
		super(tuple);
	}

	/**
	 * @param url
	 * @param domain
	 * @param spiderSeed
	 * @param regExId
	 * @param type
	 */
	public UrlTuple(BaseTuple tuple, String url, String domain, SpiderSeed spiderSeed, SpiderRegular spiderRegular) {
		super(tuple, url, domain);
		this.spiderSeed = spiderSeed;
		this.spiderRegular = spiderRegular;
	}

	/**
	 * @return the spiderSeed
	 */
	public SpiderSeed getSpiderSeed() {
		return spiderSeed;
	}

	/**
	 * @param spiderSeed
	 *            the spiderSeed to set
	 */
	public void setSpiderSeed(SpiderSeed spiderSeed) {
		this.spiderSeed = spiderSeed;
	}

	/**
	 * @return the spiderRegular
	 */
	public SpiderRegular getSpiderRegular() {
		return spiderRegular;
	}

	/**
	 * @param spiderRegular
	 *            the spiderRegular to set
	 */
	public void setSpiderRegular(SpiderRegular spiderRegular) {
		this.spiderRegular = spiderRegular;
	}

	@Deprecated
	public Long getTs() {
		if (getCreateDate() != null) {
			return getCreateDate();
		}
		return ts;
	}

	@Deprecated
	public void setTs(Long ts) {
		this.ts = ts;
		setCreateDate(ts);
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
