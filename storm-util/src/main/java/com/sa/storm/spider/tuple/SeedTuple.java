package com.sa.storm.spider.tuple;

import com.sa.common.domain.SpiderSeed;
import com.sa.redis.definition.RedisDefinition.Priority;

public class SeedTuple extends MultiQueueTuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4005093000529356198L;

	private SpiderSeed spiderSeed;

	/**
	 * Deprecated, use createDate in BaseTuple instead
	 */
	@Deprecated
	private Long ts;

	public SeedTuple() {
	}

	public SeedTuple(MultiQueueTuple tuple, SpiderSeed seed) {
		super(tuple);
		this.spiderSeed = seed;
	}

	/**
	 * @param taskType
	 * @param spiderSeed
	 */
	public SeedTuple(int taskType, String url, String domain, SpiderSeed spiderSeed) {
		this(taskType, Priority.NORMAL.getCode(), url, domain, spiderSeed);
	}

	/**
	 * @param taskType
	 * @param priority
	 * @param spiderSeed
	 */
	public SeedTuple(int taskType, int priority, String url, String domain, SpiderSeed spiderSeed) {
		super(taskType, priority, url, domain);
		this.spiderSeed = spiderSeed;
	}

	public SpiderSeed getSpiderSeed() {
		return spiderSeed;
	}

	public void setSpiderSeed(SpiderSeed spiderseeds) {
		this.spiderSeed = spiderseeds;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((spiderSeed == null) ? 0 : spiderSeed.hashCode());
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
		SeedTuple other = (SeedTuple) obj;
		if (spiderSeed == null) {
			if (other.spiderSeed != null)
				return false;
		} else if (!spiderSeed.equals(other.spiderSeed))
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
		StringBuilder builder = new StringBuilder();
		builder.append("SeedTuple [");
		if (spiderSeed != null) {
			builder.append("spiderSeed=");
			builder.append(spiderSeed);
		}
		builder.append("]");
		return builder.toString();
	}

}