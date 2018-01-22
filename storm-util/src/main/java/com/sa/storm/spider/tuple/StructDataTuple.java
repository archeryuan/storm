package com.sa.storm.spider.tuple;

import com.sa.common.domain.SpiderSeed;
import com.sa.storm.spider.domain.DataResult;

public class StructDataTuple extends MultiQueueTuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5440632297960154661L;
	private DataResult data;
	private SpiderSeed spiderseeds;

	/**
	 * 
	 */
	public StructDataTuple() {
	}

	public StructDataTuple(MultiQueueTuple tuple, DataResult data, SpiderSeed spiderseeds) {
		super(tuple);
		this.data = data;
		this.spiderseeds = spiderseeds;
	}

	public DataResult getData() {
		return data;
	}

	public void setData(DataResult data) {
		this.data = data;
	}

	public SpiderSeed getSpiderseeds() {
		return spiderseeds;
	}

	public void setSpiderseeds(SpiderSeed spiderseeds) {
		this.spiderseeds = spiderseeds;
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
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + ((spiderseeds == null) ? 0 : spiderseeds.hashCode());
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
		StructDataTuple other = (StructDataTuple) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (spiderseeds == null) {
			if (other.spiderseeds != null)
				return false;
		} else if (!spiderseeds.equals(other.spiderseeds))
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
		builder.append("StructDataTuple [");
		if (data != null) {
			builder.append("data=");
			builder.append(data);
			builder.append(", ");
		}
		if (spiderseeds != null) {
			builder.append("spiderseeds=");
			builder.append(spiderseeds);
		}
		builder.append("]");
		return builder.toString();
	}

}