/**  */
package com.sa.storm.spider.tuple;

import com.sa.common.domain.SpiderRegular;
import com.sa.common.domain.SpiderSeed;
import com.sa.storm.spider.domain.DataResult;

/** @author Kavis */
public class TargetTuple extends BaseTuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8964439845277808080L;
	private String url;
	private String domain;
	private SpiderSeed spiderSeed;
	private SpiderRegular spiderRegular;
	private DataResult dataResult;
	private Long ts;

	public TargetTuple() {
	}

	public TargetTuple(BaseTuple tuple, String url, String domain, SpiderSeed spiderSeed, SpiderRegular spiderRegular, DataResult dataResult) {
		super(tuple);
		this.url = url;
		this.domain = domain;
		this.spiderSeed = spiderSeed;
		this.spiderRegular = spiderRegular;
		this.dataResult = dataResult;
	}

	public TargetTuple(BaseTuple tuple) {
		super(tuple);
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public SpiderSeed getSpiderSeed() {
		return spiderSeed;
	}

	public void setSpiderSeed(SpiderSeed spiderSeed) {
		this.spiderSeed = spiderSeed;
	}

	public SpiderRegular getSpiderRegular() {
		return spiderRegular;
	}

	public void setSpiderRegular(SpiderRegular spiderRegular) {
		this.spiderRegular = spiderRegular;
	}

	public DataResult getDataResult() {
		return dataResult;
	}

	public void setDataResult(DataResult dataResult) {
		this.dataResult = dataResult;
	}

	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = 1;
		result = prime * result + ((domain == null) ? 0 : domain.hashCode());
		result = prime * result + ((spiderRegular == null) ? 0 : spiderRegular.hashCode());
		result = prime * result + ((spiderSeed == null) ? 0 : spiderSeed.hashCode());
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		result = prime * result + ((dataResult == null) ? 0 : dataResult.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		TargetTuple other = (TargetTuple) obj;
		if (domain == null) {
			if (other.domain != null)
				return false;
		} else if (!domain.equals(other.domain))
			return false;

		if (spiderRegular == null) {
			if (other.spiderRegular != null)
				return false;
		} else if (!spiderRegular.equals(other.spiderRegular))
			return false;

		if (spiderSeed == null) {
			if (other.spiderSeed != null)
				return false;
		} else if (!spiderSeed.equals(other.spiderSeed))
			return false;

		if (dataResult == null) {
			if (other.dataResult != null)
				return false;
		} else if (!dataResult.equals(other.dataResult))
			return false;

		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;

		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TargetTuple [url=");
		builder.append(url);
		builder.append(", domain=");
		builder.append(domain);
		builder.append(", spiderSeed=");
		builder.append(spiderSeed);
		builder.append(", spiderRegular=");
		builder.append(spiderRegular);
		builder.append(", dataResult=");
		builder.append(dataResult);
		builder.append("]");
		return builder.toString();
	}
}