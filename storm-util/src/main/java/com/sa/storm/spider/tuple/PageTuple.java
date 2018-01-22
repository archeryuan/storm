package com.sa.storm.spider.tuple;

import java.util.List;
import java.util.Map;

import com.sa.common.domain.SpiderSeed;
import com.sa.storm.spider.domain.WebPageResponse;

public class PageTuple extends MultiQueueTuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2114127644813090324L;
	private WebPageResponse webPageResponse;
	private SpiderSeed spiderSeed;
	private Map<Long, List<String>> links;

	/**
	 * 
	 */
	public PageTuple() {
	}

	public PageTuple(MultiQueueTuple tuple, WebPageResponse webPageResponse, SpiderSeed spiderSeed) {
		super(tuple);
		this.webPageResponse = webPageResponse;
		this.spiderSeed = spiderSeed;
	}

	public PageTuple(MultiQueueTuple tuple, WebPageResponse webPageResponse, SpiderSeed spiderSeed, Map<Long, List<String>> links) {
		super(tuple);
		this.webPageResponse = webPageResponse;
		this.spiderSeed = spiderSeed;
		this.links = links;
	}

	public WebPageResponse getWebPageResponse() {
		return webPageResponse;
	}

	public void setWebPageResponse(WebPageResponse webPageResponse) {
		this.webPageResponse = webPageResponse;
	}

	public SpiderSeed getSpiderSeed() {
		return spiderSeed;
	}

	public void setSpiderSeed(SpiderSeed spiderSeed) {
		this.spiderSeed = spiderSeed;
	}

	public Map<Long, List<String>> getLinks() {
		return links;
	}

	public void setLinks(Map<Long, List<String>> links) {
		this.links = links;
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
		result = prime * result + ((links == null) ? 0 : links.hashCode());
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
		PageTuple other = (PageTuple) obj;
		if (links == null) {
			if (other.links != null)
				return false;
		} else if (!links.equals(other.links))
			return false;
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
		builder.append("PageTuple [");
		if (webPageResponse != null) {
			builder.append("webPageResponse=");
			builder.append(webPageResponse);
			builder.append(", ");
		}
		if (spiderSeed != null) {
			builder.append("spiderSeed=");
			builder.append(spiderSeed);
			builder.append(", ");
		}
		if (links != null) {
			builder.append("links=");
			builder.append(links);
		}
		builder.append("]");
		return builder.toString();
	}

}