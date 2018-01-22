package com.sa.storm.sns.domain;

import com.sa.storm.domain.tuple.BaseTuple;

public class PageFansResult extends BaseTuple {

	private static final long serialVersionUID = -2492739701702804580L;

	private Fans fans;

	public Fans getFans() {
		return fans;
	}

	public void setFan(Fans fans) {
		this.fans = fans;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PageFansResult [fans=").append(fans).append("]");
		return builder.toString();
	}
	
	

}
