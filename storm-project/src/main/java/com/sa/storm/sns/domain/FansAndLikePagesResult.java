package com.sa.storm.sns.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sa.storm.domain.tuple.BaseTuple;

public class FansAndLikePagesResult extends BaseTuple {

	private static final long serialVersionUID = 181760206839686420L;

	private Fans fans;

	private List<FanPage> likePages;

	public FansAndLikePagesResult() {
		super();
	}

	public FansAndLikePagesResult(String requestId, BaseTuple original) {
		super(requestId, original);
	}

	public FansAndLikePagesResult(String requestId, String parentRequestId, Integer type, String sourceType, Integer priority,
			Map<String, String> params) {
		super(requestId, parentRequestId, type, sourceType, priority, params);
	}

	public FansAndLikePagesResult(String requestId, String parentRequestId, Integer type, String sourceType, Map<String, String> params) {
		super(requestId, parentRequestId, type, sourceType, params);
	}

	public FansAndLikePagesResult(BaseTuple original) {
		super(original);
	}

	public FansAndLikePagesResult(String requestId, Integer type, String sourceType, Map<String, String> params) {
		super(requestId, type, sourceType, params);
	}

	@JsonProperty("f")
	public Fans getFans() {
		return fans;
	}

	@JsonProperty("f")
	public void setFans(Fans fans) {
		this.fans = fans;
	}

	@JsonProperty("p")
	public List<FanPage> getLikePages() {
		return likePages;
	}

	@JsonProperty("p")
	public void setLikePages(List<FanPage> likePages) {
		this.likePages = likePages;
	}

	public void add(FanPage likePage) {
		if (null == this.likePages)
			this.likePages = new ArrayList<FanPage>();

		this.likePages.add(likePage);
	}


}
