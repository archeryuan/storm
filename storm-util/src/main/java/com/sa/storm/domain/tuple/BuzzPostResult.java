package com.sa.storm.domain.tuple;

import com.sa.storm.domain.persistence.BuzzPost;

public class BuzzPostResult extends BaseTuple {

	private static final long serialVersionUID = -6939568190900547098L;

	private BuzzPost post;

	public BuzzPostResult() {

	}

	public BuzzPostResult(BaseTuple original) {
		super(original);
	}

	public BuzzPost getPost() {
		return post;
	}

	public void setPost(BuzzPost post) {
		this.post = post;
	}

}
