package com.sa.storm.domain.tuple;

import com.sa.storm.domain.persistence.CAPost;

public class CAPostResult extends BaseTuple {

	private static final long serialVersionUID = -6939568190900547098L;

	private CAPost post;

	public CAPostResult() {

	}

	public CAPostResult(BaseTuple original) {
		super(original);
	}

	public CAPost getPost() {
		return post;
	}

	public void setPost(CAPost post) {
		this.post = post;
	}

}
