package com.sa.storm.domain.tuple;

import com.sa.storm.domain.persistence.User;

public class UserResult extends BaseTuple {

	private static final long serialVersionUID = 1L;

	private User user;

	public UserResult() {
		super();
	}

	public UserResult(BaseTuple original) {
		super(original);
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}
}
