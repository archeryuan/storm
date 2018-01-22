package com.sa.storm.domain.persistence;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserInfoForExtUsage extends BasePersistenceObject implements Serializable {
	private static final long serialVersionUID = 2L;

	private String userId;
	private String userScreenName;
	private String avatarUrl;
	private int noOfFriend;
	private String locationFrom;
	private String liveIn;
	private String gender;
	private String relationship;

	public UserInfoForExtUsage() {

	}

	public UserInfoForExtUsage(String userId) {
		this.userId = userId;
	}

	public UserInfoForExtUsage(String userId, String userScreenName, String avatarUrl) {
		this.userId = userId;
		this.userScreenName = userScreenName;
		this.avatarUrl = avatarUrl;

	}

	@JsonProperty("userId")
	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	@JsonProperty("gender")
	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	@JsonProperty("relationship")
	public String getRelationship() {
		return relationship;
	}

	public void setRelationship(String relationship) {
		this.relationship = relationship;
	}

	@JsonProperty("userScreenName")
	public String getUserScreenName() {
		return userScreenName;
	}

	public void setUserScreenName(String userScreenName) {
		this.userScreenName = userScreenName;
	}

	@JsonProperty("avatarUrl")
	public String getAvatarUrl() {
		return avatarUrl;
	}

	public void setAvatarUrl(String avatarUrl) {
		this.avatarUrl = avatarUrl;
	}

	@JsonProperty("noOfFriend")
	public int getNoOfFriend() {
		return noOfFriend;
	}

	public void setNoOfFriend(int noOfFriend) {
		this.noOfFriend = noOfFriend;
	}

	@JsonProperty("locationFrom")
	public String getLocationFrom() {
		return locationFrom;
	}

	public void setLocationFrom(String locationFrom) {
		this.locationFrom = locationFrom;
	}

	@JsonProperty("liveIn")
	public String getLiveIn() {
		return liveIn;
	}

	public void setLiveIn(String liveIn) {
		this.liveIn = liveIn;
	}

}
