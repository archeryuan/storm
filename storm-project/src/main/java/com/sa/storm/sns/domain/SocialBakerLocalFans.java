package com.sa.storm.sns.domain;

public class SocialBakerLocalFans {
	private String fbPageId;
	private String country;
	private long noOfLocalFan;
	private float percentageOfFanBase;
	
	
	public String getFbPageId() {
		return fbPageId;
	}
	public void setFbPageId(String fbPageId) {
		this.fbPageId = fbPageId;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public long getNoOfLocalFan() {
		return noOfLocalFan;
	}
	public void setNoOfLocalFan(long noOfLocalFan) {
		this.noOfLocalFan = noOfLocalFan;
	}
	public float getPercentageOfFanBase() {
		return percentageOfFanBase;
	}
	public void setPercentageOfFanBase(float percentageOfFanBase) {
		this.percentageOfFanBase = percentageOfFanBase;
	}
	
	
	
	
	
}
