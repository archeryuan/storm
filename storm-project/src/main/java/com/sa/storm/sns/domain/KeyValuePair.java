/**
 *Copyright(C) ©2012 SocialAnalytics. All rights reserved.
 *
 */
package com.sa.storm.sns.domain;

/**
 * @description
 * Key Value pair.
 * @author                            Luke
 * @created date                      2014-12-10 
 * @modification history<BR>
 * No.        Date          Modified By             <B>Why & What</B> is modified  
 *
 * @see                               
 */
public class KeyValuePair {
	
	private String key;	
	
	private long value;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("KeyValuePair [key=").append(key).append(", value=").append(value).append("]");
		return builder.toString();
	}
	

}
