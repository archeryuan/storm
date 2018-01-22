/**
 *Copyright(C) ©2012 SocialAnalytics. All rights reserved.
 *
 */
package com.sa.storm.sns.domain;

import java.io.Serializable;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

/**
 * @description
 * Page fans object.
 * @author                            Luke
 * @created date                      2014-12-9 
 * @modification history<BR>
 * No.        Date          Modified By             <B>Why & What</B> is modified  
 *
 * @see                               
 */
public class Fans implements Serializable{
	
	private static final long serialVersionUID = 1080285125743630723L;

	private String pageId;
	
	private String userId;	

	public String getPageId() {
		return pageId;
	}

	public void setPageId(String pageId) {
		this.pageId = pageId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
	
	
	

}
