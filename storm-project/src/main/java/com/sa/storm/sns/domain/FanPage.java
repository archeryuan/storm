/**
 *Copyright(C) ©2012 SocialAnalytics. All rights reserved.
 *
 */
package com.sa.storm.sns.domain;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @description Fan page object.
 * @author Luke
 * @created date 2014-12-9
 * @modification history<BR>
 *               No. Date Modified By <B>Why & What</B> is modified
 * 
 * @see
 */
public class FanPage implements Serializable {

	private static final long serialVersionUID = -5547248074332918023L;

	private String pageId;

	private String pageName;

	private String category;

	private String pageUrl;

	public FanPage() {
		super();
	}

	@JsonProperty("id")
	public String getPageId() {
		return pageId;
	}

	@JsonProperty("id")
	public void setPageId(String pageId) {
		this.pageId = pageId;
	}

	@JsonProperty("n")
	public String getPageName() {
		return pageName;
	}

	@JsonProperty("n")
	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	@JsonProperty("c")
	public String getCategory() {
		return category;
	}

	@JsonProperty("c")
	public void setCategory(String category) {
		this.category = category;
	}

	@JsonProperty("url")
	public String getPageUrl() {
		return pageUrl;
	}

	@JsonProperty("url")
	public void setPageUrl(String pageUrl) {
		this.pageUrl = pageUrl;
	}

}
