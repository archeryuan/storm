/**
 *Copyright(C) ©2012 SocialAnalytics. All rights reserved.
 *
 */
package com.sa.storm.sns.domain;

import com.sa.storm.ca.domain.ImgInfo;


/**
 * @description
 * Top like page object.
 * @author Luke
 * @created date 2014-12-11
 * @modification history<BR>
 *               No. Date Modified By <B>Why & What</B> is modified
 * 
 * @see
 */
public class TopLikePage {
	private String pageId;

	private String pageName;

	private String category;
	
	private String pageUrl;

	private long interestFans;

	private ImgInfo imgInfo;		
	
	
	public String getPageId() {
		return pageId;
	}

	public void setPageId(String pageId) {
		this.pageId = pageId;
	}

	public String getPageName() {
		return pageName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}
	
	public String getPageUrl() {
		return pageUrl;
	}

	public void setPageUrl(String pageUrl) {
		this.pageUrl = pageUrl;
	}

	public long getInterestFans() {
		return interestFans;
	}

	public void setInterestFans(long interestFans) {
		this.interestFans = interestFans;
	}
	
	public ImgInfo getImgInfo() {
		return imgInfo;
	}

	public void setImgInfo(ImgInfo imgInfo) {
		this.imgInfo = imgInfo;
	}

	public void addInterestFans(long count) {
		this.interestFans += count;
	}
	
	public void decrInterestFans(long count) {
		this.interestFans -= count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pageId == null) ? 0 : pageId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TopLikePage other = (TopLikePage) obj;
		if (pageId == null) {
			if (other.pageId != null)
				return false;
		} else if (!pageId.equals(other.pageId))
			return false;
		return true;
	}


}
