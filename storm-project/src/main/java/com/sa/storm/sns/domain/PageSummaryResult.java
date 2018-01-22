/**
 *Copyright(C) ©2012 SocialAnalytics. All rights reserved.
 *
 */
package com.sa.storm.sns.domain;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @description
 * 
 * @author Luke
 * @created date 2015-2-4
 * @modification history<BR>
 *               No. Date Modified By <B>Why & What</B> is modified
 * 
 * @see
 */
public class PageSummaryResult {

	private long fansTotalCount;

	private long fansNoHandledCount;
	
	private Map<String, Long> category2Count;
	
	private Map<String, Long> pageId2Count;
	
	public PageSummaryResult() {
		this.fansTotalCount = 0l;
		this.fansNoHandledCount = 0l;
		this.category2Count = new HashMap<String, Long>();
		this.pageId2Count = new HashMap<String, Long>();

	}

	@JsonProperty("tc")
	public long getFansTotalCount() {
		return fansTotalCount;
	}

	@JsonProperty("tc")
	public void setFansTotalCount(long fansTotalCount) {
		this.fansTotalCount = fansTotalCount;
	}

	@JsonProperty("nc")
	public long getFansNoHandledCount() {
		return fansNoHandledCount;
	}

	@JsonProperty("nc")
	public void setFansNoHandledCount(long fansNoHandledCount) {
		this.fansNoHandledCount = fansNoHandledCount;
	}

	@JsonProperty("cc")
	public Map<String, Long> getCategory2Count() {
		return category2Count;
	}

	@JsonProperty("cc")
	public void setCategory2Count(Map<String, Long> category2Count) {
		this.category2Count = category2Count;
	}

	@JsonProperty("pc")
	public Map<String, Long> getPageId2Count() {
		return pageId2Count;
	}

	@JsonProperty("pc")
	public void setPageId2Count(Map<String, Long> pageId2Count) {
		this.pageId2Count = pageId2Count;
	}

	public void decrFansTotalCount(long count) {
		this.fansTotalCount -= count;
	}

	public void decrFansNoHandledCount(long count) {
		this.fansNoHandledCount -= count;
	}

	public void addFansTotalCount(long count) {
		this.fansTotalCount += count;
	}
	
	public void addFansNoHandledCount(long count) {
		this.fansNoHandledCount += count;
	}
	
	public void addCategoryCount(String category, long count) {
		if (StringUtils.isEmpty(category))
			return;
		
		Long totalCount = category2Count.get(category);
		if (null == totalCount)
			totalCount = 0l;
		totalCount += count;
		category2Count.put(category, totalCount);
		
	}
	
	public void addPageCount(String pageId, long count) {
		if (StringUtils.isEmpty(pageId))
			return;
		
		Long totalCount = pageId2Count.get(pageId);
		if (null == totalCount)
			totalCount = 0l;
		totalCount += count;
		pageId2Count.put(pageId, totalCount);
	}
	
}
