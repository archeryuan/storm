package com.sa.storm.domain;

import java.util.List;

import com.sa.storm.domain.tuple.BaseTuple;

public class Link extends BaseTuple{
	
	private static final long serialVersionUID = 1L;
	private String url;
	private String date;
	private String site;
	private String keyword;	
	private String title;
	private List<Link> moreLink;
	
	
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public Link() {
		super();
	}
	
	public Link (BaseTuple original) {
		super(original);
	}
	
	public String getSite() {
		return site;
	}
	public void setSite(String site) {
		this.site = site;
	}
	public String getKeyword() {
		return keyword;
	}
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public List<Link> getMoreLink() {
		return moreLink;
	}
	public void setMoreLink(List<Link> moreLink) {
		this.moreLink = moreLink;
	}
	
	
}
