package com.sa.storm.domain.persistence;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import com.sa.storm.domain.persistence.BasePersistenceObject;

public class WeiboSocialMediaPost extends BasePersistenceObject implements Serializable {

	private static final long serialVersionUID = -4734388695809693960L;

	protected String id;

	protected Date publish_date;

	protected Map<String, Object> likes;

	protected Map<String, Object> shares;

	protected user user;

	protected Map<String, Object> comments;

	protected String images;

	protected Map<String, Object> videos;

	protected String name;

	protected String link;

	protected String type;

	protected String status_type;

	protected String caption;

	protected String description;

	protected String message;

	protected WeiboSocialMediaPost retweet;

	protected List<String> links;

	protected List<String> keywords;

	public WeiboSocialMediaPost() {

	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<String, Object> getLikes() {
		return likes;
	}

	public void setLikes(Map<String, Object> likes) {
		this.likes = likes;
	}

	public Map<String, Object> getShares() {
		return shares;
	}

	public void setShares(Map<String, Object> shares) {
		this.shares = shares;
	}

	public Date getPublish_date() {
		return publish_date;
	}

	public void setPublish_date(Date publish_date) {
		this.publish_date = publish_date;
	}

	public Map<String, Object> getComments() {
		return comments;
	}

	public void setComments(Map<String, Object> comments) {
		this.comments = comments;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getImages() {
		return images;
	}

	public void setImages(String images) {
		this.images = images;
	}

	public Map<String, Object> getVideos() {
		return videos;
	}

	public void setVideos(Map<String, Object> videos) {
		this.videos = videos;
	}

	public String getCaption() {
		return caption;
	}

	public void setCaption(String caption) {
		this.caption = caption;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public String getStatus_type() {
		return status_type;
	}

	public void setStatus_type(String status_type) {
		this.status_type = status_type;
	}

	public user getUser() {
		return user;
	}

	public void setUser(user user) {
		this.user = user;
	}

	public String getMessage() {
		return message;
	}

	public void SetMessage(String message) {
		this.message = message;
	}

	public WeiboSocialMediaPost getRetweet() {
		return retweet;
	}

	public void setRetweet(WeiboSocialMediaPost retweet) {
		this.retweet = retweet;
	}

	public List<String> getLinks() {
		return links;
	}

	public void setLinks(List<String> links) {
		this.links = links;
	}

	public List<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}

	public static class user {
		public String uid;
		public String uname;
		public String uPageLink;
		public String profile_picture;

		public String getuid() {
			return uid;
		}

		public void setuid(String uid) {
			this.uid = uid;
		}

		public String getuname() {
			return uname;
		}

		public void setuname(String uname) {
			this.uname = uname;
		}

		public String getuPageLink() {
			return uPageLink;
		}

		public void setuPageLink(String uPageLink) {
			this.uPageLink = uPageLink;
		}

		public String getProfile_picture() {
			return profile_picture;
		}

		public void setProfile_picture(String profile_picture) {
			this.profile_picture = profile_picture;
		}

	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
