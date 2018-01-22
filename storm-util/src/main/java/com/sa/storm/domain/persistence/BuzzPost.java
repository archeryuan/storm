package com.sa.storm.domain.persistence;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.sa.common.json.JsonUtil;

public class BuzzPost extends BasePersistenceObject implements Serializable {

	private static final long serialVersionUID = -8746253765270268978L;

	protected static final Logger log = LoggerFactory.getLogger(BuzzPost.class);

	public static final String HBASE_TABLE = "db";

	public static final byte[] HBASE_INFO_COLUMN_FAMILY = Bytes.toBytes("i");
	@JsonIgnore
	protected String id;
	@JsonIgnore
	protected String parentId;// parentId

	@JsonIgnore
	protected String title;

	@JsonIgnore
	protected String content;

	@JsonIgnore
	protected String author;
	@JsonIgnore
	protected String country; // the country of the page/group/event
	@JsonIgnore
	protected String picUrl;

	@JsonProperty("i")
	protected int isComment;// is comment
	@JsonProperty("u")
	protected String userId;// userId
	@JsonProperty("l")
	protected Long likeCount;// like count
	@JsonProperty("c")
	protected Long commentCount;// comment count
	@JsonProperty("s")
	protected Long shareCount;// share count
	@JsonProperty("p")
	protected Date publishDate;// publishDate

	@JsonIgnore
	protected Long angryCount; // angry count
	@JsonIgnore
	protected Long hahaCount; // angry count
	@JsonIgnore
	protected Long loveCount; // angry count
	@JsonIgnore
	protected Long sadCount; // angry count
	@JsonIgnore
	protected Long thankfulCount; // angry count
	@JsonIgnore
	protected Long wowCount; // angry count

	public BuzzPost() {

	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getParentId() {
		return parentId;
	}

	public void setParentId(String parentId) {
		this.parentId = parentId;
	}

	public int getIsComment() {
		return isComment;
	}

	public void setIsComment(int isComment) {
		this.isComment = isComment;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getPicUrl() {
		return picUrl;
	}

	public void setPicUrl(String picUrl) {
		this.picUrl = picUrl;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Long getLikeCount() {
		return likeCount;
	}

	public void setLikeCount(Long likeCount) {
		this.likeCount = likeCount;
	}

	public Long getCommentCount() {
		return commentCount;
	}

	public void setCommentCount(Long commentCount) {
		this.commentCount = commentCount;
	}

	public Long getShareCount() {
		return shareCount;
	}

	public void setShareCount(Long shareCount) {
		this.shareCount = shareCount;
	}

	public Date getPublishDate() {
		return publishDate;
	}

	public void setPublishDate(Date publishDate) {
		this.publishDate = publishDate;
	}

	public Long getAngryCount() {
		return angryCount;
	}

	public void setAngryCount(Long angryCount) {
		this.angryCount = angryCount;
	}

	public Long getHahaCount() {
		return hahaCount;
	}

	public Long getLoveCount() {
		return loveCount;
	}

	public Long getSadCount() {
		return sadCount;
	}

	public Long getThankfulCount() {
		return thankfulCount;
	}

	public Long getWowCount() {
		return wowCount;
	}

	public void setHahaCount(Long hahaCount) {
		this.hahaCount = hahaCount;
	}

	public void setLoveCount(Long loveCount) {
		this.loveCount = loveCount;
	}

	public void setSadCount(Long sadCount) {
		this.sadCount = sadCount;
	}

	public void setThankfulCount(Long thankfulCount) {
		this.thankfulCount = thankfulCount;
	}

	public void setWowCount(Long wowCount) {
		this.wowCount = wowCount;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public Put toHBasePut() throws JsonGenerationException, JsonMappingException, IOException {
		Put put = new Put(Bytes.toBytes(parentId));
		String content = JsonUtil.getMapper().writeValueAsString(this);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, Bytes.toBytes(id), content);
		return put;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
