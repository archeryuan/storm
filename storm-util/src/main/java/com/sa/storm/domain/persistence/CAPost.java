package com.sa.storm.domain.persistence;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.common.definition.SourceType;

public class CAPost extends BasePersistenceObject implements Serializable {

	protected static final long serialVersionUID = 5575052524207453965L;

	protected static final Logger log = LoggerFactory.getLogger(CAPost.class);

	public static final String HBASE_TABLE = "capost";
	public static final String HBASE_TOP_CONTENT_TABLE = "tcapost";
	public static final String HBASE_POST_TABLE = "ch";
	public static final String HBASE_DETAIL_POST_TABLE = "dp";

	public static final byte[] HBASE_INFO_COLUMN_FAMILY = Bytes.toBytes("i");

	public static final byte[] HBASE_NUM_LIKE = Bytes.toBytes("numLike");

	public static final byte[] HBASE_NUM_DISLIKE = Bytes.toBytes("numDisLike");

	public static final byte[] HBASE_NUM_LIKE_DATE = Bytes.toBytes("numLikeDate");

	public static final byte[] HBASE_NUM_SHARE = Bytes.toBytes("numShare");

	public static final byte[] HBASE_CREATE_DATE = Bytes.toBytes("createDate");

	public static final byte[] HBASE_VIEW_COUNT = Bytes.toBytes("viewCount");

	public static final byte[] HBASE_COMMENT_COUNT = Bytes.toBytes("commentCount");

	public static final byte[] HBASE_PUBLISH_DATE = Bytes.toBytes("publishDate");

	public static final byte[] HBASE_REPLY_COUNT = Bytes.toBytes("replyCount");

	public static final byte[] HBASE_AUTHOR_ID = Bytes.toBytes("authorID");

	public static final byte[] HBASE_TITLE = Bytes.toBytes("title");

	public static final byte[] HBASE_URL = Bytes.toBytes("url");

	public static final byte[] HBASE_POST_ID = Bytes.toBytes("postId");

	public static final byte[] HBASE_IMAGE_URL = Bytes.toBytes("imgUrl");

	public static final byte[] HBASE_NAME = Bytes.toBytes("name");

	public static final byte[] HBASE_VIDEO_URL = Bytes.toBytes("videoUrl");

	public static final byte[] HBASE_POST_CONTENT = Bytes.toBytes("postContent");

	public static final byte[] HBASE_AVATAR_URL = Bytes.toBytes("aUrl");

	public static final byte[] HBASE_USER_NAME = Bytes.toBytes("uName");

	public String rowKey;

	protected String id;

	protected String url;

	protected SourceType sourceType;

	protected Date publishDate;

	protected Date createDate;

	protected Long numLike;

	protected Long numEngage; // Likes + Comments + Shares

	protected Long upEngage; // New Engagement

	protected Long upLike; // New Likes

	protected Long upComment; // New Comments

	protected Long upShare; // New Shares

	protected Date numLikeDate;

	protected Long numShare;

	protected String postID;

	protected String pageID; // Corresponding Page

	protected boolean isComment;

	protected String uId;

	protected String uName;

	protected String aUrl;

	protected Date numShareDate;

	protected Long numDisLike;

	protected Date numDisLikeDate;

	protected Long viewCount;

	protected Long replyCount;

	protected Long commentCount;

	protected String imgUrl;

	protected String name;

	protected String videoUrl;

	protected String postContent;

	protected CAPost commentObj;

	protected Map<String, Long> topContent;

	protected String title;

	protected String showDate;

	public CAPost() {

	}

	public CAPost(Result result) {
		if (result == null) {
			return;
		}

		final String rowKey = Bytes.toString(result.getRow());
		final String sourceTypeStr = rowKey.split("#")[0];
		final String id = rowKey.split("#")[4];
		Long numLike = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE);
		final Date numLikeDate = getDateFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE_DATE);
		final Long numDisLike = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_DISLIKE);
		Long numShare = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_SHARE);
		final Date createDate = getDateFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_CREATE_DATE);
		Long commentCount = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_COMMENT_COUNT);
		final Long replyCount = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_REPLY_COUNT);
		final Long viewCount = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_VIEW_COUNT);
		final String title = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_TITLE);
		final String url = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_URL);
		final Date publishDate = getDateFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_PUBLISH_DATE);
		final String postID = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_POST_ID);
		final String uId = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_AUTHOR_ID);
		final String imgUrl = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_IMAGE_URL);
		final String postContent = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_POST_CONTENT);
		final String aUrl = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_AVATAR_URL);

		if (null == numLike)
			numLike = 0l;

		if (null == commentCount)
			commentCount = 0l;

		if (null == numShare)
			numShare = 0l;

		this.rowKey = rowKey;
		this.id = id;
		this.sourceType = SourceType.getSourceType(sourceTypeStr);
		this.numLike = numLike;
		this.numDisLike = numDisLike;
		this.numLikeDate = numLikeDate;
		this.numShare = numShare;
		this.numEngage = numLike + commentCount + numShare;
		this.createDate = createDate;
		this.replyCount = replyCount;
		this.commentCount = commentCount;
		this.viewCount = viewCount;
		this.title = title;
		this.url = url;
		this.postID = postID;
		this.publishDate = publishDate;
		this.uId = uId;
		this.imgUrl = imgUrl;
		this.postContent = postContent;
		this.aUrl = aUrl;

		// 08#98499235621#0#15-01-26#98499235621_10152951619485622_10152951621335622
		this.isComment = "0".equalsIgnoreCase(rowKey.split("#")[2]) ? true : false;

		if (this.isComment) {
			String refPostId = this.getPostID();
			if (StringUtils.isEmpty(refPostId)) {
				String[] fields = StringUtils.split(id, "_");
				if (null != fields) {
					int len = fields.length;
					if (len >= 3) {
						String f0 = fields[0];
						String f1 = fields[1];
						if (!StringUtils.isEmpty(f1) && !StringUtils.isEmpty(f1))
							refPostId = f0 + "_" + f1;

						if (len > 3) {
							String f2 = fields[2];
							if (!StringUtils.isEmpty(f2))
								refPostId = refPostId + "_" + f2;
						}

					}
				}
			}

			if (!StringUtils.isEmpty(refPostId))
				this.setPostID(refPostId);

		}

		// log.info("numEnage of Post " + this.id + " is " + this.numEngage + ", publish date:" + this.publishDate);

	}

	public CAPost(Result result, int type) {
		if (result == null) {
			return;
		}

		Long numLike = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE);
		Long numDisLike = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_DISLIKE);
		Long numShare = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_SHARE);
		Long commentCount = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_COMMENT_COUNT);
		final Date numLikeDate = getDateFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE_DATE);
		final Date createDate = getDateFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_CREATE_DATE);
		final Long replyCount = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_REPLY_COUNT);
		final Long viewCount = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_VIEW_COUNT);
		final String title = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_TITLE);
		final String url = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_URL);
		final Date publishDate = getDateFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_PUBLISH_DATE);
		final String postID = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_POST_ID);
		final String uId = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_AUTHOR_ID);
		final String imgUrl = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_IMAGE_URL);
		final String postContent = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_POST_CONTENT);
		final String aUrl = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_AVATAR_URL);

		String rowKey;
		String sourceTypeStr;
		String id;

		// If type value is 0 means it design for top content post detail.
		if (0 == type) {
			rowKey = Bytes.toString(result.getRow());
			sourceTypeStr = rowKey.split("#")[0];
			id = rowKey.split("#")[1];
		} else {
			rowKey = Bytes.toString(result.getRow());
			sourceTypeStr = rowKey.split("#")[0];
			id = rowKey.split("#")[4];

			// 08#98499235621#0#15-01-26#98499235621_10152951619485622_10152951621335622
			this.isComment = "0".equalsIgnoreCase(rowKey.split("#")[2]) ? true : false;

			if (this.isComment) {
				String refPostId = this.getPostID();
				if (StringUtils.isEmpty(refPostId)) {
					String[] fields = StringUtils.split(id, "_");
					if (null != fields) {
						int len = fields.length;
						if (len >= 3) {
							String f0 = fields[0];
							String f1 = fields[1];
							if (!StringUtils.isEmpty(f1) && !StringUtils.isEmpty(f1))
								refPostId = f0 + "_" + f1;

							if (len > 3) {
								String f2 = fields[2];
								if (!StringUtils.isEmpty(f2))
									refPostId = refPostId + "_" + f2;
							}

						}
					}
				}

				if (!StringUtils.isEmpty(refPostId))
					this.setPostID(refPostId);

			}

		}

		this.rowKey = rowKey;
		this.id = id;
		this.sourceType = SourceType.getSourceType(sourceTypeStr);
		this.numLike = numLike;
		this.numDisLike = numDisLike;
		this.numLikeDate = numLikeDate;
		this.numShare = numShare;

		if (null == numLike)
			numLike = 0l;

		if (null == commentCount)
			commentCount = 0l;

		if (null == numShare)
			numShare = 0l;

		this.numEngage = numLike + commentCount + numShare;
		this.createDate = createDate;
		this.replyCount = replyCount;
		this.commentCount = commentCount;
		this.viewCount = viewCount;
		this.title = title;
		this.url = url;
		this.postID = postID;
		this.publishDate = publishDate;
		this.uId = uId;
		this.imgUrl = imgUrl;
		this.postContent = postContent;
		this.aUrl = aUrl;
		// log.info("numEnage of Post " + this.id + " is " + this.numEngage + ", publish date:" + this.publishDate);

	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public SourceType getSourceType() {
		return sourceType;
	}

	public void setSourceType(SourceType sourceType) {
		this.sourceType = sourceType;
	}

	public Date getPublishDate() {
		return publishDate;
	}

	public void setPublishDate(Date publishDate) {
		this.publishDate = publishDate;
	}

	public Long getNumLike() {
		return numLike;
	}

	public void setNumLike(Long numLike) {
		this.numLike = numLike;
	}

	public Date getNumLikeDate() {
		return numLikeDate;
	}

	public void setNumLikeDate(Date numLikeDate) {
		this.numLikeDate = numLikeDate;
	}

	public Long getNumShare() {
		return numShare;
	}

	public void setNumShare(Long numShare) {
		this.numShare = numShare;
	}

	public Date getNumShareDate() {
		return numShareDate;
	}

	public void setNumShareDate(Date numShareDate) {
		this.numShareDate = numShareDate;
	}

	public Long getNumDisLike() {
		return numDisLike;
	}

	public void setNumDisLike(Long numDisLike) {
		this.numDisLike = numDisLike;
	}

	public Date getNumDisLikeDate() {
		return numDisLikeDate;
	}

	public void setNumDisLikeDate(Date numDisLikeDate) {
		this.numDisLikeDate = numDisLikeDate;
	}

	public Long getViewCount() {
		return viewCount;
	}

	public void setViewCount(Long viewCount) {
		this.viewCount = viewCount;
	}

	public Date getCreateDate() {
		return createDate;
	}

	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}

	public Long getReplyCount() {
		return replyCount;
	}

	public void setReplyCount(Long replyCount) {
		this.replyCount = replyCount;
	}

	public Long getCommentCount() {
		return commentCount;
	}

	public void setCommentCount(Long commentCount) {
		this.commentCount = commentCount;
	}

	public String getPageID() {
		return pageID;
	}

	public void setPageID(String pageID) {
		this.pageID = pageID;
	}

	public String getPostID() {
		return postID;
	}

	public void setPostID(String postID) {
		this.postID = postID;
	}

	public boolean isComment() {
		return isComment;
	}

	public void setComment(boolean isComment) {
		this.isComment = isComment;
	}

	public Map<String, Long> getTopContent() {
		return topContent;
	}

	public void setTopContent(Map<String, Long> topContent) {
		this.topContent = topContent;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getNumEngage() {
		return numEngage;
	}

	public void setNumEngage(Long numEngage) {
		this.numEngage = numEngage;
	}

	public Long getUpEngage() {
		return upEngage;
	}

	public void setUpEngage(Long upEngage) {
		this.upEngage = upEngage;
	}

	public Long getUpLike() {
		return upLike;
	}

	public void setUpLike(Long upLike) {
		this.upLike = upLike;
	}

	public Long getUpComment() {
		return upComment;
	}

	public void setUpComment(Long upComment) {
		this.upComment = upComment;
	}

	public Long getUpShare() {
		return upShare;
	}

	public void setUpShare(Long upShare) {
		this.upShare = upShare;
	}

	public String getuId() {
		return uId;
	}

	public void setuId(String uId) {
		this.uId = uId;
	}

	public String getuName() {
		return uName;
	}

	public void setuName(String uName) {
		this.uName = uName;
	}

	public String getaUrl() {
		return aUrl;
	}

	public void setaUrl(String aUrl) {
		this.aUrl = aUrl;
	}

	public String getImgUrl() {
		return imgUrl;
	}

	public void setImgUrl(String imgUrl) {
		this.imgUrl = imgUrl;
	}

	public String getVideoUrl() {
		return videoUrl;
	}

	public void setVideoUrl(String videoUrl) {
		this.videoUrl = videoUrl;
	}

	public String getPostContent() {
		return postContent;
	}

	public void setPostContent(String postContent) {
		this.postContent = postContent;
	}

	public CAPost getCommentObj() {
		return commentObj;
	}

	public void setCommentObj(CAPost commentObj) {
		this.commentObj = commentObj;
	}

	public String getShowDate() {
		return showDate;
	}

	public void setShowDate(String showDate) {
		this.showDate = showDate;
	}

	public Put toHBasePut() {
		Put put = new Put(Bytes.toBytes(generateHBaseRowKey(sourceType, pageID, id, publishDate, isComment)));
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE, numLike);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_DISLIKE, numDisLike);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_SHARE, numShare);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_VIEW_COUNT, viewCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_COMMENT_COUNT, commentCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_CREATE_DATE, createDate);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_REPLY_COUNT, replyCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_PUBLISH_DATE, publishDate);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_AUTHOR_ID, uId);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_TITLE, title);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_URL, url);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_POST_ID, postID);
		return put;
	}

	public Put toTopContentHBasePut(Date date) {
		Put put = new Put(Bytes.toBytes(generateHBaseRowKey(sourceType, pageID, id, isComment, date)));
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE, numLike);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_DISLIKE, numDisLike);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_SHARE, numShare);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_VIEW_COUNT, viewCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_COMMENT_COUNT, commentCount);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_CREATE_DATE, createDate);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_REPLY_COUNT, replyCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_PUBLISH_DATE, publishDate);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_AUTHOR_ID, uId);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_TITLE, title);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_URL, url);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_POST_ID, postID);
		return put;
	}

	public Put toTopContentDetailPostHBasePut() {
		Put put = new Put(Bytes.toBytes(generateIndexHBaseRowKey(sourceType, id)));
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE, numLike);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_DISLIKE, numDisLike);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_SHARE, numShare);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_VIEW_COUNT, viewCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_COMMENT_COUNT, commentCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_CREATE_DATE, createDate);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_REPLY_COUNT, replyCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_PUBLISH_DATE, publishDate);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_AUTHOR_ID, uId);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_TITLE, title);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_URL, url);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_POST_ID, postID);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_IMAGE_URL, imgUrl);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_VIDEO_URL, videoUrl);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_POST_CONTENT, postContent);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_AVATAR_URL, aUrl);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_USER_NAME, uName);
		return put;
	}

	public Put toTopContentHBasePut() {
		Put put = new Put(Bytes.toBytes(generateHBaseRowKey(sourceType, pageID, id, isComment)));
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE, numLike);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_DISLIKE, numDisLike);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_SHARE, numShare);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_VIEW_COUNT, viewCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_COMMENT_COUNT, commentCount);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_CREATE_DATE, createDate);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_REPLY_COUNT, replyCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_PUBLISH_DATE, publishDate);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_AUTHOR_ID, uId);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_TITLE, title);
		// addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_URL, url);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_POST_ID, postID);
		return put;
	}

	public Put toIndexHBasePut() {
		Put put = new Put(Bytes.toBytes(generateIndexHBaseRowKey(sourceType, id)));
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_CREATE_DATE, createDate);
		return put;
	}

	public static String generateHBaseRowKey(SourceType sourceType, String pageID, String id, Date publishDate, boolean isComment) {
		SimpleDateFormat formatter = new SimpleDateFormat("yy-MM-dd");
		int postFlag = 1;
		if (isComment) {
			postFlag = 0;
		}
		return new StringBuilder(sourceType.getSourceTypeStr()).append("#").append(pageID).append("#").append(postFlag).append("#")
				.append(formatter.format(publishDate)).append("#").append(id).toString();
	}

	public static String generateHBaseRowKey(SourceType sourceType, String pageID, String id, boolean isComment, Date date) {
		SimpleDateFormat formatter = new SimpleDateFormat("yy-MM-dd");
		int postFlag = 1;
		if (isComment) {
			postFlag = 0;
		}
		return new StringBuilder(sourceType.getSourceTypeStr()).append("#").append(pageID).append("#").append(postFlag).append("#")
				.append(formatter.format(date)).append("#").append(id).toString();
	}

	public static String generateHBaseRowKey(SourceType sourceType, String pageID, String id, boolean isComment) {
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yy-MM-dd");
		int postFlag = 1;
		if (isComment) {
			postFlag = 0;
		}
		return new StringBuilder(sourceType.getSourceTypeStr()).append("#").append(pageID).append("#").append(postFlag).append("#")
				.append(formatter.format(currentTime)).append("#").append(id).toString();
	}

	public static String generateIndexHBaseRowKey(SourceType sourceType, String id) {
		return new StringBuilder(sourceType.getSourceTypeStr()).append("#").append(id).toString();
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
