package com.sa.storm.domain.persistence;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sa.common.definition.SourceType;
import com.sa.common.util.StringUtils;
import com.sa.crawler.definition.ContentSource;
import com.sa.solr.definition.SolrField;
import com.sa.solr.util.SolrDateUtil;

public class Document extends BasePersistenceObject implements Serializable {

	private static final long serialVersionUID = 1L;

	// d table for document
	public static final String HBASE_TABLE = "d";
	public static final byte[] HBASE_FAMILY_D = Bytes.toBytes("d");
	public static final byte[] HBASE_FAMILY_I = Bytes.toBytes("i");

	public static final byte[] publishDateCol = Bytes.toBytes("pDate");
	public static final byte[] userNameCol = Bytes.toBytes("uName");
	public static final byte[] siteIdCol = Bytes.toBytes("siteId");
	public static final byte[] avatarUrlCol = Bytes.toBytes("aUrl");
	public static final byte[] sourceTypeCol = Bytes.toBytes("sType");
	public static final byte[] titleCol = Bytes.toBytes("title");
	public static final byte[] regionIdCol = Bytes.toBytes("reg");
	public static final byte[] createDateCol = Bytes.toBytes("cDate");
	public static final byte[] sentimentCol = Bytes.toBytes("sMent");
	public static final byte[] sentimentScoreCol = Bytes.toBytes("sScore");
	public static final byte[] userIdCol = Bytes.toBytes("uId");
	public static final byte[] commentCountCol = Bytes.toBytes("comCount");
	public static final byte[] retweetCountCol = Bytes.toBytes("retCount");
	public static final byte[] twitterIdCol = Bytes.toBytes("tId");
	public static final byte[] userScreenNameCol = Bytes.toBytes("sName");
	public static final byte[] sContentCol = Bytes.toBytes("sContent");
	public static final byte[] coordinateCol = Bytes.toBytes("xy");
	public static final byte[] regionNameCol = Bytes.toBytes("regName");
	public static final byte[] isoLangCodeCol = Bytes.toBytes("lang");
	public static final byte[] likeCountCol = Bytes.toBytes("numLike");
	public static final byte[] clickCountCol = Bytes.toBytes("numClick");
	public static final byte[] viewCountCol = Bytes.toBytes("viewCount");

	public static final byte[] contentCol = Bytes.toBytes("content");
	public static final byte[] domainCol = Bytes.toBytes("domain");
	public static final byte[] categoryCol = Bytes.toBytes("cat");
	public static final byte[] typeCol = Bytes.toBytes("type");
	public static final byte[] emailCol = Bytes.toBytes("email");
	public static final byte[] urlCol = Bytes.toBytes("url");
	public static final byte[] isRubbishCol = Bytes.toBytes("isRub");
	// public static final byte[] isVIPCol = Bytes.toBytes("isVIP");
	public static final byte[] toUserUniqueNameCol = Bytes.toBytes("tuName");
	public static final byte[] toUserScreenNameCol = Bytes.toBytes("tsName");
	public static final byte[] toUserIdCol = Bytes.toBytes("tuId");
	public static final byte[] replyDocIdCol = Bytes.toBytes("rId");
	public static final byte[] replyDocUserIdCol = Bytes.toBytes("rUserId");
	public static final byte[] isReplyCol = Bytes.toBytes("isRp");
	public static final byte[] isRetweetCol = Bytes.toBytes("isRt");
	public static final byte[] keywordsCol = Bytes.toBytes("kw");
	public static final byte[] segmentedTitleCol = Bytes.toBytes("segTtl");
	public static final byte[] segmentedContentCol = Bytes.toBytes("segCtnt");
	public static final byte[] tweetTypeCol = Bytes.toBytes("twType");
	public static final byte[] imgURLCol = Bytes.toBytes("imgURL");
	public static final byte[] videoURLCol = Bytes.toBytes("videoURL");
	public static final byte[] catTagsCol = Bytes.toBytes("catTags");
	public static final byte[] contentSourceCol = Bytes.toBytes("ctntSrc");
	public static final byte[] crawlerTypeCol = Bytes.toBytes("cType");
	public static final String profileIdsColPrefix = "PF";
	public static final String nameEntityLocationsColPrefix = "NE_L";
	public static final String nameEntityOrgsColPrefix = "NE_O";
	public static final String nameEntityPeopleColPrefix = "NE_P";
	public static final String nameEntityPeopleOrgsColPrefix = "NE_PO";
	public static final String nameEntityPeopleOrgRolesColPrefix = "NE_POR";

	public static final byte[] additionalTwitterIdCol = Bytes.toBytes("addTId");

	// Provide unique integer among multiple threads when generating ID
	private static int serialNum = 0;
	private static final int maxSerialNum = 9999;

	private String id; // Solr: id, HBase: rowKey
	private Date publishDate; // Solr: pDate, HBase: d:pDate
	private String userName; // Solr: uName, HBase: d:uName
	private Long siteId; // Solr: siteId, HBase: d:siteId
	private Long snsSiteId; // Solr: SNSsiteId
	private String avatarUrl; // Solr: aURL, HBase: d:aUrl
	private SourceType sourceType; // Solr: sType, HBase: d:sType
	private String title; // Solr: title, HBase: d:title
	private String content; // Solr: content, HBase: i:content
	private String rawRegionName; // Transient object for regionId/regionPId
	private Long regionId; // Solr: region, HBase: d:reg
	private Date createDate; // Solr: cDate, HBase: d:cDate
	private String domain; // Solr: domain, HBase: i:domain
	private Integer sentiment; // Solr: sMent, HBase: d:sMent
	private Float sentimentScore; // Solr: sScore, HBase: d:sScore
	private String category; // Solr: cat, HBase: i:cat
	private Long typeId; // Solr: type, HBase: i:type
	private List<Long> profileIds; // Solr: pIds
	private String email; // Solr: email, HBase: i:email
	private String userId; // Solr: uId, HBase: d:uId
	private String tweetUserId; // transient field of userId
	private String emotion; // Solr: emotion
	private String url; // Solr: link, HBase: i:url
	private Long commentCount; // Solr: comCount, HBase: d:comCount
	private Long retweetCount; // Solr: retCount, HBase: d:retcount
	private Integer isRubbish; // Solr: isRub, HBase: i:isRub
	// private Integer isVIP; // Solr: isVIP, HBase: i:isVIP

	private String twitterId; // HBase: d:tId, solr: tweetId
	private String userScreenName; // HBase: d:sName
	private String sContent; // HBase: d:sContent
	private String coordinate; // HBase: d:xy
	private String regionName; // HBase: d:regName
	private String isoLangCode; // HBase: d:lang
	private Long likeCount; // Solr: numLike, HBase: d:numLike
	private Long clickCount; // HBase: d:numClick
	private Long viewCount; // HBase: d:viewCount
	private String toUserUniqueName; // HBase: i:tuName
	private String toUserScreenName; // HBase: i:tsName
	private Long toUserId; // HBase: i:tuId
	private String replyDocId; // HBase: i:rId, Solr: rID
	private String replyTweetId; // Transient field of replyDocId
	private String replyDocUserId; // Solr: rUserId
	private Integer isReply; // HBase: i:isRp
	private Integer isRetweet; // HBase: i:isRt
	private String keywords; // HBase: i:kw
	private String segmentedTitle; // HBase: i:segTtl
	private String segmentedContent; // HBase: i:segCtnt

	private Integer tweetType; // HBase: i:twType
	private String imgURL;
	private String photoID;
	private String bigImgURL; // Hase: bigImgURL, Solr: bigImgURL
	private String videoURL;
	private Long regionPId;
	private Date birthday;
	private String gender;
	private Long educationId;
	private Long occupationId;
	private Integer app;
	private String lastFlag;
	private String fileName;
	private String location;
	private String age;
	private String historyId;
	private List<String> catTags;
	private String countryCode;

	private String node;
	private String additionalTwitterId;
	private String contentSource;
	private Integer crawlerType;

	private Set<String> locations;
	private Set<String> organizations;
	private Set<String> people;
	private Map<String, String> peopleOrgs;
	private Map<String, String> peopleOrgRoles;

	private boolean isProduction;

	public Document() {

	}

	public Document(String twitterId, SourceType sourceType, String nodeId, ContentSource contentSource) throws Exception {
		this.id = Document.generateId(sourceType, nodeId);
		this.twitterId = twitterId;
		this.sourceType = sourceType;
		this.node = nodeId;
		this.contentSource = contentSource.getValue();
	}

	public Document(String twitterId, SourceType sourceType, String nodeId, ContentSource contentSource, Integer crawlerType)
			throws Exception {
		this.id = Document.generateId(sourceType, nodeId);
		this.twitterId = twitterId;
		this.sourceType = sourceType;
		this.node = nodeId;
		this.contentSource = contentSource.getValue();
		this.crawlerType = crawlerType;
	}

	public Document(String id, String twitterId, SourceType sourceType) {
		this.id = id;
		this.twitterId = twitterId;
		this.sourceType = sourceType;
	}

	public Long getRegionPId() {
		return regionPId;
	}

	public void setRegionPId(Long regionPId) {
		this.regionPId = regionPId;
	}

	@JsonProperty("id")
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@JsonProperty("imgUrl")
	public String getImgURL() {
		return imgURL;
	}

	public void setImgURL(String imgURL) {
		this.imgURL = imgURL;
	}

	@JsonProperty("lastFlag")
	public String getLastFlag() {
		return lastFlag;
	}

	public void setLastFlag(String lastFlag) {
		this.lastFlag = lastFlag;
	}

	@JsonProperty("fileName")
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	@JsonProperty("historyId")
	public String getHistoryId() {
		return historyId;
	}

	public void setHistoryId(String historyId) {
		this.historyId = historyId;
	}

	@JsonProperty("location")
	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	@JsonProperty("age")
	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	@JsonProperty("videoUrl")
	public String getVideoURL() {
		return videoURL;
	}

	public void setVideoURL(String videoURL) {
		this.videoURL = videoURL;
	}

	@JsonProperty("publishDate")
	public Date getPublishDate() {
		return publishDate;
	}

	public void setPublishDate(Date publishDate) {
		this.publishDate = publishDate;
	}

	@JsonProperty("userName")
	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	@JsonProperty("siteId")
	public Long getSiteId() {
		return siteId;
	}

	public void setSiteId(Long siteId) {
		this.siteId = siteId;
	}

	@JsonProperty("snsSiteId")
	public Long getSnsSiteId() {
		return snsSiteId;
	}

	public void setSnsSiteId(Long snsSiteId) {
		this.snsSiteId = snsSiteId;
	}

	@JsonProperty("avatarUrl")
	public String getAvatarUrl() {
		return avatarUrl;
	}

	public void setAvatarUrl(String avatarUrl) {
		this.avatarUrl = avatarUrl;
	}

	@JsonProperty("sourceType")
	public SourceType getSourceType() {
		return sourceType;
	}

	public void setSourceType(SourceType sourceType) {
		this.sourceType = sourceType;
	}

	@JsonProperty("title")
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	@JsonProperty("content")
	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@JsonProperty("rawRegionName")
	public String getRawRegionName() {
		return rawRegionName;
	}

	public void setRawRegionName(String rawRegionName) {
		this.rawRegionName = rawRegionName;
	}

	@JsonProperty("regionId")
	public Long getRegionId() {
		return regionId;
	}

	public void setRegionId(Long regionId) {
		this.regionId = regionId;
	}

	@JsonProperty("createDate")
	public Date getCreateDate() {
		return createDate;
	}

	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}

	@JsonProperty("domain")
	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	@JsonProperty("sentiment")
	public Integer getSentiment() {
		return sentiment;
	}

	public void setSentiment(Integer sentiment) {
		this.sentiment = sentiment;
	}

	@JsonProperty("sentimentScore")
	public Float getSentimentScore() {
		return sentimentScore;
	}

	public void setSentimentScore(Float sentimentScore) {
		this.sentimentScore = sentimentScore;
	}

	@JsonProperty("category")
	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	@JsonProperty("typeId")
	public Long getTypeId() {
		return typeId;
	}

	public void setTypeId(Long typeId) {
		this.typeId = typeId;
	}

	@JsonProperty("profileIds")
	public List<Long> getProfileIds() {
		return profileIds;
	}

	public void setProfileIds(List<Long> profileIds) {
		this.profileIds = profileIds;
	}

	@JsonProperty("email")
	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	@JsonProperty("userId")
	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	@JsonProperty("emotion")
	public String getEmotion() {
		return emotion;
	}

	public void setEmotion(String emotion) {
		this.emotion = emotion;
	}

	@JsonProperty("url")
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@JsonProperty("commentCount")
	public Long getCommentCount() {
		return commentCount;
	}

	public void setCommentCount(Long commentCount) {
		this.commentCount = commentCount;
	}

	@JsonProperty("retewwtCount")
	public Long getRetweetCount() {
		return retweetCount;
	}

	public void setRetweetCount(Long retweetCount) {
		this.retweetCount = retweetCount;
	}

	@JsonProperty("isRubbish")
	public Integer getIsRubbish() {
		return isRubbish;
	}

	public void setIsRubbish(Integer isRubbish) {
		this.isRubbish = isRubbish;
	}

	@JsonProperty("twitterId")
	public String getTwitterId() {
		return twitterId;
	}

	public void setTwitterId(String twitterId) {
		this.twitterId = twitterId;
	}

	@JsonProperty("userScreenName")
	public String getUserScreenName() {
		return userScreenName;
	}

	public void setUserScreenName(String userScreenName) {
		this.userScreenName = userScreenName;
	}

	@JsonProperty("sContent")
	public String getsContent() {
		return sContent;
	}

	public void setsContent(String sContent) {
		this.sContent = sContent;
	}

	@JsonProperty("coordinate")
	public String getCoordinate() {
		return coordinate;
	}

	public void setCoordinate(String coordinate) {
		this.coordinate = coordinate;
	}

	@JsonProperty("regionName")
	public String getRegionName() {
		return regionName;
	}

	public void setRegionName(String regionName) {
		this.regionName = regionName;
	}

	@JsonProperty("isoLangCode")
	public String getIsoLangCode() {
		return isoLangCode;
	}

	public void setIsoLangCode(String isoLangCode) {
		this.isoLangCode = isoLangCode;
	}

	@JsonProperty("likeCount")
	public Long getLikeCount() {
		return likeCount;
	}

	public void setLikeCount(Long likeCount) {
		this.likeCount = likeCount;
	}

	@JsonProperty("clickCount")
	public Long getClickCount() {
		return clickCount;
	}

	public void setClickCount(Long clickCount) {
		this.clickCount = clickCount;
	}

	@JsonProperty("viewCount")
	public Long getViewCount() {
		return viewCount;
	}

	public void setViewCount(Long viewCount) {
		this.viewCount = viewCount;
	}

	@JsonProperty("toUserUniqueName")
	public String getToUserUniqueName() {
		return toUserUniqueName;
	}

	public void setToUserUniqueName(String toUserUniqueName) {
		this.toUserUniqueName = toUserUniqueName;
	}

	@JsonProperty("toUserScreenName")
	public String getToUserScreenName() {
		return toUserScreenName;
	}

	public void setToUserScreenName(String toUserScreenName) {
		this.toUserScreenName = toUserScreenName;
	}

	@JsonProperty("toUserId")
	public Long getToUserId() {
		return toUserId;
	}

	public void setToUserId(Long toUserId) {
		this.toUserId = toUserId;
	}

	@JsonProperty("replyTweetId")
	public String getReplyTweetId() {
		return replyTweetId;
	}

	public void setReplyTweetId(String replyTweetId) {
		this.replyTweetId = replyTweetId;
	}

	@JsonProperty("replyDocId")
	public String getReplyDocId() {
		return replyDocId;
	}

	public void setReplyDocId(String replyDocId) {
		this.replyDocId = replyDocId;
	}

	@JsonProperty("replyDocUserId")
	public String getReplyDocUserId() {
		return replyDocUserId;
	}

	public void setReplyDocUserId(String replyDocUserId) {
		this.replyDocUserId = replyDocUserId;
	}

	@JsonProperty("isReply")
	public Integer getIsReply() {
		return isReply;
	}

	public void setIsReply(Integer isReply) {
		this.isReply = isReply;
	}

	@JsonProperty("isRetweet")
	public Integer getIsRetweet() {
		return isRetweet;
	}

	public void setIsRetweet(Integer isRetweet) {
		this.isRetweet = isRetweet;
	}

	@JsonProperty("keywords")
	public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	@JsonProperty("segmentedTitle")
	public String getSegmentedTitle() {
		return segmentedTitle;
	}

	public void setSegmentedTitle(String segmentedTitle) {
		this.segmentedTitle = segmentedTitle;
	}

	@JsonProperty("segmentedContent")
	public String getSegmentedContent() {
		return segmentedContent;
	}

	public void setSegmentedContent(String segmentedContent) {
		this.segmentedContent = segmentedContent;
	}

	@JsonProperty("tweetType")
	public Integer getTweetType() {
		return tweetType;
	}

	public void setTweetType(Integer tweetType) {
		this.tweetType = tweetType;
	}

	@JsonProperty("birthday")
	public Date getBirthday() {
		return birthday;
	}

	public void setBirthday(Date birthday) {
		this.birthday = birthday;
	}

	@JsonProperty("gender")
	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	@JsonProperty("educationId")
	public Long getEducationId() {
		return educationId;
	}

	public void setEducationId(Long educationId) {
		this.educationId = educationId;
	}

	@JsonProperty("occupationId")
	public Long getOccupationId() {
		return occupationId;
	}

	public void setOccupationId(Long occupationId) {
		this.occupationId = occupationId;
	}

	@JsonProperty("tweetUserId")
	public String getTweetUserId() {
		return tweetUserId;
	}

	public void setTweetUserId(String tweetUserId) {
		this.tweetUserId = tweetUserId;
	}

	@JsonProperty("catTags")
	public List<String> getCatTags() {
		return catTags;
	}

	public void setCatTags(List<String> catTags) {
		this.catTags = catTags;
	}

	@JsonProperty("countryCode")
	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	@JsonProperty("node")
	public String getNode() {
		return node;
	}

	public void setNode(String node) {
		this.node = node;
	}

	@JsonProperty("addTweetId")
	public String getAdditionalTwitterId() {
		return additionalTwitterId;
	}

	public void setAdditionalTwitterId(String additionalTwitterId) {
		this.additionalTwitterId = additionalTwitterId;
	}

	public String getContentSource() {
		return contentSource;
	}

	public void setContentSource(String contentSource) {
		this.contentSource = contentSource;
	}

	/**
	 * @return the crawlerType
	 */
	public Integer getCrawlerType() {
		return crawlerType;
	}

	/**
	 * @param crawlerType
	 *            the crawlerType to set
	 */
	public void setCrawlerType(Integer crawlerType) {
		this.crawlerType = crawlerType;
	}

	public Set<String> getLocations() {
		return locations;
	}

	public void setLocations(Set<String> locations) {
		this.locations = locations;
	}

	public void addLocation(String location) {
		if (locations == null) {
			locations = new HashSet<String>();
		}
		locations.add(location);
	}

	public Set<String> getOrganizations() {
		return organizations;
	}

	public void setOrganizations(Set<String> organizations) {
		this.organizations = organizations;
	}

	public void addOrganization(String organization) {
		if (organizations == null) {
			organizations = new HashSet<String>();
		}
		organizations.add(organization);
	}

	public Set<String> getPeople() {
		return people;
	}

	public void setPeople(Set<String> people) {
		this.people = people;
	}

	public void addPerson(String person) {
		if (people == null) {
			people = new HashSet<String>();
		}
		people.add(person);
	}

	public Map<String, String> getPeopleOrgs() {
		return peopleOrgs;
	}

	public void setPeopleOrgs(Map<String, String> peopleOrgs) {
		this.peopleOrgs = peopleOrgs;
	}

	public void addPeopleOrg(String person, String org) {
		if (peopleOrgs == null) {
			peopleOrgs = new HashMap<String, String>();
		}
		peopleOrgs.put(person, org);
	}

	public Map<String, String> getPeopleOrgRoles() {
		return peopleOrgRoles;
	}

	public void setPeopleOrgRoles(Map<String, String> peopleOrgRoles) {
		this.peopleOrgRoles = peopleOrgRoles;
	}

	public void addPeopleOrgRole(String person, String orgRole) {
		if (peopleOrgRoles == null) {
			peopleOrgRoles = new HashMap<String, String>();
		}
		peopleOrgRoles.put(person, orgRole);
	}

	public boolean isProduction() {
		return isProduction;
	}

	public void setProduction(boolean isProduction) {
		this.isProduction = isProduction;
	}

	@JsonProperty("appType")
	public Integer getAppSourceType() {
		return app;
	}

	public void setAppSourceType(Integer app) {
		this.app = app;
	}

	/**
	 * Set the reply document to this document. Use this instead of setter on individual field to avoid inconsistency
	 * 
	 * @param replyTweetId
	 */
	public void setReplyDocument(String replyTweetId) {
		this.replyTweetId = replyTweetId;
		this.replyDocId = null;
		this.replyDocUserId = null;
	}

	/**
	 * Generate a HBase Put for creating HBase
	 * 
	 * @return Put object to be create into HBase
	 */
	public Put toHBasePutForCreate() {
		Put put = new Put(Bytes.toBytes(this.id));
		return this.addFieldsForCreateToPut(put);
	}

	/**
	 * Generate a HBase Put for updating HBase
	 * 
	 * @return Put object to be updated into HBase
	 */
	public Put toHBasePutForUpdate() {
		Put put = new Put(Bytes.toBytes(this.id));
		return this.addFieldsForUpdateToPut(put);
	}

	/**
	 * Generate SolrInputDocument for creating Solr Index
	 * 
	 * @return Document to be updated into index
	 * @throws ParseException
	 */
	public SolrInputDocument toSolrInputDocumentForCreate() throws ParseException {
		return this.addFieldsForCreateToSolrInputDocument(new SolrInputDocument());
	}

	/**
	 * Generate SolrInputDocument for updating Solr Index
	 * 
	 * @param solrDoc
	 *            Existing document to be updated
	 * @return
	 * @throws ParseException
	 */
	public SolrInputDocument toSolrInputDocumentForUpdate(SolrDocument solrDoc) throws ParseException {
		return this.addFieldsForUpdateToSolrInputDocument(ClientUtils.toSolrInputDocument(solrDoc));
	}

	@Override
	public String toString() {
		String[] excludedFieldNames = { "content" };
		return ReflectionToStringBuilder.toStringExclude(this, excludedFieldNames);
	}

	@JsonIgnore
	public static String getRedisKey(String sourceTypeStr, String twitterId) {
		return new StringBuffer("ct-").append(sourceTypeStr).append("-").append(twitterId).toString();
	}

	@JsonIgnore
	public static String getIdPrefix(String snsSourceTypeStr, long timestamp) {
		final String placeHolderStr = "0000";
		final long reverseTimestamp = Long.MAX_VALUE - timestamp;

		return new StringBuffer(snsSourceTypeStr).append("_").append(placeHolderStr).append("_").append(reverseTimestamp).toString();
	}

	/**
	 * Generate ID in format of sourceType_reverserTimestamp_randomNum e.g. 01_0000_9223370694424347506_00100183. Synchronized to ensure the
	 * ID generated are unique
	 * 
	 * @param sourceType
	 *            e.g. 05
	 * @return unique ID for this document
	 * @throws Exception
	 */
	public static String generateId(SourceType sourceType) throws Exception {

		final String idPrefix = Document.getIdPrefix(sourceType.getSourceTypeStr(), System.currentTimeMillis());
		int suffixNum = 0;

		synchronized (Document.class) {
			suffixNum = Document.serialNum;
			if (suffixNum >= Document.maxSerialNum) {
				Document.serialNum = 0;
			} else {
				Document.serialNum++;
			}
		}

		return new StringBuffer(idPrefix).append("_").append(StringUtils.leftPad("0", 3, "0")).append(String.format("%05d", suffixNum))
				.toString();
	}

	/**
	 * Generate ID in format of sourceType_reverseTimestamp_<node ID><serial no.>. The uniqueKey should be unique within the specified
	 * source type
	 * 
	 * @param sourceType
	 * @param nodeId
	 * @return
	 * @throws Exception
	 */
	public static String generateId(SourceType sourceType, String nodeId) throws Exception {

		final String idPrefix = Document.getIdPrefix(sourceType.getSourceTypeStr(), System.currentTimeMillis());

		int suffixNum = 0;

		synchronized (Document.class) {
			suffixNum = Document.serialNum;
			if (suffixNum >= Document.maxSerialNum) {
				Document.serialNum = 0;
			} else {
				Document.serialNum++;
			}
		}

		return new StringBuffer(idPrefix).append("_").append(StringUtils.leftPad(nodeId, 3, "0")).append(String.format("%05d", suffixNum))
				.toString();
	}

	protected Put addFieldsForCreateToPut(Put put) {
		put = this.addFieldToPut(put, HBASE_FAMILY_D, siteIdCol, this.siteId);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, createDateCol, this.createDate);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, typeCol, this.typeId);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, contentSourceCol, this.contentSource);

		if (this.crawlerType != null)
			put = this.addFieldToPut(put, HBASE_FAMILY_I, crawlerTypeCol, this.crawlerType);

		return this.addFieldsForUpdateToPut(put);
	}

	protected Put addFieldsForUpdateToPut(Put put) {
		stripNonUtf8Character();

		put = this.addFieldToPut(put, HBASE_FAMILY_D, publishDateCol, this.publishDate);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, userNameCol, this.userName);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, avatarUrlCol, this.avatarUrl);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, sourceTypeCol, this.sourceType.getSourceTypeStr());
		put = this.addFieldToPut(put, HBASE_FAMILY_D, titleCol, this.title);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, regionIdCol, this.regionId);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, sentimentCol, this.sentiment);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, sentimentScoreCol, this.sentimentScore);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, userIdCol, this.userId);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, commentCountCol, this.commentCount);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, retweetCountCol, this.retweetCount);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, twitterIdCol, this.twitterId);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, userScreenNameCol, this.userScreenName);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, sContentCol, this.sContent);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, coordinateCol, this.coordinate);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, regionNameCol, this.regionName);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, isoLangCodeCol, this.isoLangCode);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, likeCountCol, this.likeCount);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, clickCountCol, this.clickCount);
		put = this.addFieldToPut(put, HBASE_FAMILY_D, viewCountCol, this.viewCount);

		put = this.addFieldToPut(put, HBASE_FAMILY_I, contentCol, this.content);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, domainCol, this.domain);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, categoryCol, this.category);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, emailCol, this.email);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, urlCol, this.url);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, isRubbishCol, this.isRubbish);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, toUserUniqueNameCol, this.toUserUniqueName);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, toUserScreenNameCol, this.toUserScreenName);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, toUserIdCol, this.toUserId);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, replyDocIdCol, this.replyDocId);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, replyDocUserIdCol, this.replyDocUserId);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, isReplyCol, this.isReply);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, isRetweetCol, this.isRetweet);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, keywordsCol, this.keywords);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, segmentedTitleCol, this.segmentedTitle);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, segmentedContentCol, this.segmentedContent);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, imgURLCol, this.imgURL);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, videoURLCol, this.videoURL);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, tweetTypeCol, this.tweetType);
		put = this.addFieldToPut(put, HBASE_FAMILY_I, catTagsCol, StringUtils.join(catTags, ","));
		if (profileIds != null) {
			for (Long profileId : this.profileIds) {
				this.addFieldToPut(put, HBASE_FAMILY_I, Bytes.toBytes(profileIdsColPrefix + profileId), "1");
			}
		}
		put = this.addFieldToPut(put, HBASE_FAMILY_I, additionalTwitterIdCol, this.additionalTwitterId);

		if (locations != null) {
			for (String location : this.locations) {
				this.addFieldToPut(put, HBASE_FAMILY_I, Bytes.toBytes(nameEntityLocationsColPrefix + location), "");
			}
		}
		if (organizations != null) {
			for (String organization : this.organizations) {
				this.addFieldToPut(put, HBASE_FAMILY_I, Bytes.toBytes(nameEntityOrgsColPrefix + organization), "");
			}
		}
		if (people != null) {
			for (String person : this.people) {
				this.addFieldToPut(put, HBASE_FAMILY_I, Bytes.toBytes(nameEntityPeopleColPrefix + person), "");
				final String personOrg = peopleOrgs.get(person);
				final String personOrgRole = peopleOrgRoles.get(person);
				if (personOrg != null) {
					this.addFieldToPut(put, HBASE_FAMILY_I, Bytes.toBytes(nameEntityPeopleOrgsColPrefix + person), personOrg);
				}
				if (personOrgRole != null) {
					this.addFieldToPut(put, HBASE_FAMILY_I, Bytes.toBytes(nameEntityPeopleOrgRolesColPrefix + person), personOrgRole);
				}
			}
		}

		return put;
	}

	protected SolrInputDocument addFieldsForCreateToSolrInputDocument(SolrInputDocument inputDoc) throws ParseException {

		if (id != null && !id.isEmpty()) {
			inputDoc.setField(SolrField.id.getValue(), id);
		}
		if (createDate != null) {
			inputDoc.setField(SolrField.cDate.getValue(), SolrDateUtil.getSolrDateStr(createDate));
		}
		if (siteId != null) {
			inputDoc.setField(SolrField.siteId.getValue(), siteId);
		}
		if (snsSiteId != null) {
			inputDoc.setField(SolrField.SNSsiteId.getValue(), snsSiteId);
		}
		if (sourceType != null) {
			inputDoc.setField(SolrField.sType.getValue(), sourceType);
		}
		if (twitterId != null && !twitterId.isEmpty()) {
			inputDoc.setField(SolrField.tweetId.getValue(), twitterId);
		}

		return addFieldsForUpdateToSolrInputDocument(inputDoc);
	}

	protected SolrInputDocument addFieldsForUpdateToSolrInputDocument(SolrInputDocument inputDoc) throws ParseException {
		stripNonUtf8Character();

		if (publishDate != null) {
			inputDoc.setField(SolrField.pDate.getValue(), SolrDateUtil.getSolrDateStr(publishDate));
		}
		if (userName != null && !userName.isEmpty()) {
			inputDoc.setField(SolrField.uName.getValue(), userName);
		}
		if (avatarUrl != null && !avatarUrl.isEmpty()) {
			inputDoc.setField(SolrField.aURL.getValue(), avatarUrl);
		}
		if (title != null && !title.isEmpty()) {
			inputDoc.setField(SolrField.title.getValue(), title);
		}
		if (content != null && !content.isEmpty()) {
			inputDoc.setField(SolrField.content.getValue(), content);
		}
		if (regionId != null) {
			inputDoc.setField(SolrField.region.getValue(), regionId);
		}
		if (domain != null && !domain.isEmpty()) {
			inputDoc.setField(SolrField.domain.getValue(), domain);
		}
		if (sentiment != null) {
			inputDoc.setField(SolrField.sMent.getValue(), sentiment);
		}
		if (category != null && !category.isEmpty()) {
			inputDoc.setField(SolrField.cat.getValue(), category);
		}
		if (typeId != null) {
			inputDoc.setField(SolrField.type.getValue(), typeId);
		}
		if (email != null && !email.isEmpty()) {
			inputDoc.setField(SolrField.email.getValue(), email);
		}
		if (userId != null && !userId.isEmpty()) {
			inputDoc.setField(SolrField.uid.getValue(), userId);
		}
		if (url != null && !url.isEmpty()) {
			inputDoc.setField(SolrField.link.getValue(), url);
		}
		if (commentCount != null) {
			inputDoc.setField(SolrField.comCount.getValue(), commentCount);
		}
		if (retweetCount != null) {
			inputDoc.setField(SolrField.retCount.getValue(), retweetCount);
		}
		if (clickCount != null) {
			inputDoc.setField(SolrField.clickCount.getValue(), clickCount);
		}
		if (isRubbish != null) {
			inputDoc.setField(SolrField.isRub.getValue(), isRubbish);
		}
		if (imgURL != null && !imgURL.isEmpty()) {
			inputDoc.setField(SolrField.imgURL.getValue(), imgURL);
		}
		if (videoURL != null && !videoURL.isEmpty()) {
			inputDoc.setField(SolrField.videoURL.getValue(), videoURL);
		}
		if (tweetType != null) {
			inputDoc.setField(SolrField.tweetType.getValue(), tweetType);
		}
		if (replyDocId != null && !replyDocId.isEmpty()) {
			inputDoc.setField(SolrField.rID.getValue(), replyDocId);
		}
		if (replyDocUserId != null && !replyDocUserId.isEmpty()) {
			inputDoc.setField(SolrField.rUserId.getValue(), replyDocUserId);
		}
		if (birthday != null) {
			inputDoc.setField(SolrField.birthday.getValue(), SolrDateUtil.getSolrDateStr(birthday));
		}
		if (gender != null && !gender.isEmpty()) {
			inputDoc.setField(SolrField.gender.getValue(), gender);
		}
		if (educationId != null) {
			inputDoc.setField(SolrField.education.getValue(), educationId);
		}
		if (occupationId != null) {
			inputDoc.setField(SolrField.occupation.getValue(), occupationId);
		}
		if (catTags != null) {
			inputDoc.setField(SolrField.catTags.getValue(), catTags);
		}
		if (bigImgURL != null) {
			inputDoc.setField(SolrField.bigImgURL.getValue(), bigImgURL);
		}
		if (app != null) {
			inputDoc.setField(SolrField.app.getValue(), app);
		}
		return inputDoc;
	}

	private void stripNonUtf8Character() {
		userName = StringUtils.stripNonUTF8Char(userName);
		avatarUrl = StringUtils.stripNonUTF8Char(avatarUrl);
		title = StringUtils.stripNonUTF8Char(title);
		userScreenName = StringUtils.stripNonUTF8Char(userScreenName);
		sContent = StringUtils.stripNonUTF8Char(sContent);
		content = StringUtils.stripNonUTF8Char(content);
		email = StringUtils.stripNonUTF8Char(email);
		url = StringUtils.stripNonUTF8Char(url);
		imgURL = StringUtils.stripNonUTF8Char(imgURL);
		videoURL = StringUtils.stripNonUTF8Char(videoURL);
	}

	public String getPhotoID() {
		return photoID;
	}

	public void setPhotoID(String photoID) {
		this.photoID = photoID;
	}

	public String getBigImgURL() {
		return bigImgURL;
	}

	public void setBigImgURL(String bigImgURL) {
		this.bigImgURL = bigImgURL;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Document other = (Document) obj;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}

	// @Override
	// public int hashCode() {
	// return new StringBuilder(sourceType.getSourceTypeStr()).append(twitterId).toString().hashCode();
	// }
}
