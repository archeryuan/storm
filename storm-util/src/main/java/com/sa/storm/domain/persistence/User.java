package com.sa.storm.domain.persistence;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sa.common.definition.SnsSourceType;
import com.sa.common.definition.SourceType;
import com.sa.graph.definition.ProfileDefinition;

public class User extends BasePersistenceObject implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final DateFormat DATE_DOC_KEY_FORMAT = new SimpleDateFormat("yyyyMMdd");

	public static final String HBASE_TABLE = "u";
	public static final byte[] HBASE_FAMILY_P = Bytes.toBytes("p");
	public static final byte[] HBASE_FAMILY_C = Bytes.toBytes("c");
	public static final byte[] userIdCol = Bytes.toBytes("uid");
	public static final byte[] screenNameCol = Bytes.toBytes("sName");
	public static final byte[] regionIdCol = Bytes.toBytes("regId");
	public static final byte[] parentRegionIdCol = Bytes.toBytes("pRegId");
	public static final byte[] avatarUrlCol = Bytes.toBytes("aUrl");
	public static final byte[] descriptionCol = Bytes.toBytes("desc");
	public static final byte[] numFollowerCol = Bytes.toBytes("folCount");
	public static final byte[] numFollowingCol = Bytes.toBytes("fdCount");
	public static final byte[] numTweetCol = Bytes.toBytes("twCount");
	public static final byte[] createDateCol = Bytes.toBytes("cDate");
	public static final byte[] birthdayCol = Bytes.toBytes("bDay");
	public static final byte[] genderCol = Bytes.toBytes("gender");
	public static final byte[] educationIdCol = Bytes.toBytes("edu");
	public static final byte[] isVipCol = Bytes.toBytes("isVip");
	public static final byte[] isVerifiedCol = Bytes.toBytes("isVer");
	public static final byte[] tagsCol = Bytes.toBytes("tags");
	public static final byte[] occupationIdCol = Bytes.toBytes("occu");
	public static final byte[] profileCreateDateCol = Bytes.toBytes("pCDate");
	public static final byte[] kolActiveScoreCol = Bytes.toBytes("kolAct");
	public static final byte[] kolQualityScoreCol = Bytes.toBytes("kolQly");
	public static final byte[] kolInfluenceScoreCol = Bytes.toBytes("kolInf");
	public static final byte[] kolIsZombieCol = Bytes.toBytes("kolIsZom");
	public static final byte[] kolUpdateDateCol = Bytes.toBytes("kolUDate");

	private String id;
	private SourceType sourceType;
	private String userId; // Solr: uid, HBase:u: uid
	private String screenName; // Solr: uName, HBase: sName
	private Long regionId; // Solr: region, HBase:u: regId
	private Long parentRegionId; // Solr: regionPId, HBase: pRegId
	private String avatarUrl; // Solr: aURL, HBase: aURL
	private String description; // HBase: desc
	private Long numFollower; // HBase: folCount
	private Long numFollowing; // HBase: fdCount
	private Long numTweet; // HBase: twCount
	private Date createDate; // HBase: cDate
	private Date birthday; // Solr: birthday, HBase: bDay
	private String gender; // Solr: gender, HBase: gender
	private Long educationId; // Solr: education, HBase: edu
	private Integer isVip; // HBase: isVip
	private Integer isVerified; // HBase: isVer
	private String tags; // HBase: u:tags
	private Long occupationId; // Solr: occupation, HBase: occu
	private Date profileCreateDate; // HBase: pCDate

	// KOL score
	private Float kolActiveScore;
	private Float kolQualityScore;
	private Float kolInfluenceScore;
	private Boolean kolIsZombie;
	private Date kolUpdateDate;

	private Collection<User> followings;
	private Collection<User> followers;

	private Collection<Document> documents;

	public User() {

	}

	public User(SourceType sourceType, String userId) {
		this.sourceType = sourceType;
		this.userId = userId;
		this.id = generateId(sourceType, userId);
		this.createDate = new Date();
	}

	public User(Result result) throws Exception {
		if (result == null) {
			return;
		}

		final String rowKey = Bytes.toString(result.getRow());
		final String[] rowKeyArr = StringUtils.split(rowKey, "_");

		setSourceType(SourceType.getSourceType(rowKeyArr[0]));
		setUserId(rowKeyArr[1]);
		setAvatarUrl(getStringFromResult(result, HBASE_FAMILY_P, avatarUrlCol));
		setBirthday(getDateFromResult(result, HBASE_FAMILY_P, birthdayCol));
		setCreateDate(getDateFromResult(result, HBASE_FAMILY_P, createDateCol));
		setDescription(getStringFromResult(result, HBASE_FAMILY_P, descriptionCol));
		setEducationId(getLongFromResult(result, HBASE_FAMILY_P, educationIdCol));
		setGender(getStringFromResult(result, HBASE_FAMILY_P, genderCol));
		setIsVerified(getIntegerFromResult(result, HBASE_FAMILY_P, isVerifiedCol));
		setIsVip(getIntegerFromResult(result, HBASE_FAMILY_P, isVipCol));
		setKolActiveScore(getFloatFromResult(result, HBASE_FAMILY_P, kolActiveScoreCol));
		setKolInfluenceScore(getFloatFromResult(result, HBASE_FAMILY_P, kolInfluenceScoreCol));
		setKolQualityScore(getFloatFromResult(result, HBASE_FAMILY_P, kolQualityScoreCol));
		setKolIsZombie(getBooleanFromResult(result, HBASE_FAMILY_P, kolIsZombieCol));
		setKolUpdateDate(getDateFromResult(result, HBASE_FAMILY_P, kolUpdateDateCol));
		setNumFollower(getLongFromResult(result, HBASE_FAMILY_P, numFollowerCol));
		setNumFollowing(getLongFromResult(result, HBASE_FAMILY_P, numFollowingCol));
		setNumTweet(getLongFromResult(result, HBASE_FAMILY_P, numTweetCol));
		setOccupationId(getLongFromResult(result, HBASE_FAMILY_P, occupationIdCol));
		setParentRegionId(getLongFromResult(result, HBASE_FAMILY_P, parentRegionIdCol));
		setProfileCreateDate(getDateFromResult(result, HBASE_FAMILY_P, profileCreateDateCol));
		setRegionId(getLongFromResult(result, HBASE_FAMILY_P, regionIdCol));
		setScreenName(getStringFromResult(result, HBASE_FAMILY_P, screenNameCol));
		setTags(getStringFromResult(result, HBASE_FAMILY_P, tagsCol));
	}

	public static String generateId(SourceType sourceType, String userId) {
		return new StringBuilder(sourceType.getSourceTypeStr()).append("_").append(userId).toString();
	}

	@JsonProperty("id")
	public String getId() {
		return id;
	}

	@JsonProperty("sourceType")
	public SourceType getSourceType() {
		return sourceType;
	}

	public void setSourceType(SourceType sourceType) {
		this.sourceType = sourceType;
	}

	@JsonProperty("userId")
	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	@JsonProperty("screenName")
	public String getScreenName() {
		return screenName;
	}

	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}

	@JsonProperty("regionId")
	public Long getRegionId() {
		return regionId;
	}

	public void setRegionId(Long regionId) {
		this.regionId = regionId;
	}

	@JsonProperty("pRegionId")
	public Long getParentRegionId() {
		return parentRegionId;
	}

	public void setParentRegionId(Long parentRegionId) {
		this.parentRegionId = parentRegionId;
	}

	@JsonProperty("avatarUrl")
	public String getAvatarUrl() {
		return avatarUrl;
	}

	public void setAvatarUrl(String avatarUrl) {
		this.avatarUrl = avatarUrl;
	}

	@JsonProperty("desc")
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@JsonProperty("numFollower")
	public Long getNumFollower() {
		return numFollower;
	}

	public void setNumFollower(Long numFollower) {
		this.numFollower = numFollower;
	}

	@JsonProperty("numFollowing")
	public Long getNumFollowing() {
		return numFollowing;
	}

	public void setNumFollowing(Long numFollowing) {
		this.numFollowing = numFollowing;
	}

	@JsonProperty("numTweet")
	public Long getNumTweet() {
		return numTweet;
	}

	public void setNumTweet(Long numTweet) {
		this.numTweet = numTweet;
	}

	@JsonProperty("createDate")
	public Date getCreateDate() {
		return createDate;
	}

	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
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

	@JsonProperty("isVIP")
	public Integer getIsVip() {
		return isVip;
	}

	public void setIsVip(Integer isVip) {
		this.isVip = isVip;
	}

	@JsonProperty("isVerified")
	public Integer getIsVerified() {
		return isVerified;
	}

	public void setIsVerified(Integer isVerified) {
		this.isVerified = isVerified;
	}

	@JsonProperty("tags")
	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	@JsonProperty("occupationId")
	public Long getOccupationId() {
		return occupationId;
	}

	public void setOccupationId(Long occupationId) {
		this.occupationId = occupationId;
	}

	@JsonProperty("profileCreateDate")
	public Date getProfileCreateDate() {
		return profileCreateDate;
	}

	public void setProfileCreateDate(Date profileCreateDate) {
		this.profileCreateDate = profileCreateDate;
	}

	@JsonProperty("kolActiveScore")
	public Float getKolActiveScore() {
		return kolActiveScore;
	}

	public void setKolActiveScore(Float kolActiveScore) {
		this.kolActiveScore = kolActiveScore;
	}

	@JsonProperty("kolQualityScore")
	public Float getKolQualityScore() {
		return kolQualityScore;
	}

	public void setKolQualityScore(Float kolQualityScore) {
		this.kolQualityScore = kolQualityScore;
	}

	@JsonProperty("kolInfluenceScore")
	public Float getKolInfluenceScore() {
		return kolInfluenceScore;
	}

	public void setKolInfluenceScore(Float kolInfluenceScore) {
		this.kolInfluenceScore = kolInfluenceScore;
	}

	@JsonProperty("kolIsZombie")
	public Boolean getKolIsZombie() {
		return kolIsZombie;
	}

	public void setKolIsZombie(Boolean kolIsZombie) {
		this.kolIsZombie = kolIsZombie;
	}

	@JsonProperty("kolUpdateDate")
	public Date getKolUpdateDate() {
		return kolUpdateDate;
	}

	public void setKolUpdateDate(Date kolUpdateDate) {
		this.kolUpdateDate = kolUpdateDate;
	}

	@JsonProperty("followings")
	public Collection<User> getFollowings() {
		return followings;
	}

	public void setFollowing(Collection<User> followings) {
		this.followings = followings;
	}

	public void addFollowing(User user) {
		if (followings == null) {
			followings = new ArrayList<User>();
		}

		followings.add(user);
	}

	@JsonProperty("followers")
	public Collection<User> getFollowers() {
		return followers;
	}

	public void setFollowers(Collection<User> followers) {
		this.followers = followers;
	}

	public void addFollower(User follower) {
		if (followers == null) {
			followers = new ArrayList<User>();
		}

		followers.add(follower);
	}

	public Set<User> getRelatedUsers() {
		Set<User> output = new HashSet<User>();
		if (followers != null) {
			output.addAll(followers);
		}
		if (followings != null) {
			output.addAll(followings);
		}

		return output;
	}

	@JsonProperty("document")
	public Collection<Document> getDocuments() {
		return documents;
	}

	public void setDocuments(Collection<Document> Documents) {
		this.documents = Documents;
	}

	public void addDocuments(Collection<Document> documents) {
		if (documents != null) {
			if (this.documents == null) {
				this.documents = new ArrayList<Document>();
			}

			this.documents.addAll(documents);
		}
	}

	public void addDocument(Document doc) {
		if (doc != null) {
			if (this.documents == null) {
				this.documents = new ArrayList<Document>();
			}

			this.documents.add(doc);
		}
	}

	public Put toHBasePutForCreate() {
		Put put = new Put(Bytes.toBytes(id));
		return addFieldsForCreateToPut(put);
	}

	public Put toHBasePutWithDocsForCreate() {
		Put put = toHBasePutForCreate();
		return addFieldsForUpdateRelatedDocsToPut(put);
	}

	public Put toHBasePutForUpdate() {
		Put put = new Put(Bytes.toBytes(id));
		return addFieldsForUpdateToPut(put);
	}

	public Put toHBasePutWithDocsForUpdate() {
		Put put = toHBasePutForUpdate();
		return addFieldsForUpdateRelatedDocsToPut(put);
	}

	public Map<ProfileDefinition, Object> toTitanProfile() {
		Map<ProfileDefinition, Object> profile = new HashMap<ProfileDefinition, Object>();

		profile.put(ProfileDefinition.ID, id);
		profile.put(ProfileDefinition.TAG, tags);
		profile.put(ProfileDefinition.SOURCE_TYPE, sourceType.getSourceTypeStr());
		profile.put(ProfileDefinition.SNS_ID, SnsSourceType.getSNSSiteIdBySType(sourceType.getSourceTypeStr()));
		profile.put(ProfileDefinition.AVATAR_URL, avatarUrl);
		profile.put(ProfileDefinition.CREATE_DATE, createDate);
		profile.put(ProfileDefinition.DESC, description);

		if (followers != null) {
			List<Map<ProfileDefinition, Object>> followerProfiles = new ArrayList<Map<ProfileDefinition, Object>>();
			for (User follower : followers) {
				followerProfiles.add(follower.toTitanProfile());
			}
			profile.put(ProfileDefinition.FOLLOWER, followerProfiles);
		}

		profile.put(ProfileDefinition.NO_OF_FOLLOWER, numFollower);
		if (followings != null) {
			List<Map<ProfileDefinition, Object>> followingProfiles = new ArrayList<Map<ProfileDefinition, Object>>();
			for (User following : followings) {
				followingProfiles.add(following.toTitanProfile());
			}
			profile.put(ProfileDefinition.FOLLOWING, followingProfiles);
		}

		profile.put(ProfileDefinition.NO_OF_FOLLOWING, numFollowing);
		profile.put(ProfileDefinition.IS_VIP, isVip);
		profile.put(ProfileDefinition.NO_OF_TWEET, numTweet);
		profile.put(ProfileDefinition.REGION, regionId);
		profile.put(ProfileDefinition.SCREEN_NAME, screenName);
		profile.put(ProfileDefinition.SEX, gender);

		return profile;
	}

	protected Put addFieldsForCreateToPut(Put put) {
		put = addFieldToPut(put, HBASE_FAMILY_P, userIdCol, userId);
		put = addFieldToPut(put, HBASE_FAMILY_P, createDateCol, createDate);
		return addFieldsForUpdateToPut(put);
	}

	protected Put addFieldsForUpdateToPut(Put put) {
		put = addFieldToPut(put, HBASE_FAMILY_P, screenNameCol, screenName);
		put = addFieldToPut(put, HBASE_FAMILY_P, regionIdCol, regionId);
		put = addFieldToPut(put, HBASE_FAMILY_P, parentRegionIdCol, parentRegionId);
		put = addFieldToPut(put, HBASE_FAMILY_P, avatarUrlCol, avatarUrl);
		put = addFieldToPut(put, HBASE_FAMILY_P, descriptionCol, description);
		put = addFieldToPut(put, HBASE_FAMILY_P, numFollowerCol, numFollower);
		put = addFieldToPut(put, HBASE_FAMILY_P, numFollowingCol, numFollowing);
		put = addFieldToPut(put, HBASE_FAMILY_P, numTweetCol, numTweet);
		put = addFieldToPut(put, HBASE_FAMILY_P, birthdayCol, birthday);
		put = addFieldToPut(put, HBASE_FAMILY_P, genderCol, gender);
		put = addFieldToPut(put, HBASE_FAMILY_P, educationIdCol, educationId);
		put = addFieldToPut(put, HBASE_FAMILY_P, isVipCol, isVip);
		put = addFieldToPut(put, HBASE_FAMILY_P, isVerifiedCol, isVerified);
		put = addFieldToPut(put, HBASE_FAMILY_P, occupationIdCol, occupationId);
		put = addFieldToPut(put, HBASE_FAMILY_P, profileCreateDateCol, profileCreateDate);
		put = addFieldToPut(put, HBASE_FAMILY_P, kolUpdateDateCol, kolUpdateDate);
		put = addFieldToPut(put, HBASE_FAMILY_P, kolActiveScoreCol, kolActiveScore);
		put = addFieldToPut(put, HBASE_FAMILY_P, kolQualityScoreCol, kolQualityScore);
		put = addFieldToPut(put, HBASE_FAMILY_P, kolInfluenceScoreCol, kolInfluenceScore);
		put = addFieldToPut(put, HBASE_FAMILY_P, kolIsZombieCol, kolIsZombie);
		put = addFieldToPut(put, HBASE_FAMILY_P, kolUpdateDateCol, kolUpdateDate);
		return put;
	}

	protected Put addFieldsForUpdateRelatedDocsToPut(Put put) {
		if (documents != null) {
			for (Document doc : documents) {
				final byte[] hbaseKey = Bytes.toBytes(new StringBuilder(DATE_DOC_KEY_FORMAT.format(doc.getCreateDate())).append("_")
						.append(doc.getId()).toString());
				put = addFieldToPut(put, HBASE_FAMILY_C, hbaseKey, doc.getId());
			}
		}
		return put;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toStringExclude(this, "followers", "followings");
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (obj instanceof User) {
			User rhs = (User) obj;
			return id.equals(rhs.id);
		} else {
			return false;
		}
	}
}
