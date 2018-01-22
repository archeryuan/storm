/**
 *Copyright(C) ©2015 Social Hunter. All rights reserved.
 *
 */
package com.sa.storm.sns.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.common.definition.Sns;
import social.hunt.data.dao.DashboardProfileRepository;
import social.hunt.data.domain.DashboardProfile;
import social.hunt.language.util.ChineseTranslator;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.json.JsonUtil;
import com.sa.common.util.StringUtils;
import com.sa.common.util.UrlUtil;
import com.sa.redis.definition.RedisDefinition.ChannelDef;
import com.sa.redis.util.RedisUtil;
import com.sa.solr.domain.Feed;
import com.sa.storm.framework.App;

/**
 * @author Luke
 *
 */
public final class WebSocketUtil {
	private static final Logger log = LoggerFactory.getLogger(WebSocketUtil.class);

	/**
	 * Publish notification event when profile keyword has new feeds coming.
	 * 
	 * @param docs
	 * @param highlightPre
	 * @param highlightPost
	 * @param hightlightFragsize
	 * @throws Exception
	 */
	public static void publishProfileKeywordNotificationEvent(List<SolrInputDocument> docs, String highlightPre, String highlightPost,
																int hightlightFragsize) throws Exception {
		if (null == docs || docs.isEmpty())
			return;

		DashboardProfileRepository dashboardProfileRepo = App.getInstance().getContext().getBean(DashboardProfileRepository.class);
		List<DashboardProfile> profiles = dashboardProfileRepo.findByEnabled(true);
		List<ProfileTrend> profileTrendList = analysis(docs, profiles, highlightPre, highlightPost, hightlightFragsize);

		if (null == profileTrendList || profileTrendList.isEmpty())
			return;

		String msg = JsonUtil.getMapper().writeValueAsString(profileTrendList);
		RedisUtil.getFrontendInstance().publish(ChannelDef.CHANNEL_KEYWORD_NOTIFICATION, msg);
		log.info("Publish keyword notification event: {}", msg);
	}

	/**
	 * Analysis which profiles have new keyword feed coming.
	 * 
	 * @param docs
	 * @param profiles
	 * @param highlightPre
	 * @param highlightPost
	 * @param hightlightFragsize
	 * @return
	 */
	public static List<ProfileTrend> analysis(List<SolrInputDocument> docs, List<DashboardProfile> profiles, String highlightPre,
												String highlightPost, int hightlightFragsize) {
		if (null == docs || docs.isEmpty() || null == profiles || profiles.isEmpty())
			return null;

		// Prepare profile to session ids by batch.
		Map<Long, Set<String>> profileId2SessionIds = new HashMap<Long, Set<String>>();
		for (DashboardProfile profile : profiles) {
			long profileId = profile.getId();

			// Get session ids who are viewing current profile.
			Set<String> sessionIds = getSessionIds(profile);
			log.debug("profileId: {}, sessionIds: {}", profileId, sessionIds);

			if (null != sessionIds && !sessionIds.isEmpty()) {
				profileId2SessionIds.put(profileId, sessionIds);
			}
		}

		// Check documents whether containing keywords in each profile in order to trigger keyword feed coming notification.
		List<ProfileTrend> profileTrendList = new ArrayList<ProfileTrend>();
		for (SolrInputDocument doc : docs) {
			// Skip websocket notification for document with multiple contents
			Object contentObj = doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
			if (contentObj instanceof Collection) {
				@SuppressWarnings("unchecked")
				Collection<String> contents = (Collection<String>) contentObj;
				if (contents.size() > 1)
					continue;
			}

			Object value = doc.getFieldValue(SolrFieldDefinition.TITLE.getName());
			String title = null;
			if (null != value && value instanceof String)
				title = (String) value;

			value = doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
			String content = null;
			if (null != value) {
				if (value instanceof String) {
					content = (String) value;
				} else if (value instanceof Collection) {
					Collection<String> col = (Collection<String>) value;
					if (null != col && !col.isEmpty()) {
						// StringBuilder sb = new StringBuilder();
						// for (String str : col)
						// sb.append(str);

						content = StringUtils.join(col, "<BR>");
					}
				}
			}

			for (DashboardProfile profile : profiles) {
				long profileId = profile.getId();

				// Get session ids who are viewing current profile.
				Set<String> sessionIds = profileId2SessionIds.get(profileId);

				if (null == sessionIds || sessionIds.isEmpty()) { // No session for the profile, ignore pushing notification.
					continue;
				}

				Set<String> keywords = profile.getKeywords();
				Set<String> finalKeywords = new HashSet<String>();
				for (String keyword : keywords) {
					finalKeywords.add(ChineseTranslator.getInstance().simpl2Trad(keyword));
					finalKeywords.add(ChineseTranslator.getInstance().trad2Simpl(keyword));
				}

				// Check whether title contains keywords.
				String highlight = highlight(finalKeywords, title, highlightPre, highlightPost);

				// If title doesn't contains any keywords, check whether content contain keywords.
				if (StringUtils.isEmpty(highlight))
					highlight = highlight(finalKeywords, content, highlightPre, highlightPost);

				// If highlight is not empty, means title or content contain keywords.
				if (!StringUtils.isEmpty(highlight)) {
					ProfileTrend profileTrend = getProfileTrend(profileTrendList, profileId);
					if (null == profileTrend) {
						profileTrend = new ProfileTrend(profileId);
						profileTrend.addSessionIds(sessionIds);
						profileTrendList.add(profileTrend);
					}

					profileTrend.addFeed(solrInputDocument2Feed(doc, finalKeywords, null, null, hightlightFragsize));

				}
			}
		}

		// Sort feeds by publish date desc.
		sort(profileTrendList);

		return profileTrendList;

	}

	public static ProfileTrend getProfileTrend(List<ProfileTrend> profileTrendList, long profileId) {
		if (null != profileTrendList && !profileTrendList.isEmpty()) {
			for (ProfileTrend profileTrend : profileTrendList)
				if (profileTrend.getProfileId() == profileId)
					return profileTrend;
		}

		return null;
	}

	/**
	 * Get session id whose user own current profile.
	 * 
	 * @param profile
	 * @return
	 */
	public static Set<String> getSessionIds(DashboardProfile profile) {
		if (null == profile)
			return null;

		Set<String> sessionIds = new HashSet<String>();
		String wsId = AuthenUtil.getWebsocketId(profile.getUserId());

		log.debug("userId: {}, wsId: {}", profile.getUserId(), wsId);
		if (!StringUtils.isEmpty(wsId))
			sessionIds.add(wsId);

		return sessionIds;
	}

	/**
	 * High light keywords in data, if data doesn't contain any keywords return null.
	 * 
	 * @param keywords
	 * @param data
	 * @param highlightPre
	 * @param highlightPost
	 * @return
	 */
	public static String highlight(Set<String> keywords, String data, String highlightPre, String highlightPost) {
		if (null == keywords || keywords.isEmpty() || StringUtils.isEmpty(data))
			return null;

		if (StringUtils.isEmpty(highlightPre) || StringUtils.isEmpty(highlightPost)) {
			highlightPre = "<em>";
			highlightPost = "</em>";
		}

		String highlight = data;

		for (String keyword : keywords) {
			highlight = StringUtils.replace(highlight, keyword, highlightPre + keyword + highlightPost);
		}

		return highlight.length() > data.length() ? highlight : null;
	}

	/**
	 * Sort profile feeds by publish date desc.
	 * 
	 * @param profileTrendList
	 * @return
	 */
	public static List<ProfileTrend> sort(List<ProfileTrend> profileTrendList) {
		if (null != profileTrendList) {
			for (ProfileTrend profileTrend : profileTrendList) {
				List<Feed> feeds = profileTrend.getFeeds();
				Collections.sort(feeds, new Comparator<Feed>() {
					@Override
					public int compare(Feed o1, Feed o2) {
						Date d1 = o1.getpDate();
						Date d2 = o2.getpDate();
						if (null == d1 && null != d2) {
							return 1;
						} else if (null != d1 && null == d2) {
							return -1;
						} else if (null != d1 && null != d2) {
							return d2.compareTo(d1);
						}

						return 0;
					}
				});
			}
		}

		return profileTrendList;
	}

	public static Feed solrInputDocument2Feed(SolrInputDocument doc, Set<String> keywords, String highlightPre, String highlightPost,
												int hightlightFragsize) {
		if (null == doc)
			return null;

		if (hightlightFragsize <= 0)
			hightlightFragsize = 200;

		String url = null;
		Object fieldValue = doc.getFieldValue(SolrFieldDefinition.URL.getName());
		if (null != fieldValue && fieldValue instanceof String) {
			url = (String) fieldValue;
		}

		Feed feed = new Feed();
		feed.setUrl(url);

		// Get publish date.
		fieldValue = doc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
		if (null != fieldValue && fieldValue instanceof Date) {
			feed.setpDate((Date) fieldValue);
		}

		// Get source type name.
		fieldValue = doc.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());
		String sourceTypeName = null;
		if (null != fieldValue && fieldValue instanceof Integer) {
			sourceTypeName = social.hunt.common.definition.SourceType.getSourceTypeName((int) fieldValue);
		}

		feed.setSourceType(sourceTypeName);

		// Get source domain.
		fieldValue = doc.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName());
		String sourceDomain = null;
		if (null != fieldValue && fieldValue instanceof Integer) {
			Sns sns = Sns.getSns((int) fieldValue);
			if (null != sns)
				sourceDomain = sns.getNameEn();
		}

		if (StringUtils.isEmpty(sourceDomain)) {
			sourceDomain = UrlUtil.extractDomain(url);
		}

		feed.setSourceDomain(sourceDomain);

		// Get title.
		String title = null;
		fieldValue = doc.getFieldValue(SolrFieldDefinition.TITLE.getName());
		if (null != fieldValue && fieldValue instanceof String) {
			title = (String) fieldValue;
			String highlight = highlight(keywords, title, highlightPre, highlightPost);
			title = StringUtils.isEmpty(highlight) ? title : highlight;

			if (!StringUtils.isEmpty(title) && StringUtils.length(title) > hightlightFragsize) {
				title = StringUtils.substringBeforeLast(title, highlightPost) + highlightPost + "...";
			}
		}

		feed.setTitle(title);

		// Get content.
		String content = null;
		fieldValue = doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
		if (null != fieldValue) {
			if (fieldValue instanceof String) {
				content = (String) fieldValue;
			} else if (fieldValue instanceof Collection) {
				Collection<String> col = (Collection<String>) fieldValue;
				if (null != col && !col.isEmpty()) {
					StringBuilder sb = new StringBuilder();
					for (String str : col)
						sb.append(str);

					content = sb.toString();
				}
			}

			String highlight = highlight(keywords, content, highlightPre, highlightPost);
			content = StringUtils.isEmpty(highlight) ? content : highlight;

			if (!StringUtils.isEmpty(content) && StringUtils.length(content) > hightlightFragsize) {
				content = StringUtils.substringBeforeLast(content, highlightPost) + highlightPost + "...";
			}
		}

		feed.setContent(content);

		// Get photo.
		String photo = null;
		fieldValue = doc.getFieldValue(SolrFieldDefinition.IMAGES.getName());
		if (null != fieldValue) {
			if (fieldValue instanceof Collection) {
				Collection<String> col = (Collection<String>) fieldValue;
				if (null != col && !col.isEmpty()) {
					for (String img : col) {
						photo = img;
						break;
					}
				}
			} else if (fieldValue instanceof String) {
				photo = (String) fieldValue;
			}

		}

		if (null == photo) {
			fieldValue = doc.getFieldValue(SolrFieldDefinition.PRIMARY_IMAGE.getName());
			if (null != fieldValue && fieldValue instanceof String) {
				photo = (String) fieldValue;
			}
		}

		feed.setPhoto(photo);

		// Get views.
		Long views = null;
		fieldValue = doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
		if (null != fieldValue && fieldValue instanceof Long) {
			views = (Long) fieldValue;
		}
		feed.setViews(views);

		// Get likes.
		Long likes = null;
		fieldValue = doc.getFieldValue(SolrFieldDefinition.LIKE_COUNT.getName());
		if (null != fieldValue && fieldValue instanceof Long) {
			likes = (Long) fieldValue;
		}
		feed.setLikes(likes);

		// Get comments.
		Long comments = null;
		fieldValue = doc.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
		if (null != fieldValue && fieldValue instanceof Long) {
			comments = (Long) fieldValue;
		}
		feed.setComments(comments);

		// Get shares.
		Long shares = null;
		fieldValue = doc.getFieldValue(SolrFieldDefinition.SHARE_COUNT.getName());
		if (null != fieldValue && fieldValue instanceof Long) {
			shares = (Long) fieldValue;
		}
		feed.setShares(shares);

		// Get dislikes.
		Long dislikes = null;
		fieldValue = doc.getFieldValue(SolrFieldDefinition.DISLIKE_COUNT.getName());
		if (null != fieldValue && fieldValue instanceof Long) {
			dislikes = (Long) fieldValue;
		}
		feed.setDislikes(dislikes);

		return feed;

	}

	public static class ProfileTrend {
		private long profileId;

		private List<String> sessionIds;

		private List<Feed> feeds;

		public ProfileTrend(long profileId) {
			super();
			this.profileId = profileId;
			this.sessionIds = new ArrayList<String>();
			this.feeds = new ArrayList<Feed>();
		}

		public long getProfileId() {
			return profileId;
		}

		public void setProfileId(long profileId) {
			this.profileId = profileId;
		}

		public List<String> getSessionIds() {
			return sessionIds;
		}

		public void setSessionIds(List<String> sessionIds) {
			this.sessionIds = sessionIds;
		}

		public List<Feed> getFeeds() {
			return feeds;
		}

		public void setFeeds(List<Feed> feeds) {
			this.feeds = feeds;
		}

		public void addSessionId(String sessionId) {
			if (StringUtils.isEmpty(sessionId))
				return;

			if (null == this.sessionIds)
				this.sessionIds = new ArrayList<String>();

			this.sessionIds.add(sessionId);

		}

		public void addSessionIds(Collection<String> sessionIds) {
			if (null == sessionIds || sessionIds.isEmpty())
				return;

			if (null == this.sessionIds)
				this.sessionIds = new ArrayList<String>();

			this.sessionIds.addAll(sessionIds);

		}

		public void addFeed(Feed feed) {
			if (null == feeds)
				feeds = new ArrayList<Feed>();

			feeds.add(feed);

		}

		public void addFeeds(Collection<Feed> feeds) {
			if (null == feeds || feeds.isEmpty())
				return;

			if (null == this.feeds)
				this.feeds = new ArrayList<Feed>();

			this.feeds.addAll(feeds);

		}

	}

}
