/**
 *Copyright(C) ©2012 Social Hunter. All rights reserved.
 *
 */
package com.sa.storm.sns.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.competitorhunter.domain.Page;
import com.sa.crawler.definition.FacebookToken;
import com.sa.storm.ca.domain.ImgInfo;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;

/**
 * @description
 * 
 * @author                            Luke
 * @created date                      2015-2-6 
 * @modification history<BR>
 * No.        Date          Modified By             <B>Why & What</B> is modified  
 *
 * @see                               
 */
public class FacebookMobileApiUtil {
	
	private static final Logger log = LoggerFactory.getLogger(FacebookMobileApiUtil.class);
	
	private FacebookApiRequestUtil facebookApiRequestUtil;
	
	private FacebookMobileApiUtil() {
		facebookApiRequestUtil = FacebookApiRequestUtil.getInstance();
	}
	
	private static class Holder {
		public static FacebookMobileApiUtil instance;
		static {
			try {
				instance = new FacebookMobileApiUtil();
			} catch (Exception e) {
				log.error("Error in initialization", e);
			}
		}
	}

	public static FacebookMobileApiUtil getInstance() {
		return Holder.instance;
	}
	
	public String submitRequest(String url) {
		if (StringUtils.isEmpty(url))
			return null;
		
		return facebookApiRequestUtil.submitRequest(url);
	}

	public Set<String> getUserIds(String pageId, int maxUserCount) {
		String endCursor = "";
		String queryStr = bulidQueryStr(pageId, "males");
		Set<String> userIdSet = new HashSet<String>();
		boolean hasNextPage = true;
		int userCount = 0;
		
		do {
			try {
				String json = facebookApiRequestUtil.submitMobileSimulatorFansListRequest(endCursor, queryStr);
				
				log.debug("json: {} ", json);
				if (StringUtils.isEmpty(json)) {
					log.error("json is empty, queryStr: {}", queryStr);
					return userIdSet;
				}

				JSONObject jsonObj = new JSONObject(json);
				JSONArray userObjs = jsonObj.getJSONObject(queryStr).getJSONObject("results").optJSONArray("edges");
				JSONObject paging = jsonObj.getJSONObject(queryStr).getJSONObject("results").optJSONObject("page_info");

				for (int i = 0; i < userObjs.length(); ++i) {
					JSONObject user = userObjs.getJSONObject(i);					
					JSONObject node = user.getJSONObject("node");
					String id = node.optString("id");
					if (!StringUtils.isEmpty(id)) {
						userIdSet.add(id);
						++userCount;
					}
					
					if (0 == userCount % 50)
						log.info("pageId: {}, userCount: {}", pageId, userCount);
					
					if (userCount >= maxUserCount)
						break;

				}

				if ("true".equals(paging.optString("has_next_page"))) {
					endCursor = paging.getString("end_cursor");
				} else {
					hasNextPage = false;
				}

			} catch (Exception e) {
				log.error(e.getMessage(), e);
				break;
			}
		} while (hasNextPage && userCount < maxUserCount);

		return userIdSet;

	}

	public void parseAndStatistic(String userId, int maxLikePageCount, Map<String, Long> category2Count, Map<String, Long> pageId2Count) {
		if (StringUtils.isEmpty(userId))
			return;

		if (null == category2Count || null == pageId2Count) {
			log.error("Need to give two map object");
			return;
		}

		Map<String, Long> categoryIncluded = new HashMap<String, Long>(); // Each fans just calculate the same category once.

		String url = "https://graph.facebook.com/" + userId + "/likes?access_token="
				+ FacebookToken.MobileApiToken.HK_MOBILE_TOKEN;
		boolean hasNextPage = true;
		int likePageCount = 0;

		do {
			try {
				String json = facebookApiRequestUtil.submitMobileSimulatorRequest(url, null);
				
				log.debug("json: {} ", json);
				if (StringUtils.isEmpty(json)) {
					log.error("json is empty, url: {}", url);
					return;
				}

				JSONObject jsonObj = new JSONObject(json);
				JSONArray data = jsonObj.optJSONArray("data");

				for (int i = 0; i < data.length(); ++i) {
					JSONObject page = data.getJSONObject(i);
					
					String pageId = page.optString("id");
					if (StringUtils.isEmpty(pageId))
						continue;

					String pageName = page.optString("name");
					if (StringUtils.isEmpty(pageName))
						continue;

					String category = page.optString("category");
					if (StringUtils.isEmpty(category))
						continue;

					/* Statistic category. */
					if (null == categoryIncluded.get(category)) {
						Long count = category2Count.get(category);
						if (null == count)
							count = 0l;
						
						++count;
						category2Count.put(category, count);

						categoryIncluded.put(category, 1l);
					}

					/* Statistic like page. */
					Long count = pageId2Count.get(pageId);
					if (null == count)
						count = 0l;

					++count;
					pageId2Count.put(pageId, count);
					
					++likePageCount;
					
					if (0 == likePageCount % 50)
						log.info("userId: {}, likePageCount: {}", userId, likePageCount);
					
					if (likePageCount >= maxLikePageCount)
						break;

				}

				JSONObject paging = jsonObj.optJSONObject("paging");
				if (null != paging) {
					String nextPage = paging.optString("next");
					if (!StringUtils.isEmpty(nextPage)) {
						url = nextPage;
					} else {
						hasNextPage = false;
					}
				} else {
					hasNextPage = false;
				}

			} catch (Exception e) {
				log.error(e.getMessage(), e);
				break;
			}

		} while (hasNextPage && likePageCount < maxLikePageCount);

	}

	public Page getPageByMobileApi(String pageId) {
		StringBuilder requestUrlSb = new StringBuilder();

		requestUrlSb.append("https://graph.facebook.com/").append(pageId).append("?access_token=")
				.append(FacebookToken.MobileApiToken.HK_MOBILE_TOKEN);

		String json = facebookApiRequestUtil.submitMobileSimulatorRequest(requestUrlSb.toString(), null);
		if (StringUtils.isEmpty(json)) {
			requestUrlSb = new StringBuilder();
			requestUrlSb.append("https://graph.facebook.com/").append(pageId).append("?access_token=")
					.append(FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
			json = facebookApiRequestUtil.submitMobileSimulatorRequest(requestUrlSb.toString(), null);

			if (StringUtils.isEmpty(json)) {
				log.error("getPageByMobileApi error, no json return, request URL: ", requestUrlSb.toString());
				return null;
			}

		}

		return parseMobileSimulatorPageJson(json);
	}

	public ImgInfo getPageImgInfoByMobileApi(String pageId) {
		ImgInfo imgInfo = new ImgInfo();

		/* Get logo image. */
		StringBuilder requestUrlSb = new StringBuilder();
		requestUrlSb.append("https://graph.facebook.com/").append(pageId).append("/photos").append("?access_token=")
				.append(FacebookToken.MobileApiToken.HK_MOBILE_TOKEN);

		String json = facebookApiRequestUtil.submitMobileSimulatorRequest(requestUrlSb.toString(), null);
		if (StringUtils.isEmpty(json)) {
			requestUrlSb = new StringBuilder();
			requestUrlSb.append("https://graph.facebook.com/").append(pageId).append("/photos").append("?access_token=")
					.append(FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
			json = facebookApiRequestUtil.submitMobileSimulatorRequest(requestUrlSb.toString(), null);

			if (StringUtils.isEmpty(json)) {
				log.error("getPageByMobileApi error, no json return, request URL: ", requestUrlSb.toString());
			}

		}

		if (StringUtils.isEmpty(json)) {
			log.error("final no json return for logo image, pageId: {}", pageId);
		} else {
			imgInfo.setSmallImgUrl(parseLogoImg(json));
		}

		/* Get cover image. */
		requestUrlSb = new StringBuilder();
		requestUrlSb.append("https://graph.facebook.com/").append(pageId).append("?access_token=")
				.append(FacebookToken.MobileApiToken.HK_MOBILE_TOKEN);

		json = facebookApiRequestUtil.submitMobileSimulatorRequest(requestUrlSb.toString(), null);
		if (StringUtils.isEmpty(json)) {
			requestUrlSb = new StringBuilder();
			requestUrlSb.append("https://graph.facebook.com/").append(pageId).append("?access_token=")
					.append(FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
			json = facebookApiRequestUtil.submitMobileSimulatorRequest(requestUrlSb.toString(), null);

			if (StringUtils.isEmpty(json)) {
				log.error("getPageByMobileApi error, no json return, request URL: ", requestUrlSb.toString());
			}

		}

		if (StringUtils.isEmpty(json)) {
			log.error("final no json return for cover image, pageId: {}", pageId);
		} else {
			imgInfo.setLargeImgUrl(parseCoverImg(json));
		}

		/* Get share image. */
		requestUrlSb = new StringBuilder();
		requestUrlSb.append("https://graph.facebook.com/").append(pageId).append("/photos/uploaded?fields=picture")
				.append("&access_token=").append(FacebookToken.MobileApiToken.HK_MOBILE_TOKEN);

		json = facebookApiRequestUtil.submitMobileSimulatorRequest(requestUrlSb.toString(), null);
		if (StringUtils.isEmpty(json)) {
			requestUrlSb = new StringBuilder();
			requestUrlSb.append("https://graph.facebook.com/").append(pageId).append("/photos/uploaded?fields=picture")
					.append("&access_token=").append(FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
			json = facebookApiRequestUtil.submitMobileSimulatorRequest(requestUrlSb.toString(), null);

			if (StringUtils.isEmpty(json)) {
				log.error("getPageByMobileApi error, no json return, request URL: ", requestUrlSb.toString());
			}

		}

		if (StringUtils.isEmpty(json)) {
			log.error("final no json return for share image, pageId: {}", pageId);
		} else {
			imgInfo.setShareImgUrls(parseShareImg(json));
		}

		return imgInfo;
	}

	private Page parseMobileSimulatorPageJson(String json) {
		try {
			JSONObject pageObj = new JSONObject(json);

			if (pageObj.has("error")) {
				log.error("json error: {}", pageObj.opt("error"));
				return null;
			}

			Page page = new Page();
			String name = pageObj.optString("name");
			String category = pageObj.optString("category");
			String link = pageObj.optString("link");

			page.setPageName(name);
			page.setCategory(category);
			page.setPageLink(link);

			return page;
		} catch (JSONException e) {
			log.error("parseMobileSimulatorPageJson exception", e);
			e.printStackTrace();

		}

		return null;

	}

	
	private String parseLogoImg(String json) {
		String img = null;
		try {
			JSONObject jsonObj = new JSONObject(json);
			JSONArray data = jsonObj.optJSONArray("data");

			if (null != data && data.length() > 0) {
				JSONObject first = data.getJSONObject(0);
				String picture = first.optString("picture");
				if (!StringUtils.isEmpty(picture))
					img = picture;
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}

		return img;
	}

	private String parseCoverImg(String json) {
		String img = null;
		try {
			JSONObject jsonObj = new JSONObject(json);
			JSONObject cover = jsonObj.optJSONObject("cover");

			if (null != cover) {
				String source = cover.optString("source");
				if (!StringUtils.isEmpty(source))
					img = source;
			}

		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}

		return img;
	}

	private List<String> parseShareImg(String json) {
		List<String> imgs = null;
		JSONObject item;
		String picture;
		try {
			JSONObject jsonObj = new JSONObject(json);
			JSONArray data = jsonObj.optJSONArray("data");

			if (null != data) {
				for (int i = 0; i < data.length() && i < 3; ++i) {
					item = data.getJSONObject(i);
					picture = item.optString("picture");
					if (!StringUtils.isEmpty(picture)) {
						if (null == imgs)
							imgs = new ArrayList<String>();
						imgs.add(picture);
					}
				}
			}

		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}

		return imgs;
	}
	

	private String bulidQueryStr(String pageId, String gender) {
		return "intersect(" + gender + "(),likers(" + pageId + "))";
	}


	public static void  main(String[] args) {
		FacebookMobileApiUtil util = FacebookMobileApiUtil.getInstance();
		//util.getUserIds("22092443056", 100);
		Page p = util.getPageByMobileApi("15087023444");
		log.info(p.toString());
		
	}
}
