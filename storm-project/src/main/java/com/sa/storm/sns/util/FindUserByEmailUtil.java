package com.sa.storm.sns.util;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.crawler.definition.FacebookToken;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;

public class FindUserByEmailUtil {

	private HashMap<String, String> userIdAndAvatar = new HashMap<String, String>();

	private String token = "CAAAAAYsX7TsBAOClsc3jz1W3IPEsfEXZAyLbFD2ClmXI20gnJQ8zX08vNe4cpqRX2hHGZCK6IQtdO5ZB2BXFq2wbVMU0ozZAeIjMZAF1ip7pZBQPiqDU5BhCo163WvHXr1qZAwagrOdtGMs4w7wr83awcOeY3hQlx1IHDnDvmZBZCSqydZAzWKf9I85MmIAuHSK6GH4fJxOEx0ZCytl8I2dCDtAqJSlwAm81zy5lsj6GFw2mCeiZCMs72uap";
	private String tokens[] = {
			"CAAAAAYsX7TsBABu1P13f1tF5rZAIDFBq9XnBxMnbu0yZCONMUOMIzCl516QVrBN8cCHAO10hf0ZCZCa53xxhPlGMOt1hMPIOrrgZAWZBDuJbLceGU45zYJW0inY5Pj1NyDg1zIxJnzF5HwdqIZAdZBbZC3c7qcRGu0F8CT0ZCzszZB3NI3xxOppfreEuazdQQR5HujT4aw5BKyHEGE86Y5wbfFOMgCR7V1exnqYGDkytzNM6yQczpIiU5iU",
			"CAAAAAYsX7TsBAJIesCSsqZCntFfaBiQOxfxAHbIrE5vi8ETEH9pdG08E1Azsu8bsNb8YJpZB2rs1ZBetO3IBcKb3vBZCAZB6UlKlAtvwAuMQpianJu0X3HAmc7PFJ89yQbgoJZAl0kxBbzYxETlnkzB7KFiegKmZCTUXcKU1L6xQ6jaSRO7GZBIE43o0266aWTZByWo4RGgmjmuoB9AF1RSKgVgR1jTYFneoSryrOYopjjRwfnR9tZBhtv",
			"CAAAAAYsX7TsBAGPyPRpulOuOAHMpUUSX48TfIc8hIwQTW5T19M8TpDYky2uh1SPe4y49YPNOjKZBt0clwHfCOjSTTcYC8HRKoK7THQTwu5g6pIsCLXOApLTZAI7viYgdfwd5Ldsnp0yQQ4QafHJzZC9bCwNHv6D5ZCH9ZC62VALhJxAEPwNJsF8o8YJVt1hGbdiKQFye72vYync3vdWZAkVv6KzK1SBBZCozNdrxsvBKGq7ZCzYIOCiZB",
			"CAAAAAYsX7TsBAMeGZBQFvCJtJi4ZAsZCteu3mH8ZADeSTiyi32VABUGK0HbEIpLrvFboY2kSA0GyY7oZA10e8A44wMGTpK3g8zJIcuDhAmXYwYAPqXodk3aLPsCw5d3te7jXEzvHVPiE3t0dMJXbT9ZAsO7UZB45VStWDXZAa4uQAtkURXvpZCINnU5XbRtbiboTgxSzo8W91ZA31LoMkZCvcDJa7RAvHJUoGr6j8xx9O6owi2CwnCIrhHY" };
	
	private static final Logger log = LoggerFactory.getLogger(FindUserByEmailUtil.class);

	public String randomMobileTokenToFindUser(String email, int count) {
		String uId = null;
		String queryStr = "keywords_top(" + email + ")";
		try {
			int tokenKey = count % 4;
			if (tokenKey >= 4) {
				tokenKey = 0;
			}
			String json = submitRequestForMobileToken(queryStr, tokenKey);
			if (null == json)
				return null;

			JSONObject jsonObj = new JSONObject(json);

			JSONArray userObjs = jsonObj.getJSONObject(queryStr.replace("@", "\u0040")).optJSONObject("filtered_query")
					.optJSONObject("modules").optJSONArray("edges");
			if (null != userObjs.optJSONObject(0)) {
				JSONObject node = userObjs.optJSONObject(0).optJSONObject("node").optJSONObject("results").optJSONArray("edges")
						.optJSONObject(0).optJSONObject("node");

				uId = node.optString("id");
				// String avatar = node.optJSONObject("profilePicture100").optString("uri");

			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return uId;
	}
	
	public String mobileTokenToFindUser(String email, String mobileToken) {
		String uId = null;
		String queryStr = "keywords_top(" + email + ")";
		try {
			String json = submitRequestForMobileToken(queryStr, mobileToken);
			if (null == json)
				return null;

			JSONObject jsonObj = new JSONObject(json);

			JSONArray userObjs = jsonObj.getJSONObject(queryStr.replace("@", "\u0040")).optJSONObject("filtered_query")
					.optJSONObject("modules").optJSONArray("edges");
			if (null != userObjs.optJSONObject(0)) {
				JSONObject node = userObjs.optJSONObject(0).optJSONObject("node").optJSONObject("results").optJSONArray("edges")
						.optJSONObject(0).optJSONObject("node");

				uId = node.optString("id");
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} 
		return uId;
	}

	public HashMap<String, String> HttpClientProxyCrawlerToFindUser(String email, String proxyHost, int proxyPort)
			throws UnsupportedEncodingException {
		String url = "https://www.facebook.com/search.php?q=" + URLEncoder.encode(email, "UTF-8");

		String json = FacebookApiRequestUtil.getInstance().submitSocksProxyRequest(url, proxyHost, proxyPort);
		Document mDocument = Jsoup.parse(json);

		String captcha_input = mDocument.select(".captcha_input").toString();
		if (null != captcha_input && "" == captcha_input.replace(" ", "")) {
			System.out.println("blocked");
		} else {
			String userUrl = mDocument.select(".clearfix").select("._8o ").attr("href");
			if (null != userUrl && "" != userUrl.replace(" ", "")) {
				String id = userUrl.substring(25);
				String avatar = mDocument.select(".clearfix").select("img").attr("src");
				userIdAndAvatar.put("id", id);
				userIdAndAvatar.put("avatar", avatar);
			}
		}
		return userIdAndAvatar;
	}

	public String HttpClientCrawlerToFindUser(String email) throws UnsupportedEncodingException {
		String uId = null;
		String url = "https://www.facebook.com/search.php?q=" + URLEncoder.encode(email, "UTF-8");

		String json = FacebookApiRequestUtil.getInstance().submitRequest(url);
		Document mDocument = Jsoup.parse(json);

		String captcha_input = mDocument.select(".captcha_input").toString();
		if (null != captcha_input && "" == captcha_input.replace(" ", "")) {
			log.info("the request has been block {}", email);
		} else {
			String userUrl = mDocument.select(".clearfix").select("._8o ").attr("href");
			if (null != userUrl && "" != userUrl.replace(" ", "")) {
				uId = userUrl.substring(25);

				if (null != uId) {
					String userJson = submitSingleUserRequest(uId);
					JSONObject userInfo;
					try {
						userInfo = new JSONObject(userJson);
						uId = userInfo.optString("id");
						log.info("find the FBUserId {} through {}", uId, email);
					} catch (JSONException e) {
						uId = null;
					}
				}
				// String avatar = mDocument.select(".clearfix").select("img").attr("src");

			}
		}
		return uId;
	}

	public HashMap<String, String> PythonCrawlerToFindUser(String email) {

		return userIdAndAvatar;
	}

	public String getUserIdByCookieRequest(String email) throws IOException, InterruptedException {
		String uId = null;
		List<String> cmd = new ArrayList<String>();
		cmd.add("wget");
		cmd.add("-O");
		cmd.add("/home/sa/wget/facebook.txt");
		cmd.add("--load-cookies");
		cmd.add("/home/sa/wget/cookies.txt");
		cmd.add("--user-agent=\"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3 (.NET CLR 3.5.30729)\"");
		cmd.add("https://www.facebook.com/search.php?q=" + URLEncoder.encode(email, "UTF-8"));
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);
		Process process = pb.start();

		int exitValue = process.waitFor();
		if (exitValue == 0) {
			File file = new File("/home/sa/wget/facebook.txt");
			List<String> list = FileUtils.readLines(file, "UTF-8");
			String result = list.toString();
			Document mDocument = Jsoup.parse(result);
			String userUrl = mDocument.select(".clearfix").select("._8o ").attr("href");
			if (null != userUrl && "" != userUrl.replace(" ", "")) {
				String userId = userUrl.substring(25).replace(" ", "");
				if (null != userId && "" != userId) {
					String userJson = submitSingleUserRequest(userId);
					JSONObject userInfo;
					try {
						userInfo = new JSONObject(userJson);
						userId = userInfo.optString("id");
						return userId;
					} catch (JSONException e) {
						e.printStackTrace();
					}

				}

			}
		}
		return null;
	}

	private String submitRequestForMobileToken(String queryStr, int tokenKey) throws UnsupportedEncodingException {
		String query_params = "{\"queryString\":\""
				+ queryStr
				+ "\",\"should_load_streaming_image\":\"false\",\"generic_attachment_large_square_image_dimension\":\"560\",\"render_actor_in_gutter\":\"false\",\"facts_count\":10,\"numBylines\":3,\"generic_attachment_small_cover_image_height\":\"88\",\"scale\":2,\"device\":\"iphone\",\"generic_attachment_tall_cover_image_height\":\"292\",\"is_zero_rated\":\"false\",\"formatType\":\"concise\",\"resultsCount\":1000,\"generic_attachment_small_cover_image_width\":\"88\",\"render_location\":\"IOS_TIMELINE\",\"generic_attachment_tall_cover_image_width\":\"560\",\"generic_attachment_portrait_image_width\":\"224\",\"showInterestingComment\":\"false\",\"enable_attachments_redesign\":\"false\",\"taggableUserIncludeByLines\":\"false\",\"enable_image_share_attachment\":\"false\",\"generic_attachment_fallback_square_image_dimension\":\"224\",\"count\":6,\"generic_attachment_portrait_image_height\":\"336\",\"exact_match\":\"false\",\"story_reaction_surface\":\"ios_story\",\"videoThumbnailSize\":720,\"feedback_include_seen_by\":\"true\",\"media_type\":\"image/jpeg\",\"showInlineComments\":\"false\",\"renderLocation\":\"feed_mobile\",\"streaming_image_resolution\":960,\"vertical\":\"content\",\"generic_attachment_small_square_image_dimension\":\"100\"}";
		String url = "https://chmobile.facebook.com/graphql/?query_id=10152757044853380&sdk=ios&sdk_version=3&fb_api_caller_class=FBGraphQLService&format=json&app_version=6017145&query_params="
				+ URLEncoder.encode(query_params, "utf-8")
				+ "&method=get&locale=en_US&fb_api_req_friendly_name=FBGraphSearchUserQuery&pretty=0";
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, tokens[tokenKey]);
		return json;
	}
	
	private String submitRequestForMobileToken(String queryStr, String token) throws UnsupportedEncodingException {
		String query_params = "{\"queryString\":\""
				+ queryStr
				+ "\",\"should_load_streaming_image\":\"false\",\"generic_attachment_large_square_image_dimension\":\"560\",\"render_actor_in_gutter\":\"false\",\"facts_count\":10,\"numBylines\":3,\"generic_attachment_small_cover_image_height\":\"88\",\"scale\":2,\"device\":\"iphone\",\"generic_attachment_tall_cover_image_height\":\"292\",\"is_zero_rated\":\"false\",\"formatType\":\"concise\",\"resultsCount\":1000,\"generic_attachment_small_cover_image_width\":\"88\",\"render_location\":\"IOS_TIMELINE\",\"generic_attachment_tall_cover_image_width\":\"560\",\"generic_attachment_portrait_image_width\":\"224\",\"showInterestingComment\":\"false\",\"enable_attachments_redesign\":\"false\",\"taggableUserIncludeByLines\":\"false\",\"enable_image_share_attachment\":\"false\",\"generic_attachment_fallback_square_image_dimension\":\"224\",\"count\":6,\"generic_attachment_portrait_image_height\":\"336\",\"exact_match\":\"false\",\"story_reaction_surface\":\"ios_story\",\"videoThumbnailSize\":720,\"feedback_include_seen_by\":\"true\",\"media_type\":\"image/jpeg\",\"showInlineComments\":\"false\",\"renderLocation\":\"feed_mobile\",\"streaming_image_resolution\":960,\"vertical\":\"content\",\"generic_attachment_small_square_image_dimension\":\"100\"}";
		String url = "https://chmobile.facebook.com/graphql/?query_id=10152757044853380&sdk=ios&sdk_version=3&fb_api_caller_class=FBGraphQLService&format=json&app_version=6017145&query_params="
				+ URLEncoder.encode(query_params, "utf-8")
				+ "&method=get&locale=en_US&fb_api_req_friendly_name=FBGraphSearchUserQuery&pretty=0";
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, token);
		return json;
	}

	private String submitSingleUserRequest(String userId) {
		String url = "https://graph.facebook.com/" + userId + "?access_token=" + FacebookToken.MobileApiToken.USA_MOBILE_TOKEN;
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, token);
		return json;
	}

}
