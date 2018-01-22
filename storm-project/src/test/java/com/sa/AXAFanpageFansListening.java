package com.sa;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.common.json.JsonUtil;
import com.sa.crawler.definition.FacebookToken;
import com.sa.graph.ch.object.JobInfo;
import com.sa.storm.sns.domain.FBUser;
import com.sa.storm.sns.mobilesimulator.bolt.finder.CHMobileSimulatorFansFinderParserBolt;
import com.sa.storm.sns.util.ExcelUtil;
import com.sa.storm.sns.util.ExcelUtil.ExcelRow;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;

public class AXAFanpageFansListening {

	private static final Logger log = LoggerFactory.getLogger(AXAFanpageFansListening.class);
	private static int MaxSize = 20000;
	private static int FreeMaxSize = 100;
	private static List<ExcelRow> header;
	private static ExcelUtil util;
	private static String ExcelSheet = "FansList";
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public static void main(String[] args) throws IOException, JSONException {
		header = initalHeader();
		util = ExcelUtil.getInstance();
		String dateStr = DATE_FORMAT.format(new Date());
		String pageName = "AXAHK.LouisWong";
		String fileName = dateStr + "-Fans-of-" + pageName.replace(" ", "") + ".xls";
		String directory = "D://report/";
		String path = directory + fileName;
		String pageId = "991926544155336";
		String[] genders = { "males", "females" };
		long totalFanNum =0;
		for (String gender : genders) {
			
				totalFanNum += fansParser(path, pageId, pageName, gender, dateStr);

		}

	}

	private static List<ExcelRow> initalHeader() {
		List<ExcelRow> header = new ArrayList<ExcelRow>();
		Map<Integer, String> cell2Value = new HashMap<Integer, String>();
		ExcelRow excelRow = new ExcelRow();
		cell2Value = new HashMap<Integer, String>();
		cell2Value.put(0, "id");
		cell2Value.put(1, "name");
		cell2Value.put(2, "gender");
		cell2Value.put(3, "avatar");
		cell2Value.put(4, "pageLink");
		cell2Value.put(5, "location");
		cell2Value.put(6, "homeTown");
		cell2Value.put(7, "work");
		cell2Value.put(8, "education");
		cell2Value.put(9, "birthday");
		cell2Value.put(10, "realtion");
		excelRow.setCell2Value(cell2Value);
		header.add(excelRow);
		return header;
	}

	private static long fansParser(String path, String pageId, String pageName, String gender, String dateStr) throws IOException,
			JSONException {
		String nextPageUrl = "";
		String end_cursor = "";
		long userCount = 0;
		while (null != nextPageUrl) {
			String json = submitRequest(end_cursor, pageId, gender);
			try {
				JSONObject jsonObj = new JSONObject(json);
				JSONArray userObjs = jsonObj.getJSONObject("intersect(" + gender + "(),likers(" + pageId + "))").getJSONObject("results")
						.optJSONArray("edges");
				JSONObject pagging = jsonObj.getJSONObject("intersect(" + gender + "(),likers(" + pageId + "))").getJSONObject("results")
						.optJSONObject("page_info");

				if (pagging.getString("has_next_page") == "true") {
					int userLength = userObjs.length();
					if (userLength == 0) {
						break;
					}
					userCount += userLength;
					for (int i = 0; i < userObjs.length(); i++) {
						JSONObject user = userObjs.getJSONObject(i);
						JSONObject node = user.getJSONObject("node");
						String id = node.getString("id");
						String name = node.getString("name");
						String avatar = node.optJSONObject("profilePicture74").optString("uri");
						addRow(id, name, gender, avatar, path);
					}

					log.info(pageName + " current conut: {},gender {}", userCount, gender);

					end_cursor = pagging.getString("end_cursor");
				} else {
					nextPageUrl = null;
				}

				if (userCount > MaxSize) {
					break;
				}
			} catch (Exception ex) {
				log.error("ex:{}", ex.toString());
				break;
			}
		}

		return userCount;
	}

	private static String submitRequest(String end_cursor, String pageId, String gender) throws UnsupportedEncodingException {

		String query_params = "{\"generic_attachment_small_square_image_dimension\":\"100\",\"scale\":2,\"queryString\":\"intersect("
				+ gender
				+ "(),likers("
				+ pageId
				+ "))\",\"device\":\"iphone\",\"streaming_image_resolution\":960,\"showInterestingComment\":\"false\",\"generic_attachment_fallback_square_image_dimension\":\"224\",\"generic_attachment_tall_cover_image_height\":\"292\",\"media_type\":\"image\\/jpeg\",\"count\":6,\"render_actor_in_gutter\":\"false\",\"generic_attachment_small_cover_image_width\":\"88\",\"generic_attachment_portrait_image_height\":\"336\",\"feedback_include_seen_by\":\"true\",\"generic_attachment_tall_cover_image_width\":\"560\",\"enable_attachments_redesign\":\"false\",\"generic_attachment_large_square_image_dimension\":\"560\",\"render_location\":\"IOS_TIMELINE\",\"generic_attachment_small_cover_image_height\":\"88\",\"generic_attachment_portrait_image_width\":\"224\",\"taggableUserIncludeByLines\":\"false\"}";
		if (end_cursor != "") {

			query_params = "{\"generic_attachment_small_square_image_dimension\":\"100\",\"scale\":2,\"queryString\":\"intersect("
					+ gender
					+ "(),likers("
					+ pageId
					+ "))\",\"device\":\"iphone\",\"streaming_image_resolution\":960,\"showInterestingComment\":\"false\",\"generic_attachment_fallback_square_image_dimension\":\"224\",\"generic_attachment_tall_cover_image_height\":\"292\",\"media_type\":\"image\\/jpeg\",\"count\":6,\"render_actor_in_gutter\":\"false\",\"generic_attachment_small_cover_image_width\":\"88\",\"generic_attachment_portrait_image_height\":\"336\",\"feedback_include_seen_by\":\"true\",\"generic_attachment_tall_cover_image_width\":\"560\",\"enable_attachments_redesign\":\"false\",\"generic_attachment_large_square_image_dimension\":\"560\",\"render_location\":\"IOS_TIMELINE\",\"generic_attachment_small_cover_image_height\":\"88\",\"generic_attachment_portrait_image_width\":\"224\",\"taggableUserIncludeByLines\":\"false\",\"afterCursor\":\""
					+ end_cursor + "\"}";
		}
		String url = "https://chmobile.facebook.com/graphql/?query_id=10152526044563380&sdk=ios&sdk_version=3&fb_api_caller_class=FBGraphQLService&format=json&app_version=6017145&query_params="
				+ URLEncoder.encode(query_params, "utf-8")
				+ "&method=get&locale=en_US&fb_api_req_friendly_name=FBGraphSearchUserQuery&pretty=0";
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
		return json;
	}

	private static void addRow(String id, String name, String gender, String avatar, String path) throws Exception {

		FBUser user = parserUser(id);
		List<ExcelRow> content = new ArrayList<ExcelRow>();
		ExcelRow excelRow = new ExcelRow();
		excelRow.add(0, id);
		excelRow.add(1, name);
		excelRow.add(2, gender);
		excelRow.add(3, avatar);
		excelRow.add(4, "http://www.facebook.com/" + user.getUserId());
		excelRow.add(5, user.getLocation());
		excelRow.add(6, user.getHomeTown());
		excelRow.add(7, user.getWork());
		excelRow.add(8, user.getCollege());
		excelRow.add(9, user.getBirthday());
		excelRow.add(10, user.getRelation());
		content.add(excelRow);
		util.saveExcel(path, "FansList", false, true, header, content);
	}

	private static FBUser parserUser(String id) throws JSONException {
		FBUser p = new FBUser();
		try {
			String json = submitSingleUserRequest(id);
			JSONObject userInfo = new JSONObject(json);
			p.setUserId(id);
			String name = userInfo.optString("name");
			p.setName(name);
			String username = userInfo.optString("username");
			p.setUserName(username);

			String gender = null;
			if (null != userInfo.optString("gender")) {
				gender = userInfo.optString("gender");
				p.setGender(gender);
			}
			String hometown = null;
			if (null != userInfo.optJSONObject("hometown")) {
				hometown = userInfo.optJSONObject("hometown").optString("name");
				p.setHomeTown(hometown);
			}

			String location = null;
			if (null != userInfo.optJSONObject("location")) {
				location = userInfo.optJSONObject("location").optString("name");
				p.setLocation(location);
			}

			String schoolName = null;
			if (null != userInfo.optJSONArray("education")) {
				JSONArray educations = userInfo.optJSONArray("education");
				JSONObject school = userInfo.optJSONArray("education").getJSONObject(educations.length() - 1);
				schoolName = school.optJSONObject("school").optString("name");
				p.setCollege(schoolName);
			}
			String birthday = userInfo.optString("birthday");
			p.setBirthday(birthday);

			String relation = userInfo.optString("relationship_status");
			if (null != relation || "" != relation) {
				p.setRelation(relation);
			}
			String phone = userInfo.optString("mobile_phone");
			if (null != phone && "" != phone) {
				p.setPhone(phone);
			}

			JSONObject address = userInfo.optJSONObject("address");
			if (null != address) {
				p.setAddress(address.optString("street"));
			}

			JSONArray works = userInfo.optJSONArray("work");
			if (null != works) {
				JSONObject work = works.optJSONObject(0);
				JobInfo jobInfo = new JobInfo();
				if (null != work.optJSONObject("employer")) {
					jobInfo.setCompanyName(work.optJSONObject("employer").optString("name"));
				}
				if (null != work.optJSONObject("location")) {
					jobInfo.setCity(work.optJSONObject("location").optString("name"));
				}
				if (null != work.optJSONObject("position")) {
					jobInfo.setPosition(work.optJSONObject("position").optString("name"));
				}
				try {
					String JobInfoJson = JsonUtil.getMapper().writeValueAsString(jobInfo);
					p.setWork(JobInfoJson);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception ex) {
			log.error("error {}", ex.toString());
			ex.printStackTrace();
		}

		return p;

	}

	private static String submitSingleUserRequest(String userId) {
		String url = "https://graph.facebook.com/" + userId + "?access_token=" + FacebookToken.MobileApiToken.USA_MOBILE_TOKEN;
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
		return json;
	}

}
