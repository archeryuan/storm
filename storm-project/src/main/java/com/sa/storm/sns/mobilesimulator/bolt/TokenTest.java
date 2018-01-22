package com.sa.storm.sns.mobilesimulator.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;

public class TokenTest {
	private static String pageId = "385923688151129";
	private static String liveIn = "113317605345751";
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

	public static void main(String[] args) throws JSONException, IOException {
		String nextPageUrl = "";
		String end_cursor = "";
		long userCount = 0;
		String dateStr = DATE_FORMAT.format(new Date());
		String filePath = "/home/sa/fb/" + dateStr + "-Fans-of-" + pageId.replace(" ", "") + ".csv";
		FileWriter fw = new FileWriter(filePath, true);
		PrintWriter out = new PrintWriter(fw);
		out.println("UserID,UserName,UserAvatar");
		while (null != nextPageUrl) {
			String json = submitRequest(end_cursor);
			// System.out.println(json);
			JSONObject jsonObj = new JSONObject(json);
			JSONArray userObjs = jsonObj.getJSONObject("likers(" + pageId + ")").getJSONObject("results").optJSONArray("edges");
			JSONObject pagging = jsonObj.getJSONObject("likers(" + pageId + ")").getJSONObject("results").optJSONObject("page_info");

			if (pagging.getString("has_next_page") == "true") {
				userCount += userObjs.length();

				for (int i = 0; i < userObjs.length(); i++) {
					JSONObject user = userObjs.getJSONObject(i);
					JSONObject node = user.getJSONObject("node");
					String id = node.getString("id");
					String name = node.getString("name");
					String avatar = node.optJSONObject("profilePicture74").optString("uri");
					out.println(id + "," + name + "," + avatar);
				}

				System.out.println(userCount + "  |  " + new Date());
				end_cursor = pagging.getString("end_cursor");
			} else {
				nextPageUrl = null;
			}
		}
		out.close();
		fw.close();
		System.out.println("total " + userCount);
	}

	private static String submitRequest(String end_cursor) throws UnsupportedEncodingException {

		String query_params = "{\"generic_attachment_small_square_image_dimension\":\"100\",\"scale\":2,\"queryString\":\"likers("
				+ pageId
				+ ")\",\"device\":\"iphone\",\"streaming_image_resolution\":960,\"showInterestingComment\":\"false\",\"generic_attachment_fallback_square_image_dimension\":\"224\",\"generic_attachment_tall_cover_image_height\":\"292\",\"media_type\":\"image\\/jpeg\",\"count\":10,\"render_actor_in_gutter\":\"false\",\"generic_attachment_small_cover_image_width\":\"88\",\"generic_attachment_portrait_image_height\":\"336\",\"feedback_include_seen_by\":\"true\",\"generic_attachment_tall_cover_image_width\":\"560\",\"enable_attachments_redesign\":\"false\",\"generic_attachment_large_square_image_dimension\":\"560\",\"render_location\":\"IOS_TIMELINE\",\"generic_attachment_small_cover_image_height\":\"88\",\"generic_attachment_portrait_image_width\":\"224\",\"taggableUserIncludeByLines\":\"false\"}";
		if (end_cursor != "") {
			System.out.println(end_cursor);
			query_params = "{\"generic_attachment_small_square_image_dimension\":\"100\",\"scale\":2,\"queryString\":\"likers("
					+ pageId
					+ ")\",\"device\":\"iphone\",\"streaming_image_resolution\":960,\"showInterestingComment\":\"false\",\"generic_attachment_fallback_square_image_dimension\":\"224\",\"generic_attachment_tall_cover_image_height\":\"292\",\"media_type\":\"image\\/jpeg\",\"count\":10,\"render_actor_in_gutter\":\"false\",\"generic_attachment_small_cover_image_width\":\"88\",\"generic_attachment_portrait_image_height\":\"336\",\"feedback_include_seen_by\":\"true\",\"generic_attachment_tall_cover_image_width\":\"560\",\"enable_attachments_redesign\":\"false\",\"generic_attachment_large_square_image_dimension\":\"560\",\"render_location\":\"IOS_TIMELINE\",\"generic_attachment_small_cover_image_height\":\"88\",\"generic_attachment_portrait_image_width\":\"224\",\"taggableUserIncludeByLines\":\"false\",\"afterCursor\":\""
					+ end_cursor + "\"}";
		}
		// "https:31.13.79.246/graphql/?query_id=10152526044563380&sdk=ios&sdk_version=3&fb_api_caller_class=FBGraphQLService&format=json&app_version=6017145&query_params="+query_params+"&method=get&locale=en_US&fb_api_req_friendly_name=FBGraphSearchUserQuery&pretty=0";
		String url = "https://a.facebook.com/graphql/?query_id=10152526044563380&sdk=ios&sdk_version=3&fb_api_caller_class=FBGraphQLService&format=json&app_version=6017145&query_params="
				+ URLEncoder.encode(query_params, "utf-8")
				+ "&method=get&locale=en_US&fb_api_req_friendly_name=FBGraphSearchUserQuery&pretty=0";
		// System.out.println(url);
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url,null);
		return json;
	}
}
