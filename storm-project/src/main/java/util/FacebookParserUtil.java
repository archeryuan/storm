package util;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.common.json.JsonUtil;
import com.sa.crawler.definition.FacebookToken;
import com.sa.graph.ch.object.JobInfo;
import com.sa.storm.sns.domain.FBUser;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;

public class FacebookParserUtil {
	private static final Logger log = LoggerFactory.getLogger(FacebookParserUtil.class);

	private static String submitSingleUserRequest(String userId) {
		String url = "https://graph.facebook.com/" + userId + "?access_token=" + FacebookToken.MobileApiToken.USA_MOBILE_TOKEN;
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
		return json;
	}

	public static FBUser parserUser(String id) throws JSONException {
		FBUser p = null;
		try {
			p = new FBUser();
			String json = submitSingleUserRequest(id);
			JSONObject userInfo = new JSONObject(json);
			if (userInfo.has("can_post")) {
				return null;
			}

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

}
