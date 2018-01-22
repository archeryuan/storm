package com.sa.storm.sns.mobilesimulator.bolt.persistence;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.FacebookHDFSUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.common.json.JsonUtil;
import com.sa.competitorhunter.domain.RelationFansInfo;
import com.sa.crawler.definition.FacebookToken;
import com.sa.graph.ch.object.BackendJob;
import com.sa.graph.ch.object.JobInfo;
import com.sa.graph.ch.object.Page;
import com.sa.graph.ch.object.Person;
import com.sa.graph.ch.service.FBUserDataService;
import com.sa.graph.ch.service.PeopleService;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.util.ExcelUtil;
import com.sa.storm.sns.util.ExcelUtil.ExcelRow;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;

public class CHPersistenceBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(CHPersistenceBolt.class);

	private ExcelUtil util;
	private static String ExcelSheet = "FansList";
	private static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd");
	private FBUserDataService fbUserDataService;
	RedisUtil redisUtil;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			util = ExcelUtil.getInstance();
			fbUserDataService = new FBUserDataService();
			redisUtil = RedisUtil.getInstance();
		} catch (Exception e) {
			log.error("Error in EsoonFilterBolt prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void process(Tuple input) throws Exception {

		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		final String docJson = request.getParamByKey(TupleDefinition.Param.DOCUMENTS);
		if (docJson != null) {
			Document doc = JsonUtil.getMapper().readValue(docJson, Document.class);

			String pageIds = doc.getTitle();
			String fileLocation = doc.getLocation();
			String fileName = doc.getFileName();
			String hId = doc.getHistoryId();

			if (null != fileLocation) {
				String path = "/home/sa/fb/persistence/" + fileName;
				FacebookHDFSUtil.moveHDFSFile2Local(fileLocation, path);
				List<Page> pages = getPageList(pageIds);
				markInRedis(pageIds);
				saveFans(path, pages, hId, 20000);
				File tmpFile = new File(path);
				if (tmpFile.exists()) {
					tmpFile.delete();
				}
			}

		} else {
			TaskResult taskResult = new TaskResult(request, "No document in the request, ignored", TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
		}
	}

	private List<Page> getPageList(String pageIds) {
		List<Page> pageList = new ArrayList<Page>();
		for (String pageId : pageIds.split(",")) {
			Page p = new Page();
			p.setPageId(Long.parseLong(pageId));
			pageList.add(p);
		}
		return pageList;
	}

	private void saveFans(String path, List<Page> pages, String hId, int count) throws IOException {

		List<ExcelRow> excelRowList = util.load(path, ExcelSheet, count);
		int i = 0;
		if (null != excelRowList) {
			for (ExcelRow excelRow : excelRowList) {
				i++;
				if (i == 1) {
					continue;
				}

				String FBUId = excelRow.getCell2Value().get(0);
				if (FBUId.contains("Find more")) {
					continue;
				}
				String avatar = excelRow.getCell2Value().get(3);
				log.info("Fans Id {}", FBUId);
				saveUserInfoToGraphDB(FBUId, pages, hId, avatar);
			}
		}

	}

	private void saveUserInfoToGraphDB(String FBUId, List<Page> pages, String hId, String avatar) {
		try {

			String json = submitSingleUserRequest(FBUId);
			log.info("FBUid {} ", FBUId);

			Person person = parserUser(json, pages, avatar);
			// log.info("Avatar {} , Avatar from graphdb: {}",avatar,person.getAURL());
			// BackendJob job = new BackendJob();
			// job.setJobId(Long.parseLong(hId));
			fbUserDataService.saveFBUser(person, null);
		} catch (Exception e) {
			log.error("error {}", e.toString());
			e.printStackTrace();
		}
	}

	private String submitSingleUserRequest(String userId) {
		String url = "https://graph.facebook.com/" + userId + "?access_token=" + FacebookToken.MobileApiToken.USA_MOBILE_TOKEN;
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
		return json;
	}

	private Person parserUser(String json, List<Page> pages, String avatar) throws JSONException {

		Person p = new Person();
		try {
			JSONObject userInfo = new JSONObject(json);
			String id = userInfo.optString("id");
			p.setPersonId(Long.parseLong(id));
			String name = userInfo.optString("name");
			p.setName(name);
			String username = userInfo.optString("username");
			p.setFbStrId(username);

			p.setLikes(pages);

			p.setAURL(avatar);
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
				p.setCity(location);
			}

			String schoolName = null;
			if (null != userInfo.optJSONArray("education")) {
				JSONArray educations = userInfo.optJSONArray("education");
				JSONObject school = userInfo.optJSONArray("education").getJSONObject(educations.length() - 1);
				schoolName = school.optJSONObject("school").optString("name");
				p.setCollege(schoolName);
			}
			String birthday = userInfo.optString("birthday");
			if (null != birthday && "" != birthday) {
				try {
					Date BDate = sdf.parse(birthday);
					p.setBDay(BDate);
					log.info("birthdayDate {} ", BDate.toString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			String interestedIn = userInfo.optString("interested_in");

			if (null != interestedIn && "" != interestedIn) {
				log.info("genderInterest {} ", interestedIn);
				p.setGenderInt(extractEnglish(interestedIn));
			}
			JSONArray languages = userInfo.optJSONArray("languages");
			if (null != languages) {
				ArrayList<String> languageList = new ArrayList<String>();
				for (int i = 0; i < languages.length(); i++) {
					languageList.add(languages.getJSONObject(i).optString("name"));
				}
				p.setLang(languageList);
			}

			String religion = userInfo.optString("religion");
			if (null != religion && "" != religion) {
				p.setReligion(extractEnglish(religion));
			}

			String political = userInfo.optString("political");
			if (null != political && "" != political) {
				p.setPolitics(extractEnglish(political));
			}

			String relation = userInfo.optString("relationship_status");
			if (null != relation || "" != relation) {
				p.setRelation(relation);
			}
			String phone = userInfo.optString("mobile_phone");
			if (null != phone && "" != phone) {
				p.setPPhone(Long.parseLong(phone));
			}
			String website = userInfo.optString("website");
			if (null != phone && "" != phone) {
				p.setWebSite(website);
			}

			JSONObject address = userInfo.optJSONObject("address");
			if (null != address) {
				p.setAddr(address.optString("street"));
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
					p.setJobInfoJSON(JobInfoJson);
					log.info(JobInfoJson);
				} catch (Exception e) {
					log.error(e.getMessage(), e);
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		// ArrayList<Person> friends = parseFriends(id);
		// if (null != friends) {
		// p.setFriends(friends);
		// }
		return p;

	}

	private void markInRedis(String pageIds) {
		String key = null;
		if (pageIds.contains(",")) {
			TreeSet<Long> pageIdSet = new TreeSet<Long>();
			for (String pageId : pageIds.split(",")) {
				pageIdSet.add(Long.parseLong(pageId));
			}
			String pageIdStr = "";
			for (long pageId : pageIdSet) {
				pageIdStr += pageId + "-";
			}

			key = "ch-relation-" + pageIdStr;
			key = key.substring(0, key.length() - 1);
		} else {
			key = "ch-finder-" + pageIds;
		}
		int expireTime = 3600 * 24 * 15;
		redisUtil.setex(key, expireTime, String.valueOf(new Date().getTime()));

	}

	private static String extractEnglish(String str) {
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < str.length(); i++) {
			char a = str.charAt(i);
			int c = (int) a;
			if ((c >= 65 && c <= 90) || (c >= 97 && c <= 122) || c == 32) {
				result.append(a);
			}
		}

		return result.toString();
	}

	private List<ExcelRow> initalHeader() {
		List<ExcelRow> header = new ArrayList<ExcelRow>();
		Map<Integer, String> cell2Value = new HashMap<Integer, String>();
		ExcelRow excelRow = new ExcelRow();
		cell2Value = new HashMap<Integer, String>();
		cell2Value.put(0, "id");
		cell2Value.put(1, "name");
		cell2Value.put(2, "gender");
		cell2Value.put(3, "avatar");
		excelRow.setCell2Value(cell2Value);
		header.add(excelRow);
		return header;
	}

	public Set<RelationFansInfo> readFans(String path, Long hId) throws IOException {

		Set<RelationFansInfo> fans = new HashSet<RelationFansInfo>();
		List<ExcelRow> excelRowList = util.load(path, ExcelSheet, 21);
		int i = 0;
		if (null != excelRowList) {
			for (ExcelRow excelRow : excelRowList) {
				i++;
				if (i == 1) {
					continue;
				}
				String id = excelRow.getCell2Value().get(0);
				String name = excelRow.getCell2Value().get(1);
				String avatar = excelRow.getCell2Value().get(3);

				log.info("loading fans info {} ", id);
				RelationFansInfo r = new RelationFansInfo();
				r.setPageLink("https://www.facebook.com/" + id);
				r.setUserName(name);
				r.setAvatarUrl(avatar);
				r.setHistoryId(hId);
				fans.add(r);
			}
		}
		return fans;
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in fliter doc", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

	public void uploadLocalFile2HDFS(String s, String d) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path src = new Path(s);
		Path dst = new Path(d);

		hdfs.copyFromLocalFile(src, dst);

		hdfs.close();
	}

}
