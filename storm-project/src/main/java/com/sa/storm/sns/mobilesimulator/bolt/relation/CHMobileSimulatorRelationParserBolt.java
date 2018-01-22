package com.sa.storm.sns.mobilesimulator.bolt.relation;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.CHCrawlerConfiguration;
import util.FacebookHDFSUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.ch.aws.s3.FileUploader;
import com.sa.common.json.JsonUtil;
import com.sa.competitorhunter.dao.EstimationRepository;
import com.sa.competitorhunter.dao.RelationHistoryRepository;
import com.sa.competitorhunter.definition.ReportTypeEnum;
import com.sa.competitorhunter.domain.Estimation;
import com.sa.competitorhunter.domain.RelationFansInfo;
import com.sa.competitorhunter.domain.RelationHistory;
import com.sa.crawler.definition.FacebookToken;
import com.sa.crawler.definition.TaskType;
import com.sa.graph.ch.object.JobInfo;
import com.sa.redis.definition.RedisDefinition;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.domain.FBUser;
import com.sa.storm.sns.util.ExcelUtil;
import com.sa.storm.sns.util.ExcelUtil.ExcelRow;
import com.sa.storm.sns.util.SuperTestUserList;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;

public class CHMobileSimulatorRelationParserBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(CHMobileSimulatorRelationParserBolt.class);
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	private RelationHistoryRepository relationRepo;
	private EstimationRepository estimationRepo;
	private static String HDFSUrl = "hdfs://hadoop-master1:9000/relation/";
	private static int MaxSize = 10000;
	private static int FreeMaxSize = 100;
	private static List<ExcelRow> header;
	private ExcelUtil util;
	private static String ExcelSheet = "FansList";

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			relationRepo = getSpringContext().getBean(RelationHistoryRepository.class);
			estimationRepo = getSpringContext().getBean(EstimationRepository.class);
			util = ExcelUtil.getInstance();
			header = initalHeader();
		} catch (Exception e) {
			log.error("Error in EsoonFilterBolt prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, TaskRequest.class);
	}

	@Override
	public void process(Tuple input) throws Exception {

		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		final String docJson = request.getParamByKey(TupleDefinition.Param.DOCUMENTS);
		if (docJson != null) {

			Document doc = JsonUtil.getMapper().readValue(docJson, Document.class);

			String pageIds = doc.getTitle();
			String pageNames = doc.getUserName();
			String hId = doc.getHistoryId();
			String uId = doc.getUserId();
			String isFree = doc.getLastFlag();
			String isAutoSubmit = doc.getCategory();
			if (SuperTestUserList.getSuperUserList().contains(uId.trim())) {
				isFree = "0";
			}
			if (null != pageIds && "" != pageIds) {
				try {
					// parse fans information
					String dateStr = DATE_FORMAT.format(new Date());
					String fileName = dateStr + "-Fans-of-" + pageNames + ".xls";
					String path = "/home/sa/fb/" + fileName;

					long totalFanNum = 0;
					String[] genders = { "males", "females" };
					for (String gender : genders) {
						if (isFree.equals("1")) {
							totalFanNum += freeUserFansParser(path, pageIds, pageNames, gender, dateStr);
						} else {
							totalFanNum += fansParser(path, pageIds, pageNames, gender, dateStr);
						}
					}
					if (null != isAutoSubmit && isAutoSubmit.equals("1") && totalFanNum > 0) {
						fansAutoCrawl(path, fileName, doc, input);
					} else {
						if (totalFanNum > 0) {
							if (isFree.equals("1")) {
								List<ExcelRow> content = new ArrayList<ExcelRow>();
								ExcelRow excelRow = new ExcelRow();
								excelRow.add(0, "Find more than 1000+ fans, wanan see all fans? please upgrade your account.");
								content.add(excelRow);
								util.saveExcel(path, ExcelSheet, false, true, header, content);
							}

							// upload to aws file system
							File tmpFile = new File(path);
							String HDFSPath = null;
							Set<RelationFansInfo> relationFansInfo = readFans(path, Long.parseLong(hId));
							try {
								if (tmpFile.exists()) {
									HDFSPath = HDFSUrl + fileName;
									FacebookHDFSUtil.saveLocalFile2HDFS(path, HDFSPath);
									log.info("uploadToHDFS: {}", path);
								}
							} catch (Exception ex) {
								log.error("exception hadpped when upload file to hdfs {}", ex.toString());
							}

							doc.setLocation(HDFSPath);
							doc.setFileName(fileName);

							String fileLocation = FileUploader.uploadFile(tmpFile, true, true);

							// update mysql record
							RelationHistory f = relationRepo.findById(Long.parseLong(hId));
							f.setFileLocation(fileLocation);
							f.setFileName(fileName);
							f.setUpdateDate(new Date());
							if (f.getStatus() == 0) {
								f.setStatus(1);
							} else {
								f.setStatus(2);
							}
							// persist fans info
							if (null != relationFansInfo) {
								f.setFansInfos(relationFansInfo);
							}
							relationRepo.saveAndFlush(f);

							TaskRequest WordCountTaskRequest = buildWordCountTaskRequest(doc, TaskType.FB_FANS);
							emit(input, WordCountTaskRequest, "article parser emit document");

							// update the estimation

						} else {
							RelationHistory f = relationRepo.findById(Long.parseLong(hId));
							f.setUpdateDate(new Date());
							if (f.getStatus() == 0) {
								f.setStatus(1);
							} else {
								f.setStatus(2);
							}

							relationRepo.saveAndFlush(f);

						}

						File tmpFile = new File(path);
						if (tmpFile.exists()) {
							tmpFile.delete();
						}
						List<Estimation> estmation = estimationRepo.getByTypeAndUserId((short) 1, Long.parseLong(uId), 2);

						Estimation est = estmation.get(0);
						est.setActualCompletedTime(new Date());
						estimationRepo.saveAndFlush(est);
						String emailNotificationResult = FacebookApiRequestUtil.getInstance().submitRequest(
								CHCrawlerConfiguration.getInstance().getRestTomcatPath() + "sa-rest/pub/report/notification?userId=" + uId
										+ "&type=" + ReportTypeEnum.FANS_RELATION_ANALYSIS.getId());

						log.info("emailNotificationResult {} ", emailNotificationResult);
					}
				} catch (Exception ex) {
					log.error(ex.getMessage(), ex);
				}

			}

		} else {
			TaskResult taskResult = new TaskResult(request, "No document in the request, ignored", TupleDefinition.Result.SUCCESS.getCode());
			emit(input, taskResult);
		}

	}

	private void fansAutoCrawl(String path, String fileName, Document doc, Tuple input) throws Exception {
		// upload to aws file system
		File tmpFile = new File(path);
		String HDFSPath = null;
		try {
			if (tmpFile.exists()) {
				HDFSPath = HDFSUrl + fileName;
				FacebookHDFSUtil.saveLocalFile2HDFS(path, HDFSPath);
				log.info("uploadToHDFS: {}", path);
			}
		} catch (Exception ex) {
			log.error("exception hadpped when upload file to hdfs {}", ex.toString());
		}

		doc.setLocation(HDFSPath);
		doc.setFileName(fileName);

		TaskRequest WordCountTaskRequest = buildWordCountTaskRequest(doc, TaskType.FB_FANS);
		emit(input, WordCountTaskRequest, "article parser emit document");
		tmpFile.delete();
	}

	private TaskRequest buildWordCountTaskRequest(Document doc, TaskType taskType) throws Exception {
		Map<String, String> params = new HashMap<String, String>();
		TaskRequest request = new TaskRequest(UUID.randomUUID().toString(), null, taskType.getCode(), UUID.randomUUID().toString(),
				RedisDefinition.Priority.NORMAL.getCode(), params);
		request.addParam(TupleDefinition.Param.DOCUMENTS, JsonUtil.getMapper().writeValueAsString(doc));

		return request;
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

	private long fansParser(String path, String pageIds, String pageNames, String gender, String dateStr) throws IOException, JSONException {
		log.info("begin to crawl, gender: " + gender);
		String nextPageUrl = "";
		String end_cursor = "";
		long userCount = 0;
		String queryStr = bulidQueryStr(pageIds, gender);

		while (null != nextPageUrl) {
			String json = submitRequest(end_cursor, pageIds, gender, queryStr);
			try {
				JSONObject jsonObj = new JSONObject(json);
				JSONArray userObjs = jsonObj.getJSONObject(queryStr).getJSONObject("results").optJSONArray("edges");
				JSONObject pagging = jsonObj.getJSONObject(queryStr).getJSONObject("results").optJSONObject("page_info");

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

						// List<ExcelRow> content = new ArrayList<ExcelRow>();
						// ExcelRow excelRow = new ExcelRow();
						// excelRow.add(0, id);
						// excelRow.add(1, name);
						// excelRow.add(2, gender);
						// excelRow.add(3, avatar);
						//
						// content.add(excelRow);
						// util.saveExcel(path, "FansList", false, true, header, content);
						try {
							addRow(id, name, gender, avatar, path);
						} catch (Exception e) {
							log.error(e.getMessage(), e);
							continue;
						}
					}

					log.info("current conut: {}, gender: {}", userCount, gender);

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

	private void addRow(String id, String name, String gender, String avatar, String path) throws Exception {

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

	private long freeUserFansParser(String path, String pageIds, String pageNames, String gender, String dateStr) throws IOException,
			JSONException {
		log.info("begin to crawl, gender: " + gender);
		String nextPageUrl = "";
		String end_cursor = "";
		long userCount = 0;
		String queryStr = bulidQueryStr(pageIds, gender);

		while (null != nextPageUrl) {
			String json = submitRequest(end_cursor, pageIds, gender, queryStr);
			// System.out.println(json);
			try {
				JSONObject jsonObj = new JSONObject(json);
				JSONArray userObjs = jsonObj.getJSONObject(queryStr).getJSONObject("results").optJSONArray("edges");
				JSONObject pagging = jsonObj.getJSONObject(queryStr).getJSONObject("results").optJSONObject("page_info");

				if (pagging.getString("has_next_page") == "true") {
					int userLength = userObjs.length();
					if (userLength == 0) {
						break;
					}
					userCount += userLength;
					if (userCount < 21) {
						for (int i = 0; i < userObjs.length(); i++) {
							JSONObject user = userObjs.getJSONObject(i);
							JSONObject node = user.getJSONObject("node");
							String id = node.getString("id");
							String name = node.getString("name");
							String avatar = node.optJSONObject("profilePicture74").optString("uri");

							// List<ExcelRow> content = new ArrayList<ExcelRow>();
							// ExcelRow excelRow = new ExcelRow();
							// excelRow.add(0, id);
							// excelRow.add(1, name);
							// excelRow.add(2, gender);
							// excelRow.add(3, avatar);
							// content.add(excelRow);
							// util.saveExcel(path, "FansList", false, true, header, content);
							addRow(id, name, gender, avatar, path);
						}
					}
					log.info(pageNames + " current conut: {}, gender: {}", userCount, gender);
					end_cursor = pagging.getString("end_cursor");
				} else {
					nextPageUrl = null;
				}
				if (userCount > FreeMaxSize) {
					break;
				}
			} catch (Exception ex) {

				log.error("ex:{}", ex.toString());
				break;
			}
		}
		return userCount;
	}

	private String bulidQueryStr(String queryUrl, String gender) {

		String[] pageIds = queryUrl.split(",");
		String likers = "";
		for (String pageId : pageIds) {
			likers += "likers(" + pageId + "),";
		}
		likers = likers.substring(0, likers.length() - 1);
		String queryStr = "intersect(" + gender + "()," + likers + ")";

		return queryStr;
	}

	private String submitRequest(String end_cursor, String pageId, String gender, String queryStr) throws UnsupportedEncodingException {

		String query_params = "{\"generic_attachment_small_square_image_dimension\":\"100\",\"scale\":2,\"queryString\":\""
				+ queryStr
				+ "\",\"device\":\"iphone\",\"streaming_image_resolution\":960,\"showInterestingComment\":\"false\",\"generic_attachment_fallback_square_image_dimension\":\"224\",\"generic_attachment_tall_cover_image_height\":\"292\",\"media_type\":\"image\\/jpeg\",\"count\":6,\"render_actor_in_gutter\":\"false\",\"generic_attachment_small_cover_image_width\":\"88\",\"generic_attachment_portrait_image_height\":\"336\",\"feedback_include_seen_by\":\"true\",\"generic_attachment_tall_cover_image_width\":\"560\",\"enable_attachments_redesign\":\"false\",\"generic_attachment_large_square_image_dimension\":\"560\",\"render_location\":\"IOS_TIMELINE\",\"generic_attachment_small_cover_image_height\":\"88\",\"generic_attachment_portrait_image_width\":\"224\",\"taggableUserIncludeByLines\":\"false\"}";
		if (end_cursor != "") {
			query_params = "{\"generic_attachment_small_square_image_dimension\":\"100\",\"scale\":2,\"queryString\":\""
					+ queryStr
					+ "\",\"device\":\"iphone\",\"streaming_image_resolution\":960,\"showInterestingComment\":\"false\",\"generic_attachment_fallback_square_image_dimension\":\"224\",\"generic_attachment_tall_cover_image_height\":\"292\",\"media_type\":\"image\\/jpeg\",\"count\":6,\"render_actor_in_gutter\":\"false\",\"generic_attachment_small_cover_image_width\":\"88\",\"generic_attachment_portrait_image_height\":\"336\",\"feedback_include_seen_by\":\"true\",\"generic_attachment_tall_cover_image_width\":\"560\",\"enable_attachments_redesign\":\"false\",\"generic_attachment_large_square_image_dimension\":\"560\",\"render_location\":\"IOS_TIMELINE\",\"generic_attachment_small_cover_image_height\":\"88\",\"generic_attachment_portrait_image_width\":\"224\",\"taggableUserIncludeByLines\":\"false\",\"afterCursor\":\""
					+ end_cursor + "\"}";
		}
		String url = "https://chmobile.facebook.com/graphql/?query_id=10152526044563380&sdk=ios&sdk_version=3&fb_api_caller_class=FBGraphQLService&format=json&app_version=6017145&query_params="
				+ URLEncoder.encode(query_params, "utf-8")
				+ "&method=get&locale=en_US&fb_api_req_friendly_name=FBGraphSearchUserQuery&pretty=0";
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
		return json;
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
