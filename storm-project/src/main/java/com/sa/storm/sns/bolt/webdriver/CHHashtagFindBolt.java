package com.sa.storm.sns.bolt.webdriver;

import java.io.File;
import java.io.IOException;
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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.CHCrawlerConfiguration;
import util.FacebookParserUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.ch.aws.s3.FileUploader;
import com.sa.common.json.JsonUtil;
import com.sa.competitorhunter.dao.EstimationRepository;
import com.sa.competitorhunter.dao.FinderHistoryRepository;
import com.sa.competitorhunter.definition.ReportTypeEnum;
import com.sa.competitorhunter.domain.Estimation;
import com.sa.competitorhunter.domain.FinderHistory;
import com.sa.competitorhunter.object.TopFans;
import com.sa.competitorhunter.object.TopHashtag;
import com.sa.crawler.definition.FacebookToken;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.domain.FBUser;
import com.sa.storm.sns.util.ExcelUtil;
import com.sa.storm.sns.util.ExcelUtil.ExcelRow;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;

public class CHHashtagFindBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(CHHashtagFindBolt.class);
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	private FinderHistoryRepository finderRepo;
	private EstimationRepository estimationRepo;

	private static List<ExcelRow> header;
	private ExcelUtil util;
	private static String ExcelSheet = "UsersList";
	private static final String token = "CAAAAAYsX7TsBAAlS7Kd1gNnf7rfE5JjYxWzw4ceKZAc0CqugyHOySaqn6azND2gXZBQEQeijyEtoL8mLi6QdRzpRcPhBPhbdhFKS5Bz5336grXAdUNbByRDrgthjRW7O6W4MeJRrCnBKActOqZCSJvH5YX8oX5CzBVrP9mHZCUWdtx2othJy74niJZA85oiwt1CmvZBZBZC7gWtg1uG3R3MtDjGYUhIqGiPKHW5p7CF5gsq8I1anB6M8";

	private static ArrayList<String> userIdList;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			finderRepo = getSpringContext().getBean(FinderHistoryRepository.class);
			estimationRepo = getSpringContext().getBean(EstimationRepository.class);
			header = initalHeader();
			util = ExcelUtil.getInstance();
		} catch (Exception e) {
			log.error("Error in CHHashtagFindBolt prepare", e);
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

			String queryUrl = doc.getUrl();
			String hashtag = doc.getKeywords();
			String hId = doc.getHistoryId();
			String uId = doc.getUserId();

			long type = doc.getTypeId();
			String fileName = "";
			String dateStr = DATE_FORMAT.format(new Date());
			String commandFile = "";
			short taskType = 0;
			if (type == 1L) {
				taskType = 4;
				fileName = dateStr + "-Hashtag-relate-to-Hashtag-" + hashtag + ".csv";
				commandFile = "fb-hashtag.py";

				String path = "/home/sa/fb/hashtag/" + fileName;

				queryUrl = "https://www.facebook.com/hashtag/" + URLEncoder.encode(hashtag, "UTF-8");
				log.info("queryUrl: " + queryUrl + " uId " + uId + " hId " + hId);
				int exitValue = executePythonCommand(queryUrl, path, commandFile);
				if (exitValue == 0) {
					// read word
					File file = new File(path);
					List<String> list = FileUtils.readLines(file, "UTF-8");
					List<TopHashtag> hashtagList = new ArrayList<TopHashtag>();
					int i = 0;
					for (String word : list) {
						if (i > 20) {
							break;
						}
						if ("" == word) {
							continue;
						}
						TopHashtag hashtagWord = new TopHashtag();
						hashtagWord.setHashtag(word);
						hashtagWord.setTimes(1);
						hashtagList.add(hashtagWord);
						i++;
					}

					FinderHistory f = finderRepo.findById(Long.parseLong(hId));
					if (i > 0) {
						String hashtagStr = JsonUtil.getMapper().writeValueAsString(hashtagList);
						f.setTopHashtag(hashtagStr);
						String fileLocation = FileUploader.uploadFile(new File(path), true, true);
						log.info("path: {}, location: {}", path, fileLocation);
						f.setFileLocation(fileLocation);
						f.setFileName(fileName);
					}
					f.setUpdateDate(new Date());

					if (f.getStatus() == 0) {
						f.setStatus(1);
					} else {
						f.setStatus(2);
					}
					finderRepo.saveAndFlush(f);

					// 2.estimation
					List<Estimation> estmation = estimationRepo.getByTypeAndUserId(taskType, Long.parseLong(uId), 2);

					Estimation est = estmation.get(0);
					est.setActualCompletedTime(new Date());
					estimationRepo.saveAndFlush(est);

					String emailNotificationResult = FacebookApiRequestUtil.getInstance().submitRequest(
							CHCrawlerConfiguration.getInstance().getRestTomcatPath() + "sa-rest/pub/report/notification?userId=" + uId
									+ "&type=" + ReportTypeEnum.HASHTAG_FINDER_REQUEST.getId());
					log.info("emailNotificationResult {} ", emailNotificationResult);
				}
			} else {

				userIdList = new ArrayList<String>();
				taskType = 3;
				fileName = dateStr + "-Fans-withPostMentionedHashtag-" + hashtag + ".xls";
				String path = "/home/sa/fb/hashtag/" + fileName;
				// commandFile = "fb-hashtag-user.py";
				long userCount = findUserByFBGraphSearch(path, hashtag);

				userCount += findUserByFBKeywordSearch(path, hashtag);
				userCount += findUserByFBKeywordSearch(path, "%23" + hashtag);
				// read top 20 users from file
				FinderHistory f = finderRepo.findById(Long.parseLong(hId));
				if (userCount > 0) {
					Set<TopFans> finderFansInfo = readFans(path, Long.parseLong(hId));
					String topFans = JsonUtil.getMapper().writeValueAsString(finderFansInfo);
					String fileLocation = FileUploader.uploadFile(new File(path), true, true);
					log.info("path: {}, location: {}", path, fileLocation);
					f.setTopFans(topFans);
					f.setFileLocation(fileLocation);
					f.setFileName(fileName);
				}

				f.setUpdateDate(new Date());

				if (f.getStatus() == 0) {
					f.setStatus(1);
				} else {
					f.setStatus(2);
				}
				finderRepo.saveAndFlush(f);

				// 2.estimation
				List<Estimation> estmation = estimationRepo.getByTypeAndUserId(taskType, Long.parseLong(uId), 2);

				Estimation est = estmation.get(0);
				est.setActualCompletedTime(new Date());
				estimationRepo.saveAndFlush(est);

				String emailNotificationResult = FacebookApiRequestUtil.getInstance().submitRequest(
						CHCrawlerConfiguration.getInstance().getRestTomcatPath() + "sa-rest/pub/report/notification?userId=" + uId
								+ "&type=" + ReportTypeEnum.FANS_FINDER_REQUEST.getId());
				log.info("emailNotificationResult {} ", emailNotificationResult);

			}
			TaskResult taskResult = new TaskResult(request, "genenate file sucessfully", TupleDefinition.Result.SUCCESS.getCode());
			emit(input, taskResult);
		} else {
			TaskResult taskResult = new TaskResult(request, "No document in the request, ignored", TupleDefinition.Result.SUCCESS.getCode());
			emit(input, taskResult);

		}

	}

	private void addRow(String id, String name, String avatar, String path) throws Exception {

		FBUser user = FacebookParserUtil.parserUser(id);
		List<ExcelRow> content = new ArrayList<ExcelRow>();
		ExcelRow excelRow = new ExcelRow();
		if (null == user) {
			excelRow.add(0, id);
			excelRow.add(1, name);
			excelRow.add(2, avatar);

		} else {
			excelRow.add(0, id);
			excelRow.add(1, name);
			excelRow.add(2, avatar);
			String gender = null != user.getGender() ? user.getGender() : "";
			excelRow.add(3, gender);
			excelRow.add(4, "http://www.facebook.com/" + user.getUserId());
			excelRow.add(5, user.getLocation());
			excelRow.add(6, user.getHomeTown());
			excelRow.add(7, user.getWork());
			excelRow.add(8, user.getCollege());
			excelRow.add(9, user.getBirthday());
			excelRow.add(10, user.getRelation());
		}
		content.add(excelRow);
		util.saveExcel(path, "FansList", false, true, header, content);
	}

	private long findUserByFBGraphSearch(String path, String hashtag) throws Exception {
		String nextPageUrl = "";
		String end_cursor = null;
		long userCount = 0;
		String queryStr = "keywords_top(\\\"" + hashtag + "\\\")";
		int count = 0;
		while (null != nextPageUrl) {
			String json = submitRequest(end_cursor, queryStr);
			JSONObject jsonObj = new JSONObject(json);
			JSONArray postObjs = jsonObj.getJSONObject("keywords_top(\"" + hashtag + "\")").getJSONObject("filtered_query")
					.optJSONObject("modules").optJSONArray("edges");

			JSONObject pagging = jsonObj.getJSONObject("keywords_top(\"" + hashtag + "\")").getJSONObject("filtered_query")
					.optJSONObject("modules").optJSONObject("page_info");
			log.info("hashtag find user {}", end_cursor);
			for (int i = 0; i < postObjs.length(); i++) {
				JSONObject user = postObjs.getJSONObject(i);

				JSONObject node = user.getJSONObject("node");
				JSONArray edges = node.optJSONObject("results").optJSONArray("edges");
				JSONObject subNode = edges.optJSONObject(0).optJSONObject("node");

				if (subNode.has("actors")) {
					JSONObject actor = subNode.optJSONArray("actors").optJSONObject(0);
					if (null != actor) {
						String id = actor.optString("id");
						if (userIdList.contains(id)) {
							continue;
						}
						userIdList.add(id);
						String name = actor.optString("name");
						String avatar = actor.optJSONObject("profilePicture50").optString("uri");
						try {
							addRow(id, name, avatar, path);
						} catch (Exception e) {
							log.error(e.getMessage(), e);
							continue;
						}
						userCount += postObjs.length();
					}
				}

			}

			if (pagging.getString("has_next_page") == "true") {
				end_cursor = pagging.getString("end_cursor");
				int userLength = postObjs.length();
				if (userLength == 0) {
					break;
				}
			} else {
				nextPageUrl = null;
			}
			count++;
			if (userCount > 1000 || count > 50) {
				break;
			}

		}

		return userCount;
	}

	private long findUserByFBKeywordSearch(String path, String hashtag) {
		String nextPageUrl = "";
		String url = buildUrl(hashtag, FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
		long userCount = 0;
		int pageNum = 0;

		while (null != url) {
			try {
				String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url,
						FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
				log.info("Hashtag find user {}", url);

				JSONObject jsonObj = new JSONObject(json);
				JSONArray postObjs = jsonObj.optJSONArray("data");
				for (int i = 0; i < postObjs.length(); i++) {
					JSONObject postObj = postObjs.getJSONObject(i);
					if (null != postObj.optJSONObject("from")) {
						String id = postObj.optJSONObject("from").optString("id");
						if (userIdList.contains(id)) {
							continue;
						}
						userIdList.add(id);

						String name = postObj.optJSONObject("from").optString("name");
						String avatar = null;

						StringBuilder userSb = new StringBuilder();

						userSb.append("https://graph.facebook.com/").append(id).append("/picture").append("?access_token=")
								.append(FacebookToken.MobileApiToken.USA_MOBILE_TOKEN).append("&redirect=false");
						String avatarStr = FacebookApiRequestUtil.getInstance().submitRequest(userSb.toString());
						if (null != avatarStr && "" != avatarStr) {
							avatar = avatarStr.substring(1, avatarStr.length() - 1);
						}

						try {
							addRow(id, name, avatar, path);
						} catch (Exception e) {
							log.error(e.getMessage(), e);
							continue;
						}
						userCount += postObjs.length();
					}
				}
				JSONObject pagingObj = jsonObj.optJSONObject("paging");
				url = (pagingObj == null) ? null : pagingObj.optString("next");
				pageNum++;
				if (pageNum > 50) {
					break;
				}
			} catch (Exception e) {
				log.error(e.getMessage(), e);
				continue;
			}
		}
		return userCount;
	}

	private static String buildUrl(String hashtag, String token) {

		return "https://graph.facebook.com/search?q=" + hashtag + "&access_token=" + FacebookToken.MobileApiToken.USA_MOBILE_TOKEN;
	}

	public Set<TopFans> readFans(String path, Long hId) throws IOException {

		Set<TopFans> fans = new HashSet<TopFans>();
		List<ExcelRow> excelRowList = util.load(path, ExcelSheet, 21);
		int i = 0;
		if (null != excelRowList) {
			for (ExcelRow excelRow : excelRowList) {
				i++;
				if (i == 1) {
					continue;
				}
				String id = excelRow.getCell2Value().get(0);
				if (id.contains("Find more")) {
					continue;
				}
				String name = excelRow.getCell2Value().get(1);
				String avatar = excelRow.getCell2Value().get(2);

				TopFans r = new TopFans();
				r.setLink("https://www.facebook.com/" + id);
				r.setUserName(name);
				r.setaUrl(avatar);
				fans.add(r);
			}
		}
		return fans;
	}

	private List<ExcelRow> initalHeader() {
		List<ExcelRow> header = new ArrayList<ExcelRow>();
		Map<Integer, String> cell2Value = new HashMap<Integer, String>();
		ExcelRow excelRow = new ExcelRow();
		cell2Value = new HashMap<Integer, String>();
		cell2Value.put(0, "id");
		cell2Value.put(1, "name");
		cell2Value.put(2, "avatar");
		cell2Value.put(3, "gender");
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

	private static int executePythonCommand(String url, String filePath, String commandFile) throws IOException, InterruptedException {

		List<String> cmd = new ArrayList<String>();
		cmd.add("python");
		cmd.add("/home/sa/fb/hashtag/" + commandFile);
		cmd.add("-queryurl");
		cmd.add(url);
		cmd.add("-file");
		cmd.add(filePath);

		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);

		Process process = null;

		process = pb.start();

		int w = process.waitFor();
		return w;
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		// TODO Auto-generated method stub
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

	private static String submitRequest(String end_cursor, String queryStr) throws Exception {
		String q = "{\"should_load_streaming_image\":\"false\",\"generic_attachment_large_square_image_dimension\":\"560\",\"numBylines\":3,\"render_actor_in_gutter\":\"false\",\"facts_count\":10,\"queryString\":\""
				+ queryStr
				+ "\",\"generic_attachment_small_cover_image_height\":\"88\",\"scale\":2,\"device\":\"iphone\",\"generic_attachment_tall_cover_image_height\":\"292\",\"is_zero_rated\":\"false\",\"formatType\":\"concise\",\"generic_attachment_small_cover_image_width\":\"88\",\"resultsCount\":1000,\"use_chrono_sort\":\"false\",\"generic_attachment_tall_cover_image_width\":\"560\",\"generic_attachment_portrait_image_width\":\"224\",\"enable_attachments_redesign\":\"true\",\"in_channels_experiment\":\"false\",\"enable_image_share_attachment\":\"false\",\"generic_attachment_fallback_square_image_dimension\":\"224\",\"count\":6,\"generic_attachment_portrait_image_height\":\"336\",\"story_reaction_surface\":\"ios_story\",\"exact_match\":\"false\",\"feedback_include_seen_by\":\"true\",\"media_type\":\"image/jpeg\",\"videoThumbnailSize\":720,\"renderLocation\":\"feed_mobile\",\"call_site\":\"ios:post_search\",\"streaming_image_resolution\":960,\"vertical\":\"content\",\"generic_attachment_small_square_image_dimension\":\"100\"}";
		if (null != end_cursor) {
			q = "{\"should_load_streaming_image\":\"false\",\"generic_attachment_large_square_image_dimension\":\"560\",\"numBylines\":3,\"render_actor_in_gutter\":\"false\",\"facts_count\":10,\"queryString\":\""
					+ queryStr
					+ "\",\"generic_attachment_small_cover_image_height\":\"88\",\"scale\":2,\"device\":\"iphone\",\"generic_attachment_tall_cover_image_height\":\"292\",\"is_zero_rated\":\"false\",\"formatType\":\"concise\",\"generic_attachment_small_cover_image_width\":\"88\",\"resultsCount\":1000,\"use_chrono_sort\":\"false\",\"generic_attachment_tall_cover_image_width\":\"560\",\"generic_attachment_portrait_image_width\":\"224\",\"enable_attachments_redesign\":\"true\",\"in_channels_experiment\":\"false\",\"enable_image_share_attachment\":\"false\",\"generic_attachment_fallback_square_image_dimension\":\"224\",\"count\":6,\"generic_attachment_portrait_image_height\":\"336\",\"story_reaction_surface\":\"ios_story\",\"exact_match\":\"false\",\"feedback_include_seen_by\":\"true\",\"media_type\":\"image/jpeg\",\"videoThumbnailSize\":720,\"renderLocation\":\"feed_mobile\",\"call_site\":\"ios:post_search\",\"streaming_image_resolution\":960,\"vertical\":\"content\",\"generic_attachment_small_square_image_dimension\":\"100\",\"afterCursor\":\""
					+ end_cursor + "\"}";
		}
		String url = "https://chmobile.facebook.com/graphql/?sdk_version=3&fb_api_req_friendly_name=FBGraphSearchKeywordsQuery&pretty=0&fb_api_caller_class=FBGraphQLService&app_version=9917221&format=json&query_params="
				+ URLEncoder.encode(q, "utf-8") + "&method=get&locale=en_US&sdk=ios&query_id=10152928620438380";
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, token);
		return json;
	}

}
