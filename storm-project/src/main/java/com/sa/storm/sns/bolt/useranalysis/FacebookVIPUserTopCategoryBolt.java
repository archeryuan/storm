package com.sa.storm.sns.bolt.useranalysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.CHCrawlerConfiguration;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.common.json.JsonUtil;
import com.sa.competitorhunter.dao.EstimationRepository;
import com.sa.competitorhunter.dao.VipMappingHistoryRepository;
import com.sa.competitorhunter.definition.ReportTypeEnum;
import com.sa.competitorhunter.domain.Estimation;
import com.sa.competitorhunter.domain.Page;
import com.sa.competitorhunter.domain.VipMappingHistory;
import com.sa.competitorhunter.object.TopVipUser;
import com.sa.crawler.definition.TaskType;
import com.sa.graph.ch.exception.ApplicationException;
import com.sa.graph.ch.object.Person;
import com.sa.graph.ch.service.PeopleService;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.ca.domain.ImgInfo;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.domain.TopCategory;
import com.sa.storm.sns.domain.TopLikePage;
import com.sa.storm.sns.util.FacebookMobileApiUtil;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;

public class FacebookVIPUserTopCategoryBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(FacebookVIPUserTopCategoryBolt.class);
	private static ArrayList<String> posts = new ArrayList<String>();
	private static String HDFSUrl = "hdfs://hadoop-master1:9000/fb/";
	private FacebookMobileApiUtil facebookMobileApiUtil;
	private RedisUtil redisUtil;
	private static final int MAX_LIKE_PAGE_COUNT = 1000;
	private static final int TOP_COUNT = 20;
	private VipMappingHistoryRepository vipMappingHistoryRepository;
	private EstimationRepository estimationRepo;
	private ArrayList<TopVipUser> vipUserList;
	private static String restUrl;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			facebookMobileApiUtil = FacebookMobileApiUtil.getInstance();
			redisUtil = RedisUtil.getInstance();
			vipMappingHistoryRepository = getSpringContext().getBean(VipMappingHistoryRepository.class);
			estimationRepo = getSpringContext().getBean(EstimationRepository.class);
			restUrl = CHCrawlerConfiguration.getInstance().getRestTomcatPath();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("Error in prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, TaskRequest.class);
	}

	@Override
	public void process(Tuple input) throws Exception {

		DocumentResult docResult = (DocumentResult) getInputByClass(input, DocumentResult.class);
		Document doc = docResult.getDocument();
		if (null != doc) {
			try {
				String jobId = doc.getFileName();
				Long hId = Long.parseLong(doc.getHistoryId());
				String userId = doc.getUserId();
				String FBUserId = doc.getUserName();
				String filePath = "/home/sa/fb/user/" + jobId;
				long totalUserCount = doc.getViewCount();
				if (FBUserId != null || totalUserCount > 1) {
					log.info("jobId {}", hId);
					Set<String> userIdSet = getUserIds(Long.parseLong(userId), hId);
					int fansCount = userIdSet.size();

					if (fansCount <= 0) {
						log.error("no fans found, jobId: {}", jobId);
						return;
					}

					Map<String, Long> category2Count = new HashMap<String, Long>();
					Map<String, Long> pageId2Count = new HashMap<String, Long>();

					for (String uId : userIdSet) {
						facebookMobileApiUtil.parseAndStatistic(uId, MAX_LIKE_PAGE_COUNT, category2Count, pageId2Count);
					}

					/* Rank category. */
					List<Map.Entry<String, Long>> afterRankingCategory = new ArrayList<Map.Entry<String, Long>>(category2Count.entrySet());
					Collections.sort(afterRankingCategory, new Comparator<Map.Entry<String, Long>>() {
						public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
							Long v1 = o1.getValue();
							Long v2 = o2.getValue();

							if (null != v1 && null != v2)
								return v2 > v1 ? 1 : 0;

							return 0;
						}
					});

					/* Rank like page. */
					List<Map.Entry<String, Long>> afterRankingLikePage = new ArrayList<Map.Entry<String, Long>>(pageId2Count.entrySet());
					Collections.sort(afterRankingLikePage, new Comparator<Map.Entry<String, Long>>() {
						public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
							Long v1 = o1.getValue();
							Long v2 = o2.getValue();

							if (null != v1 && null != v2)
								return v2 > v1 ? 1 : 0;

							return 0;
						}
					});
					int categoryTotalCount = afterRankingCategory.size();
					int likePageTotalCount = afterRankingLikePage.size();

					log.info("fans count:{}, total category count: {}, total like page count:{}", new Object[] { fansCount,
							categoryTotalCount, likePageTotalCount });

					/* Get top 20 category and like page. */
					List<TopCategory> topCategoryList = new ArrayList<TopCategory>();
					List<TopLikePage> topLikePageList = new ArrayList<TopLikePage>();

					StringBuilder sbTopCategory = new StringBuilder();

					/* Top category. */
					int top = 0;
					for (int i = 0; (i < categoryTotalCount) && (top < TOP_COUNT); ++i) {
						String categoryName = afterRankingCategory.get(i).getKey();
						Long count = afterRankingCategory.get(i).getValue();

						if (Pattern.matches("(\\w*\\$+\\w*)|(^\\d+$)", categoryName))
							continue;

						TopCategory topCategory = new TopCategory();
						topCategory.setCategoryName(categoryName);
						topCategory.setInterestFans(count);

						sbTopCategory.append(top + 1).append(",").append(categoryName).append("\n");

						topCategoryList.add(topCategory);

						++top;

					}

					/* Top like page. */
					StringBuilder sbTopLikePage = new StringBuilder();

					top = 0;
					for (int j = 0; (j < likePageTotalCount) && (top < TOP_COUNT); ++j) {
						String likePageId = afterRankingLikePage.get(j).getKey();
						Long count = afterRankingLikePage.get(j).getValue();

						/* Get page info. */
						Page page = facebookMobileApiUtil.getPageByMobileApi(likePageId);

						/* Get images. */
						ImgInfo imgInfo = facebookMobileApiUtil.getPageImgInfoByMobileApi(likePageId);

						sbTopLikePage.append(top + 1).append(",").append(page.getPageName()).append("\n");

						TopLikePage topLikePage = new TopLikePage();
						topLikePage.setCategory(page.getCategory());
						topLikePage.setPageId(likePageId);
						topLikePage.setPageName(page.getPageName());
						topLikePage.setInterestFans(count);
						topLikePage.setPageUrl(page.getPageLink());
						topLikePage.setImgInfo(imgInfo);
						topLikePageList.add(topLikePage);

						++top;

					}

					String topCategory = JsonUtil.getMapper().writeValueAsString(topCategoryList);
					String topLikePage = JsonUtil.getMapper().writeValueAsString(topLikePageList);
					String topVIPUser = JsonUtil.getMapper().writeValueAsString(vipUserList);
					VipMappingHistory vipMappingHistory = vipMappingHistoryRepository.findById(hId);
					vipMappingHistory.setTopCategory(topCategory);
					vipMappingHistory.setTopPage(topLikePage);
					vipMappingHistory.setStatus(1);
					vipMappingHistory.setMatchedUser((long) fansCount);
					vipMappingHistory.setTopVipUser(topVIPUser);
					vipMappingHistoryRepository.saveAndFlush(vipMappingHistory);

					TaskRequest WordCountTaskRequest = buildWordCountTaskRequest(doc, TaskType.FB_VIP_USER);
					emit(input, WordCountTaskRequest, "article parser emit document");

					String resultJson = FacebookApiRequestUtil.getInstance().submitRequest(
							restUrl + "facebook-rest/pub/data/fbvipuser?hid=" + hId.toString() + "&file=" + jobId);

					log.info("result Json {}", resultJson);

					List<Estimation> estmation = estimationRepo.getByTypeAndUserId((short) 5, Long.parseLong(userId), 2);
					if (null != estmation) {
						Estimation est = estmation.get(0);
						est.setActualCompletedTime(new Date());
						estimationRepo.saveAndFlush(est);
					}
				} else {

					VipMappingHistory vipMappingHistory = vipMappingHistoryRepository.findById(hId);
					vipMappingHistory.setStatus(1);
					vipMappingHistoryRepository.saveAndFlush(vipMappingHistory);
					List<Estimation> estmation = estimationRepo.getByTypeAndUserId((short) 5, Long.parseLong(userId), 2);
					if (null != estmation) {
						Estimation est = estmation.get(0);
						est.setActualCompletedTime(new Date());
						estimationRepo.saveAndFlush(est);
					}
				}
				String emailNotificationResult = FacebookApiRequestUtil.getInstance().submitRequest(
						CHCrawlerConfiguration.getInstance().getRestTomcatPath() + "sa-rest/pub/report/notification?userId=" + userId
								+ "&type=" + ReportTypeEnum.VIP_MAPPING_REQUEST.getId());
				log.info("emailNotificationResult {} ", emailNotificationResult);

			} catch (Exception ex) {
				ex.printStackTrace();
				log.error("ex {}", ex.toString());
				TaskResult taskResult = new TaskResult(docResult, "exception when counting", TupleDefinition.Result.FAIL.getCode());
				emit(input, taskResult);
			}
		} else {
			TaskResult taskResult = new TaskResult(docResult, "doc is empty", TupleDefinition.Result.SUCCESS.getCode());
			emit(input, taskResult);
		}

	}

	private TaskRequest buildWordCountTaskRequest(Document doc, TaskType taskType) throws Exception {
		Map<String, String> params = new HashMap<String, String>();
		TaskRequest request = new TaskRequest(UUID.randomUUID().toString(), null, taskType.getCode(), "01",
				RedisDefinition.Priority.NORMAL.getCode(), params);
		request.addParam(TupleDefinition.Param.DOCUMENTS, JsonUtil.getMapper().writeValueAsString(doc));

		return request;
	}

	// private Set<String> getUserIds() {
	// Set<String> userIds = new TreeSet<String>();
	// Map<String, String> userIdsMap = redisUtil.hgetAll("ch-mail-user");
	// Iterator<String> iter = userIdsMap.keySet().iterator();
	// while (iter.hasNext()) {
	// String key = iter.next();
	// String value = userIdsMap.get(key);
	// userIds.add(value);
	// log.info("UserId {}", value);
	// }
	//
	// return userIds;
	// }

	private Set<String> getUserIds(long userId, long jobId) throws JSONException, ApplicationException {
		log.info("123 sourceId {}, jobId {}", userId, jobId);
		Set<String> userIds = new TreeSet<String>();
		vipUserList = new ArrayList<TopVipUser>();
		List<Person> list = new PeopleService().getListOfPeople(jobId);
		int vipUserCount = 0;
		for (int i = 0; i < list.size(); i++) {

			Person p = list.get(i);
			String uId = p.getPersonId().toString();
			userIds.add(uId);
			if (vipUserCount < 30) {
				String name = p.getName();
				String aUrl = getAvatar(uId);
				vipUserList.add(new TopVipUser(uId, aUrl, name));
			}
			vipUserCount++;
		}
		return userIds;
	}

	private String getAvatar(String uId) throws JSONException {
		String result = FacebookApiRequestUtil.getInstance().submitRequest(
				"https://graph.facebook.com/" + uId + "/picture?type=large&redirect=false");
		JSONObject avatarJsonObj = new JSONObject(result);
		String aUrl = avatarJsonObj.optJSONObject("data").optString("url");
		return aUrl;
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		// TODO Auto-generated method stub
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in fliter doc", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

}
