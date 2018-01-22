package com.sa.storm.sns.bolt.analysis;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.ch.aws.s3.FileUploader;
import com.sa.common.json.JsonUtil;
import com.sa.competitorhunter.dao.FansInterestRepository;
import com.sa.competitorhunter.dao.PageRepository;
import com.sa.competitorhunter.domain.FansInterest;
import com.sa.competitorhunter.domain.Page;
import com.sa.competitorhunter.service.EstimationService;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.ca.domain.ImgInfo;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.common.ConstantPool;
import com.sa.storm.sns.domain.Fans;
import com.sa.storm.sns.domain.FansAndLikePagesResult;
import com.sa.storm.sns.domain.PageSummaryResult;
import com.sa.storm.sns.domain.TopCategory;
import com.sa.storm.sns.domain.TopLikePage;
import com.sa.storm.sns.util.FacebookMobileApiUtil;

public class StatisticFansInterestByMobileApiBolt extends BaseBolt {

	private static final long serialVersionUID = -7442013439435489706L;

	private static final Logger log = LoggerFactory.getLogger(StatisticFansInterestByMobileApiBolt.class);

	private static final int TOP_COUNT = 20;

	private FacebookMobileApiUtil facebookMobileApiUtil;

	private RedisUtil redisUtil;

	private PageRepository pageRepo;

	private FansInterestRepository fansInterestRepo;

	private EstimationService estimationService;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			facebookMobileApiUtil = FacebookMobileApiUtil.getInstance();
			redisUtil = RedisUtil.getInstance();
			pageRepo = getSpringContext().getBean(PageRepository.class);
			fansInterestRepo = getSpringContext().getBean(FansInterestRepository.class);
			estimationService = getSpringContext().getBean(EstimationService.class);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void process(Tuple input) throws Exception {
		FansAndLikePagesResult fansAndLikePagesResult = (FansAndLikePagesResult) getInputByClass(input, FansAndLikePagesResult.class);
		final String jobUserId = fansAndLikePagesResult.getParamByKey(TupleDefinition.Param.USER_ID);

		if (null != fansAndLikePagesResult && !StringUtils.isEmpty(jobUserId)) {
			Fans fans = fansAndLikePagesResult.getFans();
			String pageId = fans.getPageId();

			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
			String date = df.format(cal.getTime());
			String key = ConstantPool.FANS_INTEREST_PREFIX + date;

			/* Check redis. */
			String value = redisUtil.getField(key, pageId);
			if (StringUtils.isEmpty(value) || "nil".equals(value)) {
				log.error("page not crawl, pageId: {}", pageId);
				return;
			}

			PageSummaryResult pageSummaryResult = JsonUtil.getMapper().readValue(value, PageSummaryResult.class);
			long fansNoHandledCount = pageSummaryResult.getFansNoHandledCount();
			long fansTotalCount = pageSummaryResult.getFansTotalCount();
			log.info("pageId: {}, fansNoHandledCount: {}, fansTotalCount: {}", new Object[] { pageId, fansNoHandledCount, fansTotalCount });

			if (fansNoHandledCount > 0)
				return;

			Map<String, Long> category2Count = pageSummaryResult.getCategory2Count();
			Map<String, Long> pageId2Count = pageSummaryResult.getPageId2Count();

			/* Rank category. */
			List<Map.Entry<String, Long>> afterRankingCategory = new ArrayList<Map.Entry<String, Long>>(category2Count.entrySet());
			Collections.sort(afterRankingCategory, new Comparator<Map.Entry<String, Long>>() {
				public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
					Long v1 = o1.getValue();
					Long v2 = o2.getValue();

					if (null != v1 && null != v2)
						return v1 == v2 ? 0 : ((v1 < v2) ? 1 : -1);

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
						return v1 == v2 ? 0 : ((v1 < v2) ? 1 : -1);

					return 0;
				}
			});
			int categoryTotalCount = afterRankingCategory.size();
			int likePageTotalCount = afterRankingLikePage.size();

			log.info("fans count:{}, total category count: {}, total like page count:{}", new Object[] { fansTotalCount,
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

				if (pageId.equals(likePageId))
					continue;

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

			Page myPage = pageRepo.findByPageId(pageId);
			String myPageName = myPage.getPageName();

			/* Upload file. */
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-SSS");
			String fileDirectory = "/home/sa/facebook/interest_analysis_result/";
			String fileName = myPageName + "_" + "fansInterest" + "_" + sdf.format(cal.getTime()) + ".csv";
			String reusltFilePath = fileDirectory + fileName;

			List<String> lines = new ArrayList<String>();
			byte[] bom = { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };
			lines.add(new String(bom));

			StringBuilder header = new StringBuilder("");
			header.append("Top 20 categories that " + myPageName + "'s Fans interested\n");
			header.append("Ranking").append(",").append("Category");

			lines.add(header.toString());
			sbTopCategory.append("--------------------------------------------------------------------------------------------");
			lines.add(sbTopCategory.toString());

			header = new StringBuilder("");
			header.append("Top 20 pages that " + myPageName + "'s Fans interested\n");
			header.append("Ranking").append(",").append("Fan Pages");

			lines.add(header.toString());
			lines.add(sbTopLikePage.toString());

			File file = new File(reusltFilePath);
			FileUtils.writeLines(file, "utf-8", lines, "\n");
			String exportUrl = FileUploader.uploadFile(file, true, true);

			/* Store top category and like page record into database. */
			Date now = Calendar.getInstance().getTime();
			long userId = Long.parseLong(jobUserId);
			long pageIdLong = myPage.getId();
			String statisticDate = df.format(now);
			
			FansInterest fansInterest = new FansInterest();
			fansInterest.setUserId(userId);
			fansInterest.setPageId(pageIdLong);
			fansInterest.setStatisticDate(statisticDate);
			fansInterest.setCreateDate(now);
			fansInterest.setUpdateDate(now);
			fansInterest.setExportUrl(exportUrl);
			fansInterest.setFileName(fileName);
			fansInterest.setFileName(fileName);
			fansInterest.setTotalFans(fansTotalCount);
			fansInterest.setTotalCategory(categoryTotalCount);
			fansInterest.setTotalLikePage(likePageTotalCount);
			fansInterest.setStatus(FansInterest.STATUS_COMPLETED);

			String json = JsonUtil.getMapper().writeValueAsString(topCategoryList);
			fansInterest.setTopCategory(json);

			json = JsonUtil.getMapper().writeValueAsString(topLikePageList);
			fansInterest.setTopLikePage(json);

			fansInterestRepo.saveAndFlush(fansInterest);

			/* Update estimation. */
			estimationService.setCompleted(userId, (short) 2);

			log.info("Finish, userId: {}, pageId: {}", userId, pageId);

		} else {
			TaskResult taskResult = new TaskResult(fansAndLikePagesResult, "fansAndLikePagesResult or jobUserId is empty ",
					TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
		}

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		FansAndLikePagesResult fansAndLikePagesResult = (FansAndLikePagesResult) getInputByClass(input, FansAndLikePagesResult.class);
		TaskResult error = new TaskResult(fansAndLikePagesResult, "Error in statistic fans interest", e,
				TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

}
