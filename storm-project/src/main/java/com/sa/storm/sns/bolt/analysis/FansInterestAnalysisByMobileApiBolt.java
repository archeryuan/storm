package com.sa.storm.sns.bolt.analysis;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.CHCrawlerConfiguration;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.ch.aws.s3.FileUploader;
import com.sa.common.json.JsonUtil;
import com.sa.competitorhunter.dao.FansInterestRepository;
import com.sa.competitorhunter.dao.PageRepository;
import com.sa.competitorhunter.definition.ReportTypeEnum;
import com.sa.competitorhunter.domain.FansInterest;
import com.sa.competitorhunter.domain.Page;
import com.sa.competitorhunter.service.EstimationService;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.ca.domain.ImgInfo;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.domain.TopCategory;
import com.sa.storm.sns.domain.TopLikePage;
import com.sa.storm.sns.util.ExcelUtil;
import com.sa.storm.sns.util.ExcelUtil.ExcelRow;
import com.sa.storm.sns.util.FacebookMobileApiUtil;

public class FansInterestAnalysisByMobileApiBolt extends BaseBolt {

	private static final long serialVersionUID = -7442013439435489706L;

	private static final Logger log = LoggerFactory.getLogger(FansInterestAnalysisByMobileApiBolt.class);
	
	private static final String RESULT_FILE_DIRECTORY = "/home/sa/facebook/interest_analysis_result/";

	private static final int MAX_USER_COUNT = 100;

	private static final int MAX_LIKE_PAGE_COUNT = 1000;

	private static final int TOP_COUNT = 20;

	private FacebookMobileApiUtil facebookMobileApiUtil;

	private PageRepository pageRepo;

	private FansInterestRepository fansInterestRepo;

	private EstimationService estimationService;

	private ExcelUtil excelUtil;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			facebookMobileApiUtil = FacebookMobileApiUtil.getInstance();
			pageRepo = getSpringContext().getBean(PageRepository.class);
			fansInterestRepo = getSpringContext().getBean(FansInterestRepository.class);
			estimationService = getSpringContext().getBean(EstimationService.class);
			excelUtil = ExcelUtil.getInstance();
			
			File directory = new File(RESULT_FILE_DIRECTORY);
			if (!directory.exists()) {
				FileUtils.forceMkdir(directory);
			}			

		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void process(Tuple input) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		final String pageId = request.getParamByKey(TupleDefinition.Param.PAGE_ID);
		final String jobUserId = request.getParamByKey(TupleDefinition.Param.USER_ID);

		Page myPage = pageRepo.findByPageId(pageId);
		String myPageName = myPage.getPageName();

		Date start = new Date();

		if (!StringUtils.isEmpty(pageId)) {

			Set<String> userIdSet = facebookMobileApiUtil.getUserIds(pageId, MAX_USER_COUNT);
			int fansCount = userIdSet.size();

			log.info("fans count:{}, pageId: {}", new Object[] { fansCount, pageId });

			if (fansCount <= 0) {
				log.error("no fans found, pageId: {}", pageId);
				return;
			}

			Map<String, Long> category2Count = new HashMap<String, Long>();
			Map<String, Long> pageId2Count = new HashMap<String, Long>();

			int num = 1;
			for (String userId : userIdSet) {
				log.info("Statistic No: {}, userId: {}", num, userId);
				facebookMobileApiUtil.parseAndStatistic(userId, MAX_LIKE_PAGE_COUNT, category2Count, pageId2Count);
				++num;
			}

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

			log.info("fans count:{}, total category count: {}, total like page count:{}", new Object[] { fansCount, categoryTotalCount,
					likePageTotalCount });

			/* Get top 20 category and like page. */
			List<TopCategory> topCategoryList = new ArrayList<TopCategory>();
			List<TopLikePage> topLikePageList = new ArrayList<TopLikePage>();

			List<ExcelRow> topCategorySheetHeader = new ArrayList<ExcelRow>();
			List<ExcelRow> topLikePageSheetHeader = new ArrayList<ExcelRow>();
			List<ExcelRow> topCategorySheetContent = new ArrayList<ExcelRow>();
			List<ExcelRow> topLikePageSheetContent = new ArrayList<ExcelRow>();

			ExcelRow excelRow = new ExcelRow();
			excelRow.add(0, "Top 20 categories that " + myPageName + "'s Fans interested");
			topCategorySheetHeader.add(excelRow);

			excelRow = new ExcelRow();
			excelRow.add(0, "Ranking");
			excelRow.add(1, "Category");
			topCategorySheetHeader.add(excelRow);

			excelRow = new ExcelRow();
			excelRow.add(0, "Top 20 pages that " + myPageName + "'s Fans interested");
			topLikePageSheetHeader.add(excelRow);

			excelRow = new ExcelRow();
			excelRow.add(0, "Ranking");
			excelRow.add(1, "Fan Page");
			topLikePageSheetHeader.add(excelRow);

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

				topCategoryList.add(topCategory);

				excelRow = new ExcelRow();
				excelRow.add(0, String.valueOf(top + 1));
				excelRow.add(1, categoryName);
				topCategorySheetContent.add(excelRow);

				++top;

			}

			/* Top like page. */
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

				TopLikePage topLikePage = new TopLikePage();
				topLikePage.setCategory(page.getCategory());
				topLikePage.setPageId(likePageId);
				topLikePage.setPageName(page.getPageName());
				topLikePage.setInterestFans(count);
				topLikePage.setPageUrl(page.getPageLink());
				topLikePage.setImgInfo(imgInfo);

				topLikePageList.add(topLikePage);

				excelRow = new ExcelRow();
				excelRow.add(0, String.valueOf(top + 1));
				excelRow.add(1, page.getPageName());
				topLikePageSheetContent.add(excelRow);

				++top;

			}

			/* Upload file. */
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-SSS");
			String fileName = myPageName + "_" + "fansInterest" + "_" + sdf.format(cal.getTime()) + ".xls";
			String reusltFilePath = RESULT_FILE_DIRECTORY + fileName;			
			
			excelUtil.saveExcel(reusltFilePath, "Top Category", false, true, topCategorySheetHeader, topCategorySheetContent);
			excelUtil.saveExcel(reusltFilePath, "Top Like Page", false, true, topLikePageSheetHeader, topLikePageSheetContent);

			File file = new File(reusltFilePath);
			String exportUrl = FileUploader.uploadFile(file, true, true);

			/* Store top category and like page record into database. */
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
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
			fansInterest.setTotalFans(fansCount);
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
			TaskResult taskResult = new TaskResult(request, "pageId is empty ", TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
		}

		Date end = new Date();
		long cost = end.getTime() - start.getTime();
		log.info("cost time(minutes): {}, userId: {}, pageId: {}", new Object[] { cost / 1000 / 60, jobUserId, pageId });

		/* Email alert. */
		String emailNotificationResult = facebookMobileApiUtil.submitRequest(CHCrawlerConfiguration.getInstance().getRestTomcatPath()
				+ "sa-rest/pub/report/notification?userId=" + jobUserId + "&type=" + ReportTypeEnum.FANS_INTERESTS_ANALYSIS.getId());

		log.info("emailNotificationResult {} ", emailNotificationResult);

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in FansInterestAnalysisByMobileApiBolt", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

}
