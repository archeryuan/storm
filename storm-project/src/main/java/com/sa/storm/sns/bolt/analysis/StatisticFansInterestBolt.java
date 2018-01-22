package com.sa.storm.sns.bolt.analysis;

import java.io.File;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.sa.common.definition.SourceType;
import com.sa.common.json.JsonUtil;
import com.sa.competitorhunter.dao.FansInterestRepository;
import com.sa.competitorhunter.dao.PageRepository;
import com.sa.competitorhunter.domain.FansInterest;
import com.sa.competitorhunter.domain.Page;
import com.sa.competitorhunter.service.EstimationService;
import com.sa.crawler.definition.TaskType;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.ca.domain.ImgInfo;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.common.ConstantPool;
import com.sa.storm.sns.domain.FanPage;
import com.sa.storm.sns.domain.Fans;
import com.sa.storm.sns.domain.FansAndLikePagesResult;
import com.sa.storm.sns.domain.KeyValuePair;
import com.sa.storm.sns.domain.KeyValuePairComparator;
import com.sa.storm.sns.domain.ParserResult;
import com.sa.storm.sns.domain.TopCategory;
import com.sa.storm.sns.domain.TopLikePage;
import com.sa.storm.sns.parser.BaseParser;
import com.sa.storm.sns.parser.ParserFactory;

public class StatisticFansInterestBolt extends BaseBolt {

	private static final long serialVersionUID = -7442013439435489706L;

	private static final Logger log = LoggerFactory.getLogger(StatisticFansInterestBolt.class);

	private RedisUtil redisUtil;

	private PageRepository pageRepo;
	
	private FansInterestRepository fansInterestRepo;
	
	private EstimationService estimationService;
	
	private BaseParser<ImgInfo> parser;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			redisUtil = RedisUtil.getInstance();
			pageRepo = getSpringContext().getBean(PageRepository.class);
			fansInterestRepo = getSpringContext().getBean(FansInterestRepository.class);
			estimationService =  getSpringContext().getBean(EstimationService.class);
			parser = ParserFactory.getInstance().getParser(SourceType.FACEBOOK.getSourceTypeStr(), TaskType.FB_PAGE_IMG.getCode(), ImgInfo.class);

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
			List<FanPage> likePageList = fansAndLikePagesResult.getLikePages();
			String myPageId = fans.getPageId();

			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
			String date = df.format(cal.getTime());

			Set<String> categoryKeys = new HashSet<String>();
			Set<String> likePageKeys = new HashSet<String>();

			String likePageId;
			String likePageName;
			String likePageCategory;
			String likePageUrl;
			String key;

			for (FanPage likePage : likePageList) {
				likePageId = likePage.getPageId();
				likePageName = likePage.getPageName();
				likePageCategory = likePage.getCategory();
				likePageUrl = likePage.getPageUrl();
				
				/*Store category key.*/
				key = ConstantPool.FANS_INTEREST_PREFIX + date + "_" + myPageId + "_" + likePageCategory;
				categoryKeys.add(key);
				
				/* Statistic like page. */
				key = ConstantPool.FANS_INTEREST_PREFIX + date + "_" + myPageId + "_" + likePageId;
				likePageKeys.add(key);
				redisUtil.addCount(key, ConstantPool.LIKE_PAGE_FANS_COUNT, 1);
				redisUtil.saveField(key, ConstantPool.LIKE_PAGE_NAME, likePageName);
				redisUtil.saveField(key, ConstantPool.LIKE_PAGE_CATEGORY, likePageCategory);
				redisUtil.saveField(key, ConstantPool.LIKE_PAGE_URL, likePageUrl);
			}

			/* Statistic category, each category increase once for one fans which is different from statistic like page. */
			for (String categoryKey : categoryKeys) {
				redisUtil.incr(categoryKey);
			}
			
			/* Check redis. */
			key = ConstantPool.FANS_INTEREST_PREFIX + date + "_" + myPageId;
			long totalFansCount = Long.parseLong(redisUtil.getField(key, ConstantPool.PAGE_TOTAL_FANS_COUNT));
			long currentFansCount = Long.parseLong(redisUtil.getField(key, ConstantPool.PAGE_CURRENT_FANS_COUNT));

			/* Completed. */
			if (currentFansCount <= 0) {				
				/* Rank category and like page. */
				List<KeyValuePair> categoryStatistics = new ArrayList<KeyValuePair>();
				List<KeyValuePair> likePageStatistics = new ArrayList<KeyValuePair>();

				for (String categoryKey : categoryKeys) {
					long value = Long.parseLong(redisUtil.get(categoryKey));
					KeyValuePair keyValuePair = new KeyValuePair();
					keyValuePair.setKey(categoryKey);
					keyValuePair.setValue(value);

					categoryStatistics.add(keyValuePair);
				}

				for (String likePageKey : likePageKeys) {
					long value = Long.parseLong(redisUtil.getField(likePageKey, ConstantPool.LIKE_PAGE_FANS_COUNT));
					KeyValuePair keyValuePair = new KeyValuePair();
					keyValuePair.setKey(likePageKey);
					keyValuePair.setValue(value);

					likePageStatistics.add(keyValuePair);
				}

				KeyValuePairComparator keyValuePairComparator = new KeyValuePairComparator();
				Collections.sort(categoryStatistics, keyValuePairComparator);
				Collections.sort(likePageStatistics, keyValuePairComparator);

				/* Store top results into database. */
				int categorySize = categoryStatistics.size();
				int likePageSize = likePageStatistics.size();

				List<TopCategory> topCategoryList = new ArrayList<TopCategory>();
				List<TopLikePage> topLikePageList = new ArrayList<TopLikePage>();

				StringBuilder sbTopCategory = new StringBuilder();
				TopCategory topCategory;
				TopLikePage topLikePage;
				String categoryName;
				long value;
				
				/*Top category rank.*/
				int top = 0;
				for (int i = categorySize - 1; (i >= 0) && (top < 20); --i, ++top) {
					key = categoryStatistics.get(i).getKey(); // "ch-interest-" + date + "_" + myPageId + "_" + likePageCategory;
					value = categoryStatistics.get(i).getValue();
					categoryName = key.split("_")[2];
					
					if (Pattern.matches("(\\w*\\$+\\w*)|(^\\d+$)", categoryName))
						continue;

					sbTopCategory.append(top + 1).append(",").append(categoryName).append("\n");
					
					topCategory = new TopCategory();
					topCategory.setCategoryName(categoryName);
					topCategory.setInterestFans(value);

					topCategoryList.add(topCategory);

				}

				/*Top like page rank.*/
				StringBuilder sbTopLikePage = new StringBuilder();
				ImgInfo imgInfo = null;
				top = 0;
				for (int j = likePageSize - 1; (j >= 0) && (top < 20); --j, ++top) {
					key = likePageStatistics.get(j).getKey();// "ch-interest-" + date + "_" + myPageId + "_" + likePageId
					value = likePageStatistics.get(j).getValue();
						
					likePageId = key.split("_")[2];
					likePageName = redisUtil.getField(key, ConstantPool.LIKE_PAGE_NAME);
					likePageName =  URLDecoder.decode(likePageName, "utf-8");
					likePageCategory = redisUtil.getField(key, ConstantPool.LIKE_PAGE_CATEGORY);
					likePageUrl = redisUtil.getField(key, ConstantPool.LIKE_PAGE_URL);
					
					if (Pattern.matches("(\\w*\\$+\\w*)|(^\\d+$)", likePageName) || Pattern.matches("(\\w*\\$+\\w*)|(^\\d+$)", likePageCategory))
						continue;
										
					/*Get images.*/
					try {
						TaskRequest request = new TaskRequest("C1", null, TaskType.FB_PAGE_IMG.getCode(), SourceType.FACEBOOK.getSourceTypeStr(), new HashMap<String, String>());
						request.addParam(TupleDefinition.Param.USER_ID, likePageId);

						ParserResult<ImgInfo> result = parser.parse(request, "", 0);
						Collection<ImgInfo> col = result.getResults();
						imgInfo = (ImgInfo)col.toArray()[0];										
						
					} catch (Exception e) {
						log.error(e.getMessage(), e);
					}
					
					sbTopLikePage.append(top + 1).append(",").append(likePageName).append("\n");

					
					topLikePage = new TopLikePage();
					topLikePage.setPageId(likePageId);
					topLikePage.setPageName(likePageName);
					topLikePage.setCategory(likePageCategory);
					topLikePage.setPageUrl(likePageUrl);
					topLikePage.setInterestFans(value);
					topLikePage.setImgInfo(imgInfo);

					topLikePageList.add(topLikePage);

				}

				Page myPage = pageRepo.findByPageId(myPageId);
				String myPageName = myPage.getPageName();
				
				/*Upload file.*/
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-SSS");
				String fileDirectory = "/home/sa/facebook/python/interest/";
				String fileName = myPageName + "_" + "fansInterest" + "_" + sdf.format(cal.getTime()) + ".csv";
				String reusltFilePath = fileDirectory + fileName;
				
				List<String> lines = new ArrayList<String>();
				byte[] bom = {(byte)0xEF, (byte)0xBB, (byte)0xBF};
				lines.add(new String(bom));
				
				StringBuilder header = new StringBuilder("");
				header.append("Top 20 categories that " + myPageName + "'s Fans interested\n");
				header.append("Ranking").append(",").append("Category").append("\n");
				
				lines.add(header.toString());
				sbTopCategory.append("--------------------------------------------------------------------------------------------");
				lines.add(sbTopCategory.toString());
				
				header = new StringBuilder("");
				header.append("Top 20 pages that "  + myPageName + "'s Fans interested\n");
				header.append("Ranking").append(",").append("Fan Pages").append("\n");
				
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
				fansInterest.setStatisticDate(statisticDate);				
				fansInterest.setPageId(pageIdLong);
				fansInterest.setCreateDate(now);					
				fansInterest.setUpdateDate(now);
				fansInterest.setFileName(fileName);
				fansInterest.setExportUrl(exportUrl);
				fansInterest.setTotalFans(totalFansCount);
				fansInterest.setTotalCategory(categorySize);
				fansInterest.setTotalLikePage(likePageSize);
				fansInterest.setStatus(FansInterest.STATUS_COMPLETED);
				
				String json = JsonUtil.getMapper().writeValueAsString(topCategoryList);
				fansInterest.setTopCategory(json);
				
				json = JsonUtil.getMapper().writeValueAsString(topLikePageList);
				fansInterest.setTopLikePage(json);

				fansInterestRepo.saveAndFlush(fansInterest);
				
				log.info("total category count: {}, total like page count:{}", new Object[] { categorySize, likePageSize });

				/* Update estimation. */
				estimationService.setCompleted(userId, (short)2);
				
				
				/**
				 * Clear useless redis key.
				 * Note: has problem!*/
				/*String pattern = ConstantPool.FANS_INTEREST_PREFIX + date + "_" + myPageId + "*";
				Set<String> keys = redisUtil.keys(pattern);
				if (null != keys && keys.size() > 0)
					redisUtil.del(keys.toArray(new String[keys.size()]));*/

			}

		} else {
			TaskResult taskResult = new TaskResult(fansAndLikePagesResult, "fansAndLikePagesResult or jobUserId is null ",
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
