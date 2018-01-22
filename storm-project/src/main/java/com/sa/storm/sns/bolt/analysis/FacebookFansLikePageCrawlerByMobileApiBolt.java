package com.sa.storm.sns.bolt.analysis;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.common.json.JsonUtil;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.common.ConstantPool;
import com.sa.storm.sns.domain.Fans;
import com.sa.storm.sns.domain.FansAndLikePagesResult;
import com.sa.storm.sns.domain.PageSummaryResult;
import com.sa.storm.sns.util.FacebookMobileApiUtil;

public class FacebookFansLikePageCrawlerByMobileApiBolt extends BaseBolt {

	private static final long serialVersionUID = 818464233051979334L;

	private static final Logger log = LoggerFactory.getLogger(FacebookFansLikePageCrawlerByMobileApiBolt.class);

	private static final int MAX_LIKE_PAGE_COUNT = 1000;

	private RedisUtil redisUtil;

	private FacebookMobileApiUtil facebookMobileApiUtil;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			// PythonCommandUtil.getInstance().deployTemplates(getGlobalTaskId());
			redisUtil = RedisUtil.getInstance();
			facebookMobileApiUtil = FacebookMobileApiUtil.getInstance();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, FansAndLikePagesResult.class);

	}

	@Override
	public void process(Tuple input) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		final String jobUserId = request.getParamByKey(TupleDefinition.Param.USER_ID);
		final String fansJson = request.getParamByKey(TupleDefinition.Param.PAGE_FANS);

		if (!StringUtils.isEmpty(jobUserId) && !StringUtils.isEmpty(fansJson)) {
			Fans fans = JsonUtil.getMapper().readValue(fansJson, Fans.class);
			String pageId = fans.getPageId();
			String userId = fans.getUserId();

			Map<String, Long> category2Count = new HashMap<String, Long>();
			Map<String, Long> pageId2Count = new HashMap<String, Long>();

			facebookMobileApiUtil.parseAndStatistic(userId, MAX_LIKE_PAGE_COUNT, category2Count, pageId2Count);

			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
			String date = df.format(cal.getTime());
			String key = ConstantPool.FANS_INTEREST_PREFIX + date;

			/* Update redis. */
			String value = redisUtil.getField(key, pageId);
			PageSummaryResult pageSummaryResult = JsonUtil.getMapper().readValue(value, PageSummaryResult.class);

			for (Map.Entry<String, Long> entry : category2Count.entrySet()) {
				pageSummaryResult.addCategoryCount(entry.getKey(), entry.getValue());
			}

			for (Map.Entry<String, Long> entry : pageId2Count.entrySet()) {
				pageSummaryResult.addPageCount(entry.getKey(), entry.getValue());
			}

			pageSummaryResult.decrFansNoHandledCount(1);
			redisUtil.saveField(key, pageId, JsonUtil.getMapper().writeValueAsString(pageSummaryResult));

			/* Emit request. */
			FansAndLikePagesResult fansAndLikePagesResult = new FansAndLikePagesResult(request);
			fansAndLikePagesResult.setFans(fans);
			fansAndLikePagesResult.addParam(TupleDefinition.Param.USER_ID, jobUserId);
			emit(input, fansAndLikePagesResult, "Emit FansAndLikePagesResult to StatisticFansInterestBolt");

		} else {
			TaskResult taskResult = new TaskResult(request, "jobUserId or fans is empty", TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
		}

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in crawling fans like page", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);

	}

}
