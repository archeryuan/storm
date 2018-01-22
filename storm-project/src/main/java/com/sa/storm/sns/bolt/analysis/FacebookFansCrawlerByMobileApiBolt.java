package com.sa.storm.sns.bolt.analysis;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.common.definition.SourceType;
import com.sa.common.json.JsonUtil;
import com.sa.crawler.definition.TaskType;
import com.sa.redis.definition.RedisDefinition.Priority;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.common.ConstantPool;
import com.sa.storm.sns.domain.Fans;
import com.sa.storm.sns.domain.PageSummaryResult;
import com.sa.storm.sns.util.FacebookMobileApiUtil;

public class FacebookFansCrawlerByMobileApiBolt extends BaseBolt {

	private static final long serialVersionUID = -3479122763163146919L;

	private static final Logger log = LoggerFactory.getLogger(FacebookFansCrawlerByMobileApiBolt.class);

	private static final int MAX_USER_COUNT = 100;

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
		declareOutputByClass(declarer, TaskRequest.class);
	}

	@Override
	public void process(Tuple input) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		final String pageId = request.getParamByKey(TupleDefinition.Param.PAGE_ID);
		final String jobUserId = request.getParamByKey(TupleDefinition.Param.USER_ID);

		if (!StringUtils.isEmpty(pageId)) {

			Set<String> userIdSet = facebookMobileApiUtil.getUserIds(pageId, MAX_USER_COUNT);
			int fansCount = userIdSet.size();

			log.info("fans count:{}, pageId: {}", new Object[] { fansCount, pageId });

			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
			String date = df.format(cal.getTime());
			String key = ConstantPool.FANS_INTEREST_PREFIX + date;
			
			/* Update redis. */
			PageSummaryResult pageSummaryResult;
			String value = redisUtil.getField(key, pageId);
			if (StringUtils.isEmpty(value) || "nil".equals(value)) {
				pageSummaryResult = new PageSummaryResult();
			} else {
				pageSummaryResult = JsonUtil.getMapper().readValue(value, PageSummaryResult.class);
			}

			pageSummaryResult.addFansTotalCount(fansCount);
			pageSummaryResult.addFansNoHandledCount(fansCount);

			redisUtil.saveField(key, pageId, JsonUtil.getMapper().writeValueAsString(pageSummaryResult));
			

			/*Emit fans like page request.*/
			for (String userId : userIdSet) {
				Fans fans = new Fans();
				fans.setPageId(pageId);
				fans.setUserId(userId);

				TaskRequest fanLikePageRequest = new TaskRequest(UUID.randomUUID().toString(), null, TaskType.FB_FANS.getCode(),
						SourceType.FACEBOOK.getSourceTypeStr(), new Integer(Priority.NORMAL.getCode()), new HashMap<String, String>());
				fanLikePageRequest.addParam(TupleDefinition.Param.USER_ID, jobUserId);
				fanLikePageRequest.addParam(TupleDefinition.Param.PAGE_FANS, JsonUtil.getMapper().writeValueAsString(fans));
				emit(input, fanLikePageRequest, "Submit fan like page request to Redis");

			}			

		} else {
			TaskResult taskResult = new TaskResult(request, "pageId is empty ", TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
		}

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in crawling fans", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

	

}
