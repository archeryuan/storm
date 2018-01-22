package com.sa.storm.sns.bolt.analysis;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
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
import com.sa.storm.sns.domain.FanPage;
import com.sa.storm.sns.domain.Fans;
import com.sa.storm.sns.domain.FansAndLikePagesResult;

public class FacebookFansLikePageCrawlerBolt extends BaseBolt {

	private static final long serialVersionUID = 818464233051979334L;

	private static final Logger log = LoggerFactory.getLogger(FacebookFansLikePageCrawlerBolt.class);

	private RedisUtil redisUtil;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			// PythonCommandUtil.getInstance().deployTemplates(getGlobalTaskId());
			redisUtil = RedisUtil.getInstance();
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
			String requestUrl = "https://www.facebook.com/search/" + userId + "/pages-liked";
			String gender = "male";
			String location = "HongKong";
			String age = "30";
			String fileDirectory = "/home/sa/facebook/python/interest/";
			String pyFilePath = fileDirectory + "fb-like-page.py";
			String fileName = pageId + "_" + userId + "_" + UUID.randomUUID().toString();
			String reusltFilePath = fileDirectory + fileName;

			int exitValue = executePythonCommand(pyFilePath, requestUrl, reusltFilePath, age, location, gender);
			
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
			String date = df.format(cal.getTime());
			String key = ConstantPool.FANS_INTEREST_PREFIX + date + "_" + pageId;

			if (0 == exitValue) {
				File file = new File(reusltFilePath);
				List<String> likePageList = FileUtils.readLines(file, "utf-8");

				List<FanPage> fanPageList = new ArrayList<FanPage>();			
				for (String likePage : likePageList) {
					String[] info = StringUtils.split(likePage, ",");
					FanPage fanPage = new FanPage();
					fanPage.setPageId(info[0]);
					fanPage.setPageName(info[1]);
					fanPage.setCategory(info[2]);
					fanPage.setPageUrl(info[3]);
					fanPageList.add(fanPage);
				}

				log.info("like page count: {}, userId:{}", new Object[] { fanPageList.size(), userId });

				FansAndLikePagesResult fansAndLikePagesResult = new FansAndLikePagesResult(request);
				fansAndLikePagesResult.setFans(fans);
				fansAndLikePagesResult.setLikePages(fanPageList);
				fansAndLikePagesResult.addParam(TupleDefinition.Param.USER_ID, jobUserId);
				emit(input, fansAndLikePagesResult, "Emit FansAndLikePagesResult to StatisticFansInterestBolt");

				/* Update redis. */
				redisUtil.addCount(key, ConstantPool.PAGE_CURRENT_FANS_COUNT, -1);

				/* Delete file. */
				FileUtils.deleteQuietly(file);

			} else {
				/* Update redis. */				
				redisUtil.addCount(key, ConstantPool.PAGE_CURRENT_FANS_COUNT, -1);

				TaskResult taskResult = new TaskResult(request, "execute python error ", TupleDefinition.Result.FAIL.getCode());
				emit(input, taskResult);
			}

		} else {
			TaskResult taskResult = new TaskResult(request, "jobUserId or fans is null", TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
		}

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);		
		final String fansJson = request.getParamByKey(TupleDefinition.Param.PAGE_FANS);
		if (!StringUtils.isEmpty(fansJson)) {
			Fans fans = JsonUtil.getMapper().readValue(fansJson, Fans.class);
			String pageId = fans.getPageId();
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
			String date = df.format(cal.getTime());
			
			/* Update redis. */
			String key = ConstantPool.FANS_INTEREST_PREFIX + date + "_" + pageId;
			redisUtil.addCount(key, ConstantPool.PAGE_CURRENT_FANS_COUNT, -1);
		}
		
		TaskResult error = new TaskResult(request, "Error in crawling fans like page", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);

		

	}

	private int executePythonCommand(String pyFilePath, String url, String filePath, String age, String location, String gender)
			throws IOException, InterruptedException {
		List<String> cmd = new ArrayList<String>();
		cmd.add("python");
		cmd.add(pyFilePath);
		cmd.add("-url");
		cmd.add(url);
		cmd.add("-file");
		cmd.add(filePath);
		/*
		 * cmd.add("-age"); cmd.add(age); cmd.add("-livein"); cmd.add(location); cmd.add("-gender"); cmd.add(gender);
		 */
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);

		Process process = null;

		process = pb.start();

		int w = process.waitFor();
		log.info("existValue:" + w);

		return w;
	}

}
