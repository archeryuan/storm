package com.sa.storm.sns.bolt.analysis;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
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

public class FacebookFansCrawlerBolt extends BaseBolt {

	private static final long serialVersionUID = -3479122763163146919L;

	private static final Logger log = LoggerFactory.getLogger(FacebookFansCrawlerBolt.class);
	
	private RedisUtil redisUtil;

	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			//PythonCommandUtil.getInstance().deployTemplates(getGlobalTaskId());
			redisUtil = RedisUtil.getInstance();

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
			String requestUrl = "https://www.facebook.com/search/" + pageId + "/likers";
			String gender = "male";
			String location = "HongKong";
			String age = "30";
			String fileDirectory = "/home/sa/facebook/python/interest/";
			String pyFilePath = fileDirectory + "fb-fans.py";
			String fileName = pageId + "_" + UUID.randomUUID().toString();
			String reusltFilePath = fileDirectory + fileName;

			int exitValue = executePythonCommand(pyFilePath, requestUrl, reusltFilePath, age, location, gender);

			if (0 == exitValue) {
				File file = new File(reusltFilePath);
				List<String> userIdList = FileUtils.readLines(file, "utf-8");
				
				log.info("fans count:{}, pageId: {}", new Object[]{userIdList.size(), pageId});
				
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				Calendar cal = Calendar.getInstance();
				String date = df.format(cal.getTime());
				String key;				
				for (String userId : userIdList) {
					Fans fans = new Fans();
					fans.setPageId(pageId);
					fans.setUserId(userId);

					TaskRequest fanLikePageRequest = new TaskRequest(UUID.randomUUID().toString(), null, TaskType.FB_FANS.getCode(),
							SourceType.FACEBOOK.getSourceTypeStr(), new Integer(Priority.NORMAL.getCode()), new HashMap<String, String>());
					fanLikePageRequest.addParam(TupleDefinition.Param.USER_ID, jobUserId);
					fanLikePageRequest.addParam(TupleDefinition.Param.PAGE_FANS, JsonUtil.getMapper().writeValueAsString(fans));
					emit(input, fanLikePageRequest, "Submit fan like page request to Redis");					

					/* Update redis. */
					key = ConstantPool.FANS_INTEREST_PREFIX + date + "_" + pageId;
					redisUtil.addCount(key, ConstantPool.PAGE_TOTAL_FANS_COUNT, 1);
					redisUtil.addCount(key, ConstantPool.PAGE_CURRENT_FANS_COUNT, 1);					 

				}

				/* Delete file. */
				FileUtils.deleteQuietly(file);

			} else {
				TaskResult taskResult = new TaskResult(request, "execute python error ", TupleDefinition.Result.FAIL.getCode());
				emit(input, taskResult);
			}

		} else {
			TaskResult taskResult = new TaskResult(request, "pageId is null ", TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
		}

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in crawling fans", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

	private int executePythonCommand(String pyFilePath, String url, String filePath, String age, String location, String gender) throws IOException,
			InterruptedException {
		List<String> cmd = new ArrayList<String>();
		cmd.add("python");
		cmd.add(pyFilePath);
		cmd.add("-url");
		cmd.add(url);
		cmd.add("-file");
		cmd.add(filePath);
		/*cmd.add("-age");
		cmd.add(age);
		cmd.add("-livein");
		cmd.add(location);
		cmd.add("-gender");
		cmd.add(gender);*/
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);

		Process process = pb.start();

		int w = process.waitFor();
		log.info("existValue:" + w);

		return w;
	}

}
