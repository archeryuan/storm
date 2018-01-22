package com.sa.storm.sns.bolt.webdriver;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.persistence.UserInfoForExtUsage;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

public class CHFinderTaskAssignBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(CHFinderTaskAssignBolt.class);
	private static String HDFSUrl = "hdfs://hadoop-master1:9000/user/sa/facebook/";
	// private static String HDFSUrl = "hdfs://pre-prod-hadoop:9000/user/ubuntu/facebook/";
	private String findFansStream = TaskRequest.class.getName() + "-find-fans";
	private String findHashtagStream = TaskRequest.class.getName() + "-find-hashtag";

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {

		} catch (Exception e) {
			log.error("Error in EsoonFilterBolt prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, findFansStream, TaskRequest.class, null);
		declareOutputByClass(declarer, findHashtagStream, TaskRequest.class, null);
	}

	@Override
	public void process(Tuple input) throws Exception {

		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		final String isHashTagTask = request.getParamByKey(TupleDefinition.Param.HASHTAG_ID);
		if (isHashTagTask.equals("y")) {
			emit(findHashtagStream, input, request, null, "request for find hashtag task");
		}
		if (isHashTagTask.equals("n")) {
			emit(findFansStream, input, request, null, "request for find fans");
		}else{
			
		}

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



}
