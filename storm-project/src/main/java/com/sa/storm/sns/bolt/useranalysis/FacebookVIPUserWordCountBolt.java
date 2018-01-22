package com.sa.storm.sns.bolt.useranalysis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.FacebookHDFSUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.crawler.definition.FacebookToken;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.service.RedisPubSubAckQueueService;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;
import comp.sa.subtopic.nlp.IRLas;

public class FacebookVIPUserWordCountBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(FacebookVIPUserWordCountBolt.class);
	private static ArrayList<String> posts = new ArrayList<String>();
	private static String HDFSUrl = "hdfs://hadoop-master1:9000/fb/";
	private static RedisPubSubAckQueueService queueService;
	private static RedisDefinition.MessageQueue queue;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, DocumentResult.class);
	}

	@Override
	public void process(Tuple input) throws Exception {

		DocumentResult docResult = (DocumentResult) getInputByClass(input, DocumentResult.class);
		Document doc = docResult.getDocument();
		if (null != doc) {

			String jobId = doc.getFileName();
			String isLast = doc.getLastFlag();
			String userId = doc.getUserId();
			String filePath = "/home/sa/fb/user/" + jobId;
			log.info("jobId {}", jobId);

			if (isLast.equals("1")) {
				File uploadFile = new File(filePath);
				if (uploadFile.exists()) {
					FacebookHDFSUtil.uploadLocalFile2HDFS(filePath, HDFSUrl + jobId);

					// emit to WordCount Bolt to process
					DocumentResult documentResult = new DocumentResult(docResult);
					Document nextTaskDoc = new Document();
					nextTaskDoc.setFileName(jobId);
					documentResult.setDocument(nextTaskDoc);
					emit(input, documentResult, "emit to WordCount bolt");
				}
			}

		}
		TaskResult taskResult = new TaskResult(docResult, "process successfully", TupleDefinition.Result.SUCCESS.getCode());
		emit(input, taskResult);
	}

	private ArrayList<String> tokenlization(String sentense) throws NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException {
		System.setProperty("java.library.path", "/home/sa/NLPIR/ictclas_nlpir:" + System.getProperty("java.library.path"));

		Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
		fieldSysPath.setAccessible(true);
		fieldSysPath.set(null, null);
		System.out.println(System.getProperty("java.library.path"));
		ArrayList<String> list = IRLas.getInstance().doSegment(sentense);
		return list;
	}

	private void wirteFile(String id, String filePath) throws IOException, JSONException, NoSuchFieldException, SecurityException,
			IllegalArgumentException, IllegalAccessException {
		posts = new ArrayList<String>();
		String json = requestFrindPosts(id);
		parsePosts(json);
		FileWriter fw = new FileWriter(filePath, true);
		PrintWriter out = new PrintWriter(fw);

		for (String post : posts) {
			ArrayList<String> tokenWords = tokenlization(post);
			for (String word : tokenWords) {
				out.println(word);
			}
		}
		out.close();
		fw.close();
	}

	private static String requestFrindPosts(String userId) throws JSONException {
		String url = "https://graph.facebook.com/" + userId + "/posts/?access_token=" + FacebookToken.MobileApiToken.USA_MOBILE_TOKEN;
		System.out.println(url);
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
		return json;
	}

	private void parsePosts(String json) throws JSONException {
		JSONObject jsonObj = new JSONObject(json);
		JSONObject pagingObj = jsonObj.optJSONObject("paging");
		JSONArray postObjs = jsonObj.optJSONArray("data");
		for (int i = 0; i < postObjs.length(); i++) {
			System.out.println(i);
			JSONObject postObj = postObjs.getJSONObject(i);
			String description = postObj.optString("description");
			if (!postObj.has("description") && postObj.has("message")) {
				description = postObj.optString("message");
			}
			if (null != description && "" != description.replace(" ", "")) {
				posts.add(description);
			}
		}
		String lrt = (pagingObj == null) ? null : pagingObj.optString("next");
		if (null != lrt) {
			String nextPageJson = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(lrt,
					FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
			if (null != nextPageJson && null != jsonObj.optJSONArray("data")) {
				parsePosts(nextPageJson);
			}
		}

	}

	private ArrayList<String> requestFrindsList(String userId) {
		ArrayList<String> friendLists = new ArrayList<String>();
		String url = "https://graph.facebook.com/" + userId + "/friends?access_token=" + FacebookToken.MobileApiToken.USA_MOBILE_TOKEN;
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
		return friendLists;
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		// TODO Auto-generated method stub
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in fliter doc", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

}
