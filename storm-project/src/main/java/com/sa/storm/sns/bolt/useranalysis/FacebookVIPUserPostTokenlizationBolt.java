package com.sa.storm.sns.bolt.useranalysis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
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
import cn.com.cjf.CJFBeanFactory;
import cn.com.cjf.ChineseJF;

import com.sa.crawler.definition.FacebookToken;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;
import comp.sa.subtopic.nlp.IRLas;

public class FacebookVIPUserPostTokenlizationBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(FacebookVIPUserPostTokenlizationBolt.class);
	private ArrayList<String> posts = new ArrayList<String>();
	private ArrayList<String> checkins = new ArrayList<String>();
	private static String HDFSUrl = "hdfs://hadoop-master1:9000/fb/";
	private static String HDFSCheckInUrl = "hdfs://hadoop-master1:9000/ci/";
	private ChineseJF tradSimpConvertor;
	private List<String> stopWords;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		tradSimpConvertor = CJFBeanFactory.getChineseJF();
		stopWords = loadStopWord();
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
			try {
				String jobId = doc.getFileName();
				String isLast = doc.getLastFlag();
				String userId = doc.getUserId();
				String FBUserId = doc.getUserName();
				String hId = doc.getHistoryId();
				String filePath = "/home/sa/fb/user/" + jobId;
				String checkinsFilePath = "/home/sa/fb/checkins/" + jobId;
				long totalUserCount = doc.getViewCount();

				// 1.parser friends information and persistence

				// ArrayList<String> friendsList = requestFrindsList(userId);

				// 2.get the post of user and save in local file
				if (null != FBUserId) {

					wirteFile(FBUserId, filePath);
					writeCheckins(FBUserId, checkinsFilePath);

				}
				// 3. if the last one, upload the file to HDFS
				if (isLast.equals("1")) {
					File uploadFile = new File(filePath);
					if (uploadFile.exists()) {
						FacebookHDFSUtil.uploadLocalFile2HDFS(filePath, HDFSUrl + jobId);
						FacebookHDFSUtil.uploadLocalFile2HDFS(checkinsFilePath, HDFSCheckInUrl + jobId);
					}
					// emit to WordCount Bolt to process
					DocumentResult documentResult = new DocumentResult(docResult);
					Document nextTaskDoc = new Document();
					nextTaskDoc.setFileName(jobId);
					nextTaskDoc.setHistoryId(hId);
					nextTaskDoc.setUserName(FBUserId);
					nextTaskDoc.setUserId(userId);
					nextTaskDoc.setViewCount(totalUserCount);
					documentResult.setDocument(nextTaskDoc);
					emit(input, documentResult, "emit to TopCategory bolt");
				}
			} catch (Exception e) {
				log.error(e.getMessage(), e.toString());
				TaskResult taskResult = new TaskResult(docResult, "exception when crawling Facebook UserId",
						TupleDefinition.Result.FAIL.getCode());
				emit(input, taskResult);
			}

		}
		TaskResult taskResult = new TaskResult(docResult, "process successfully", TupleDefinition.Result.SUCCESS.getCode());
		emit(input, taskResult);
	}

	private List<String> loadStopWord() {
		ArrayList<String> stopWords = new ArrayList<String>();
		stopWords.add("the");
		stopWords.add("of");
		stopWords.add("a");
		stopWords.add("and");
		stopWords.add("to");
		stopWords.add("is");
		stopWords.add("are");
		stopWords.add("were");
		stopWords.add("in");
		stopWords.add("we");
		stopWords.add("i");
		stopWords.add("you");
		stopWords.add("he");
		stopWords.add("his");
		stopWords.add("she");
		stopWords.add("her");
		stopWords.add("him");
		stopWords.add("d");
		stopWords.add("my");
		stopWords.add("me");
		stopWords.add("for");
		stopWords.add("that");
		stopWords.add("one");
		// File file = new File("stopwords.txt");
		// List<String> stopWords = null;
		// try {
		// stopWords = FileUtils.readLines(file, "UTF-8");
		// } catch (IOException e) {
		// log.error("error {} ", e.toString());
		// e.printStackTrace();
		// }

		return stopWords;

	}

	private ArrayList<String> tokenlization(String sentense) throws NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException {
		// log.info("java.library.path {}", System.getProperty("java.library.path"));
		ArrayList<String> list = IRLas.getInstance().doSegment(sentense);
		return list;
	}

	private static ArrayList<String> EngTokenlization(String str) {
		ArrayList<String> wordList = new ArrayList<String>();
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < str.length(); i++) {
			char a = str.charAt(i);
			int c = (int) a;
			if ((c >= 65 && c <= 90) || (c >= 97 && c <= 122) || c == 32) {
				result.append(a);
			}
		}
		String wordArray[] = result.toString().split(" ");
		for (String word : wordArray) {
			try {
				if ("" != word.replace(" ", "")) {
					wordList.add(word);
				}
			} catch (Exception ex) {
				continue;
			}
		}
		return wordList;
	}

	private void wirteFile(String id, String filePath) throws IOException, JSONException, NoSuchFieldException, SecurityException,
			IllegalArgumentException, IllegalAccessException {
		posts = new ArrayList<String>();
		checkins = new ArrayList<String>();
		String json = requestFriendPosts(id);
		try {
			parsePosts(json);
		} catch (Exception ex) {
			log.error("Parser Error {}", ex.toString());
		}
		FileWriter fw = new FileWriter(filePath, true);
		PrintWriter out = new PrintWriter(fw);
		if (!System.getProperty("java.library.path").contains("/home/sa/NLPIR/ictclas_nlpir")) {

			System.setProperty("java.library.path", "/home/sa/NLPIR/ictclas_nlpir:" + System.getProperty("java.library.path"));
			Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
			fieldSysPath.setAccessible(true);
			fieldSysPath.set(null, null);
		}

		for (String post : posts) {
			ArrayList<String> tokenWords = tokenlization(post);
			if (null != tokenWords) {
				for (String word : tokenWords) {
					word = tradSimpConvertor.chineseJan2Fan(word);
					out.println(word);
				}
			}

			ArrayList<String> engTokenWords = EngTokenlization(post);
			if (null != engTokenWords) {
				for (String word : engTokenWords) {
					if (!stopWords.contains(word.toLowerCase()) && "" != word.replace(" ", ""))
						out.println(word.toLowerCase());
				}
			}
			// try {
			// segmentCommand(filePath, post);
			// } catch (InterruptedException e) {
			// log.error("segment error {}", e.toString());
			// }
		}
		out.close();
		fw.close();
	}

	private void writeCheckins(String id, String filePath) throws IOException, JSONException, NoSuchFieldException, SecurityException,
			IllegalArgumentException, IllegalAccessException {
		FileWriter fw = new FileWriter(filePath, true);
		PrintWriter out = new PrintWriter(fw);

		for (String checkin : checkins) {
			out.println(checkin);
		}
		out.close();
		fw.close();
	}

	private static String requestFriendPosts(String userId) throws JSONException {
		String url = "https://graph.facebook.com/" + userId + "/posts/?access_token=" + FacebookToken.MobileApiToken.USA_MOBILE_TOKEN;
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
		return json;
	}

	private void parsePosts(String json) throws JSONException {
		JSONObject jsonObj = new JSONObject(json);
		JSONObject pagingObj = jsonObj.optJSONObject("paging");
		JSONArray postObjs = jsonObj.optJSONArray("data");
		for (int i = 0; i < postObjs.length(); i++) {
			JSONObject postObj = postObjs.getJSONObject(i);
			String description = postObj.optString("description");
			if (!postObj.has("description") && postObj.has("message")) {
				description = postObj.optString("message");
			}
			if (null != description && "" != description.replace(" ", "")) {
				posts.add(description);
			}
			if (null != postObj.optJSONObject("place")) {
				try {
					// String checkinsId = postObj.optJSONObject("place").optString("id");
					// String checkinsJson = FacebookApiRequestUtil.getInstance().submitRequest("http://graph.facebook.com/" + checkinsId);
					// JSONObject checkinsJsonObj = new JSONObject(checkinsJson);
					// String catetoryName = checkinsJsonObj.optString("name");
					// if (null != catetoryName) {
					// System.out.println("checkin " + catetoryName);
					// checkins.add(catetoryName);
					// }

					String checkinName = postObj.optJSONObject("place").optString("name");
					checkins.add(checkinName);
				} catch (Exception ex) {
					continue;
				}

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
