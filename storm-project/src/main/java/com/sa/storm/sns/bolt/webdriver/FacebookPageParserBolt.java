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

import util.CHCrawlerConfiguration;

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

public class FacebookPageParserBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(FacebookPageParserBolt.class);
	private static String HDFSUrl;

	private String detailPageStream = DocumentResult.class.getName() + "-detail-page";
	private String basicPageStream = DocumentResult.class.getName() + "-basic-page";

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			HDFSUrl = CHCrawlerConfiguration.getInstance().getFinderFansHDFSPath();
		} catch (Exception e) {
			log.error("Error in EsoonFilterBolt prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, detailPageStream, DocumentResult.class, null);
		declareOutputByClass(declarer, basicPageStream, DocumentResult.class, null);
	}

	@Override
	public void process(Tuple input) throws Exception {

		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		final String docJson = request.getParamByKey(TupleDefinition.Param.DOCUMENTS);
		if (docJson != null) {
			Document doc = JsonUtil.getMapper().readValue(docJson, Document.class);

			String url = doc.getUrl();
			log.info("url:" + url);
			String pageId = doc.getTitle();
			String pageName = doc.getUserName();
			String gender = doc.getGender();
			String hId = doc.getHistoryId();
			String lastFlag = doc.getLastFlag();
			String uId = doc.getUserId();
			log.info("gender:" + gender);

			String fileDirectory = CHCrawlerConfiguration.getInstance().getFinderParserPath();
			log.info("fileDirectory: {}", fileDirectory);

			String fileId = UUID.randomUUID().toString();
			String tmpFileName = "fans-" + fileId;
			String tmpFilePath = fileDirectory + tmpFileName;
			String resultFileName = "result-" + fileId;
			String reusltFilePath = fileDirectory + resultFileName;

			int exitValue = executePythonCommand(url, fileDirectory, tmpFilePath, lastFlag);
			File RFile = new File(reusltFilePath);
			FileWriter fw = new FileWriter(reusltFilePath, true);
			PrintWriter out = new PrintWriter(fw);
			if (exitValue == 0) {

				File file = new File(tmpFilePath);
				List<String> list = FileUtils.readLines(file, "UTF-8");
				log.info("fileSize:" + list.size() + "  " + url);
				for (String s : list) {
					try {
						if (s.replace(" ", "").equals("last")) {
							log.info("DetailParser last");
							DocumentResult detailDocResult = new DocumentResult(request);
							Document detailPage = new Document();
							detailPage.setLastFlag("l");
							detailPage.setHistoryId(hId);
							detailPage.setTitle(pageId);
							detailPage.setUserId(uId);
							detailDocResult.setDocument(detailPage);
							emit(detailPageStream, input, detailDocResult, null, "resultFile");
						} else {

							String info[] = s.split(",");
							if (info.length < 3) {
								log.info("infomation is not complete");
								continue;
							}
							String userId = info[0];
							String userScreenName = info[1];
							String avatarUrl = info[2];
							// String digitId = info[3];
							if (null == userId || "" == userId) {
								continue;
							}
							UserInfoForExtUsage user = new UserInfoForExtUsage(userId);
							user.setUserId(userId);
							user.setAvatarUrl(avatarUrl);
							user.setGender(gender);
							user.setUserScreenName(userScreenName);

							// emit detail page request
							DocumentResult detailDocResult = new DocumentResult(request);
							Document detailPage = new Document();
							detailPage.setUserScreenName(userId);
							detailPage.setLastFlag("f");
							// detailPage.setId(digitId);
							detailDocResult.setDocument(detailPage);
							emit(detailPageStream, input, detailDocResult, null, "resultFile");
							out.println(JsonUtil.getMapper().writeValueAsString(user));
						}
					} catch (Exception e) {
						log.info("exception: {}", e.toString());
						continue;
					}
				}

				log.info("HDFSUrl: " + HDFSUrl);
				uploadLocalFile2HDFS(reusltFilePath, HDFSUrl);
				file.delete();
			} else {

			}
			RFile.delete();
			out.close();
			fw.close();

			// write to hdfs

			// emit fileName to next bolt
			// emiTaskRequest(input, tmpFileName);

			DocumentResult docResult = new DocumentResult(request);
			Document resultFile = new Document();
			resultFile.setFileName(resultFileName);
			resultFile.setTitle(pageId);
			resultFile.setUserName(pageName);
			resultFile.setHistoryId(hId);
			resultFile.setLastFlag(lastFlag);
			docResult.setDocument(resultFile);

			emit(basicPageStream, input, docResult, null, "resultFile");
		} else {
			TaskResult taskResult = new TaskResult(request, "No document in the request, ignored", TupleDefinition.Result.SUCCESS.getCode());
			emit(input, taskResult);

		}

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		// TODO Auto-generated method stub
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in fliter doc", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

	private void emiTaskRequest(Tuple input, String fileName) throws Exception {
		Document doc = new Document();
		doc.setUrl(fileName);
		TaskRequest request = new TaskRequest(UUID.randomUUID().toString(), "1", TaskType.KEYWORD.getCode(),
				SourceType.FACEBOOK.getSourceTypeStr(), new Integer(Priority.NORMAL.getCode()), new HashMap<String, String>());
		request.addParam(TupleDefinition.Param.DOCUMENTS, JsonUtil.getMapper().writeValueAsString(doc));
		emit(input, request, "Submit task request to Redis");

	}

	public void uploadLocalFile2HDFS(String s, String d) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path src = new Path(s);
		Path dst = new Path(d);

		hdfs.copyFromLocalFile(src, dst);

		hdfs.close();
	}

	private int executePythonCommand(String url, String fileDirectory, String filePath, String lastFlag) throws IOException,
			InterruptedException {
		List<String> cmd = new ArrayList<String>();
		cmd.add("python");
		cmd.add(fileDirectory + "fb-fans.py");
		cmd.add("-queryurl");
		cmd.add(url);
		cmd.add("-file");
		cmd.add(filePath);
		cmd.add("-flag");
		cmd.add(lastFlag);
		log.info("cmd :" + cmd.toString());
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);

		Process process = null;

		process = pb.start();
		log.info("Python Command " + cmd.toString());
		int w = process.waitFor();
		log.info("existValue:" + w);
		return w;
	}

}
