package com.sa.storm.sns.bolt.relation;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.CHCrawlerConfiguration;
import util.FacebookHDFSUtil;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;


import com.sa.common.json.JsonUtil;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

public class FacebookRelationParserBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(FacebookRelationParserBolt.class);
	private static String HDFSUrl;

	// private static String HDFSUrl = "hdfs://pre-prod-hadoop:9000/user/ubuntu/facebook/";
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			HDFSUrl = CHCrawlerConfiguration.getInstance().getRelationHDFSPath();

		} catch (Exception e) {
			log.error("Error in EsoonFilterBolt prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, DocumentResult.class);
	}

	@Override
	public void process(Tuple input) throws Exception {

		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		final String docJson = request.getParamByKey(TupleDefinition.Param.DOCUMENTS);
		if (docJson != null) {
			Document doc = JsonUtil.getMapper().readValue(docJson, Document.class);

			String url = doc.getUrl();
			String fileName = doc.getFileName();
			String isLast = doc.getLastFlag();

			log.info("url:" + url);
			String gender = doc.getGender();
			String location = doc.getLocation();
			String hid = doc.getHistoryId();
			String uId = doc.getUserId();
			String pageNames = doc.getUserName(); 
			String fileDirectory = CHCrawlerConfiguration.getInstance().getRelationParserPath();
			String reusltFilePath = fileDirectory + fileName;
			log.info("filePath {}", reusltFilePath);

			int exitValue = executePythonCommand(url, reusltFilePath, location, gender);
			File file = new File(reusltFilePath);
			if (exitValue == 0) {
				log.info("lasgFlag:{}", isLast);
				if (file.exists()) {
					FacebookHDFSUtil.moveLocalFile2HDFS(reusltFilePath, HDFSUrl + fileName);
					log.info("uploadToHDFS: {}", fileName);
				}
				if (isLast.equals("l")) {
					log.info("latsFlag");

					DocumentResult documentResult = new DocumentResult(request);
					Document persistDoc = new Document();
					persistDoc.setHistoryId(hid);
					persistDoc.setUserId(uId);
					persistDoc.setUserName(pageNames);
					persistDoc.setFileName(HDFSUrl + fileName);
					documentResult.setDocument(persistDoc);
					emit(input, documentResult, "emit to persist");
					// TaskResult taskResult = new TaskResult(request, "upload file to HDFS", TupleDefinition.Result.SUCCESS.getCode());
					// emit(input, taskResult);
				} else {
					TaskResult taskResult = new TaskResult(request, "upload file to HDFS", TupleDefinition.Result.SUCCESS.getCode());
					emit(input, taskResult);
				}

				file.delete();

			}

		} else {
			TaskResult taskResult = new TaskResult(request, "doc is null ", TupleDefinition.Result.FAIL.getCode());
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

	private void appendFile(String localFile, String hdfsPath) throws IOException {
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		InputStream in = new FileInputStream(localFile);
		OutputStream out = fs.append(new Path(hdfsPath));
		IOUtils.copyBytes(in, out, config);
	}

	private void uploadLocalFile2HDFS(String s, String d) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path src = new Path(s);
		Path dst = new Path(d);

		hdfs.copyFromLocalFile(src, dst);

		hdfs.close();
	}

	private int executePythonCommand(String url, String filePath, String location, String gender) throws IOException, InterruptedException {
		List<String> cmd = new ArrayList<String>();
		cmd.add("python");
		cmd.add("/home/sa/fb/relation/fb-relation.py");
		cmd.add("-queryurl");
		cmd.add(url);
		cmd.add("-file");
		cmd.add(filePath);
		cmd.add("-livein");
		cmd.add(location);
		cmd.add("-gender");
		cmd.add(gender);
		log.info("cmd {}", cmd.toString());
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);

		Process process = null;

		process = pb.start();

		int w = process.waitFor();
		log.info("existValue:" + w);
		return w;
	}

}
