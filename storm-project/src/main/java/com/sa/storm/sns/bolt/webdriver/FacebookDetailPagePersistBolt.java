package com.sa.storm.sns.bolt.webdriver;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.BasicResponseHandler;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.CHCrawlerConfiguration;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import com.sa.common.util.HttpClientUtil;
import com.sa.competitorhunter.dao.EstimationRepository;
import com.sa.competitorhunter.dao.FinderHistoryRepository;
import com.sa.competitorhunter.domain.Estimation;
import com.sa.competitorhunter.domain.FinderHistory;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskResult;

public class FacebookDetailPagePersistBolt extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(FacebookDetailPagePersistBolt.class);

	private HttpClient httpClient;

	private static String restUrl;

	private static String upLoadRestUrl;

	private FinderHistoryRepository finderHistoryRepo;

	private EstimationRepository estimationRepo;

	// private static final String restUrl = "http://pre-prod-tomcat:8080/ch-persistent/pub/data/persistSingleUser";

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			restUrl = CHCrawlerConfiguration.getInstance().getGraphDBDetailPersistenceUrl();
			upLoadRestUrl = CHCrawlerConfiguration.getInstance().getGraphDBUploadUrl();
			finderHistoryRepo = getSpringContext().getBean(FinderHistoryRepository.class);
			estimationRepo = getSpringContext().getBean(EstimationRepository.class);
		} catch (Exception e) {
			log.error("Error in FacebookPersistBolt prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, DocumentResult.class);
	}

	@Override
	public void process(Tuple input) throws Exception {
		DocumentResult docResult = (DocumentResult) getInputByClass(input, DocumentResult.class);
		Document doc = docResult.getDocument();
		String lastFlag = doc.getLastFlag();
		log.info("lastFlag " + lastFlag);
		String FBUId = doc.getUserScreenName();
		String hId = doc.getHistoryId();
		String pageId = doc.getTitle();
		String uId = doc.getUserId();
		String facebookUserId = doc.getUserScreenName();
		String pageName = doc.getUserName();
		if (lastFlag.equals("l")) {
			// generate a fans list of Fanpage, upload to AWS, updated Mysql History(FinderHistory, Estimation)
			pageName = URLEncoder.encode(pageName, "UTF-8");
			String json = submitRequest(upLoadRestUrl + "?pageid=" + pageId + "&pagename=" + pageName);
			JSONObject jsonObj = new JSONObject(json);
			String fileLocation = jsonObj.optString("result");
			log.info("fileLocation " + fileLocation);

			FinderHistory finderHistory = finderHistoryRepo.findById(Long.parseLong(hId));
			finderHistory.setFileLocation(fileLocation);
			finderHistory.setStatus(1);
			finderHistoryRepo.saveAndFlush(finderHistory);

			List<Estimation> estmation = estimationRepo.getByTypeAndUserId((short) 3, Long.parseLong(uId), 2);

			Estimation est = estmation.get(0);
			est.setActualCompletedTime(new Date());
			estimationRepo.saveAndFlush(est);

		} else {
			String fileDirectory = CHCrawlerConfiguration.getInstance().getFinderParserPath();
			String fileId = UUID.randomUUID().toString();
			String tmpFileName = "detail-" + fileId;
			String tmpFilePath = fileDirectory + tmpFileName;
			try {
				int exitValue = executePythonCommand(facebookUserId, fileDirectory, tmpFilePath);

				if (exitValue == 0) {
					File file = new File(tmpFilePath);
					List<String> list = FileUtils.readLines(file, "UTF-8");
					if (null != list && list.size() != 0) {
						String detailInfo = list.get(0);
						String friendNum = detailInfo.split("\\|")[0];
						String liveIn = detailInfo.split("\\|")[1];
						if (friendNum != null && liveIn != null) {
							// Utils.sleep(3000);
							String requestResult = submitRequest(restUrl + "?uid=" + FBUId + "&livein="
									+ URLEncoder.encode(liveIn, "UTF-8") + "&friendnum=" + URLEncoder.encode(friendNum, "UTF-8"));
							log.info("singleUserRequest:{}", restUrl + "?uid=" + FBUId + "&livein=" + liveIn + "&friendnum=" + friendNum);
							log.info("singleUserRequestResult:{}", requestResult);

						}
					}
					file.delete();
				}
				TaskResult taskResult = new TaskResult(docResult, "perisit single user info", TupleDefinition.Result.SUCCESS.getCode());
				emit(input, taskResult);
				// emit(input, docResult, null, "Forward news document result for persistence");
			} catch (Exception ex) {
				TaskResult taskResult = new TaskResult(docResult, "perisit single user info with exception", ex,
						TupleDefinition.Result.FAIL.getCode());
				emit(input, taskResult);
				log.info("ex: {}", ex.toString());
			}
		}

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		DocumentResult request = (DocumentResult) getInputByClass(input, DocumentResult.class);

		TaskResult error = new TaskResult(request, "Error in post analysis processor", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

	public String submitRequest(String url) {
		httpClient = HttpClientUtil.getInstance().getClient();
		String result = new String();
		HttpRequestBase httpRequest = null;

		try {
			httpRequest = new HttpGet(url);
			final ResponseHandler<String> handler = new BasicResponseHandler();
			result = httpClient.execute(httpRequest, handler);
		} catch (Exception e) {
			if (httpRequest != null) {
				httpRequest.abort();
			}
		} finally {
			if (httpRequest != null) {
				httpRequest.releaseConnection();
			}
		}

		return result;
	}

	private int executePythonCommand(String uId, String fileDirectory, String file) throws IOException, InterruptedException {
		List<String> cmd = new ArrayList<String>();
		cmd.add("python");
		cmd.add(fileDirectory + "fb-detail.py");
		cmd.add("-uid");
		cmd.add(uId);
		cmd.add("-file");
		cmd.add(file);

		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);

		Process process = null;

		process = pb.start();

		int w = process.waitFor();
		log.info("existValue:" + w);
		return w;
	}

}
