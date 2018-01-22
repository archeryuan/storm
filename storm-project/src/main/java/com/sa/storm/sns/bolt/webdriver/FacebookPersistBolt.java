package com.sa.storm.sns.bolt.webdriver;

import java.net.URLEncoder;
import java.util.Map;

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
import com.sa.competitorhunter.dao.FinderHistoryRepository;
import com.sa.competitorhunter.domain.FinderHistory;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

public class FacebookPersistBolt extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(FacebookPersistBolt.class);

	private HttpClient httpClient;

	private static String restUrl;

	private static String upLoadRestUrl;

	private FinderHistoryRepository finderHistoryRepo;

	// private static final String restUrl = "http://pre-prod-tomcat:8080/ch-persistent/pub/data/titanPersist";

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			restUrl = CHCrawlerConfiguration.getInstance().getGraphDBPersistenceUrl();
			upLoadRestUrl = CHCrawlerConfiguration.getInstance().getGraphDBUploadUrl();
			finderHistoryRepo = getSpringContext().getBean(FinderHistoryRepository.class);
		} catch (Exception e) {
			log.error("Error in FacebookPersistBolt prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, TaskRequest.class);
	}

	@Override
	public void process(Tuple input) throws Exception {

		DocumentResult docResult = (DocumentResult) getInputByClass(input, DocumentResult.class);
		Document doc = docResult.getDocument();
		String resultFileName = doc.getFileName();
		String flag = doc.getLastFlag();
		String hId = doc.getHistoryId();
		log.info("fileName: " + resultFileName);
		String pageId = doc.getTitle();
		String pageName = doc.getUserName();
		log.info("requestUrl: " + restUrl + "?pageid=" + pageId + "&file=" + resultFileName);
		Utils.sleep(10000);
		String requestResult = submitRequest(restUrl + "?pageid=" + pageId + "&file=" + resultFileName);
		log.info("requestResult: " + requestResult);

		if (flag.equals("l")) {
			pageName = URLEncoder.encode(pageName, "UTF-8");
			String json = submitRequest(upLoadRestUrl + "?pageid=" + pageId + "&pagename=" + pageName);
			JSONObject jsonObj = new JSONObject(json);
			String fileLocation = jsonObj.optString("result");
			log.info("fileLocation " + fileLocation + " pageName " + pageName);

			FinderHistory finderHistory = finderHistoryRepo.findById(Long.parseLong(hId));
			finderHistory.setFileLocation(fileLocation);
			finderHistory.setStatus(1);
			finderHistoryRepo.saveAndFlush(finderHistory);

		}
		TaskResult taskResult = new TaskResult(docResult, "Forward news document result for persistence",
				TupleDefinition.Result.SUCCESS.getCode());
		emit(input, taskResult);

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
}
