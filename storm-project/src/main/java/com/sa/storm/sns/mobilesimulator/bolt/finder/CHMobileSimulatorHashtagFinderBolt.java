package com.sa.storm.sns.mobilesimulator.bolt.finder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.ch.aws.s3.FileUploader;
import com.sa.common.json.JsonUtil;
import com.sa.competitorhunter.dao.EstimationRepository;
import com.sa.competitorhunter.dao.FinderHistoryRepository;
import com.sa.competitorhunter.domain.Estimation;
import com.sa.competitorhunter.domain.FinderHistory;
import com.sa.crawler.definition.FacebookToken;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;

public class CHMobileSimulatorHashtagFinderBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(CHMobileSimulatorHashtagFinderBolt.class);
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	private FinderHistoryRepository finderRepo;
	private EstimationRepository estimationRepo;
	private static String Token = FacebookToken.MobileApiToken.HK_MOBILE_TOKEN;
	private final static byte commonCsvHead[] = { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };

	// private static String HDFSUrl = "hdfs://pre-prod-hadoop:9000/user/ubuntu/facebook/";

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			finderRepo = getSpringContext().getBean(FinderHistoryRepository.class);
			estimationRepo = getSpringContext().getBean(EstimationRepository.class);
		} catch (Exception e) {
			log.error("Error in EsoonFilterBolt prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void process(Tuple input) throws Exception {

		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		final String docJson = request.getParamByKey(TupleDefinition.Param.DOCUMENTS);
		if (docJson != null) {
			Document doc = JsonUtil.getMapper().readValue(docJson, Document.class);

			String queryUrl = doc.getUrl();
			String hashtag = doc.getKeywords();
			String hId = doc.getHistoryId();
			String uId = doc.getUserId();
			System.out.println("queryUrl: " + queryUrl + " uId " + uId + " hId " + hId);
			long type = doc.getTypeId();

			String fileName = "";
			String path = "";
			String dateStr = DATE_FORMAT.format(new Date());
			Set<String> HashTags = null;
			short taskType = 0;
			if (type == 1L) {
				taskType = 4;
				fileName = dateStr + "-Hashtag-relate-to-Hashtag-" + hashtag + ".csv";
				path = "/home/sa/fb/" + fileName;
				HashTags = parseHashtag(hashtag, path);
			} else {
				taskType = 3;
				fileName = dateStr + "-Fans-withPostMentionedHashtag-" + hashtag + ".csv";
				path = "/home/sa/fb/" + fileName;
			}

			if (null != HashTags) {
				// upload to aws file system
				String fileLocation = FileUploader.uploadFile(new File(path), true, true);
				// update mysql record
				// 1.history
				FinderHistory f = finderRepo.findById(Long.parseLong(hId));
				f.setFileLocation(fileLocation);
				f.setFileName(fileName);
				f.setUpdateDate(new Date());
				if (f.getStatus() == 0) {
					f.setStatus(1);
				} else {
					f.setStatus(2);
				}
				finderRepo.saveAndFlush(f);

				// 2.estimation
				List<Estimation> estmation = estimationRepo.getByTypeAndUserId(taskType, Long.parseLong(uId), 2);

				Estimation est = estmation.get(0);
				est.setActualCompletedTime(new Date());
				estimationRepo.saveAndFlush(est);
			}
			TaskResult taskResult = new TaskResult(request, "parse successfully", TupleDefinition.Result.SUCCESS.getCode());
			emit(input, taskResult);
		} else {
			TaskResult taskResult = new TaskResult(request, "No document in the request, ignored", TupleDefinition.Result.SUCCESS.getCode());
			emit(input, taskResult);

		}

	}

	private Set<String> parseHashtag(String hashTag, String filePath) throws IOException {
		Set<String> HashtagSet = new TreeSet<String>();
		String nextPageUrl = "";
		String url = buildUrl(hashTag, Token);
		int pageNum = 0;
		FileWriter fw = new FileWriter(filePath, true);
		PrintWriter out = new PrintWriter(fw);
		String bs = new String(commonCsvHead);
		out.write(bs);
		while (null != nextPageUrl) {
			try {
				String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url,
						FacebookToken.MobileApiToken.USA_MOBILE_TOKEN);
				// System.out.println(json);
				JSONObject jsonObj = new JSONObject(json);
				JSONArray postObjs = jsonObj.optJSONArray("data");
				for (int i = 0; i < postObjs.length(); i++) {
					JSONObject postObj = postObjs.getJSONObject(i);
					if (null != postObj.optString("description") && "" != postObj.optString("description")) {
						String description = postObj.optString("description");
						HashtagSet.addAll(findHashtag(description));
					} else if (null != postObj.optString("caption") && "" != postObj.optString("caption")) {
						String caption = postObj.optString("caption");
						HashtagSet.addAll(findHashtag(caption));
					}
				}
				JSONObject pagingObj = jsonObj.optJSONObject("paging");
				nextPageUrl = (pagingObj == null) ? null : pagingObj.optString("next");
				pageNum++;
				if (pageNum > 60) {
					out.close();
					fw.close();
					break;
				}
			} catch (Exception ex) {
				break;
			}
		}
		for (String h : HashtagSet) {
			out.println(h);
		}
		out.close();
		fw.close();
		return HashtagSet;
	}

	private static Set<String> findHashtag(String description) {
		Set<String> hashtagSet = new TreeSet<String>();
		String strs[] = description.replaceAll("(\r\n|\r|\n|\n\r)", " ").split(" ");
		for (String str : strs) {
			if (str.replace(" ", "").contains("#")) {
				hashtagSet.add(str);
			}
		}
		return hashtagSet;

	}

	private static String buildUrl(String hashtag, String token) throws UnsupportedEncodingException {

		return "https://graph.facebook.com/search?q=%23" + URLEncoder.encode(hashtag, "UTF-8") + "&access_token=" + token;
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
