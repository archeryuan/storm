package com.sa.storm.sns.bolt.document;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import cn.com.cjf.CJFBeanFactory;
import cn.com.cjf.ChineseJF;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.definition.SourceType;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

public class ExtractKeywordBolt extends BaseBolt {

	private static final long serialVersionUID = 4586670132080308429L;

	private static final Logger log = LoggerFactory.getLogger(ExtractKeywordBolt.class);

	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

	private static final String HDFS_URL = "hdfs://hadoop-master1:9000/keyword/";

	private static final String LOCAL_FILE_DIR = "/home/sa/ch/keyword/";

	private static final int KEY_WORD_COUNT = 5;

	private ChineseJF tradSimpConvertor;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		tradSimpConvertor = CJFBeanFactory.getChineseJF();
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, DocumentResult.class);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void process(Tuple input) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		final Map<String, String> params = request.getParams();

		if (null == params || params.isEmpty()) {
			TaskResult taskResult = new TaskResult(request, "No params ", TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
			return;
		}

		Map<String, String> newParams = new HashMap<String, String>();
		newParams.putAll(params);

		String title = params.get(SolrFieldDefinition.TITLE.getName());
		String content = params.get(SolrFieldDefinition.CONTENT.getName());

		// if (!StringUtils.isEmpty(title) || !StringUtils.isEmpty(content)) {
		// title = tradSimpConvertor.chineseFan2Jan(title);
		// content = tradSimpConvertor.chineseFan2Jan(content);
		//
		// KeyWordComputer kwc = new KeyWordComputer(KEY_WORD_COUNT);
		// Collection<Keyword> keywordList = kwc.computeArticleTfidf(title, content);
		// log.info("keywords: {}", keywordList);
		//
		// if (null != keywordList && !keywordList.isEmpty()) {
		// Set<String> realKeywords = new HashSet<String>();
		// for (Keyword kw : keywordList) {
		// if (StringUtils.length(kw.getName()) > 1)
		// realKeywords.add(kw.getName());
		// }
		//
		//
		// if (!realKeywords.isEmpty()) {
		// String keywords = newParams.get(SolrFieldDefinition.KEYWORDS.getName());
		// try {
		// Collection<String> originKeywordList = JsonUtil.getMapper().readValue(keywords, Collection.class);
		// if (null != originKeywordList && !originKeywordList.isEmpty())
		// realKeywords.addAll(originKeywordList);
		// } catch (Exception e) {
		// try {
		// String originKeyword = JsonUtil.getMapper().readValue(keywords, String.class);
		// realKeywords.add(originKeyword);
		// } catch (Exception ex) {
		// realKeywords.add(keywords);
		// }
		//
		// }
		//
		// newParams.put(SolrFieldDefinition.KEYWORDS.getName(), JsonUtil.getMapper().writeValueAsString(realKeywords));
		// }
		//
		// // Save keyword into hdfs./
		// // String dateStr = DATE_FORMAT.format(new Date());
		// // String requestId = request.getRequestId();
		// // String filePath = LOCAL_FILE_DIR + dateStr + "/" + requestId;
		// // try {
		// // File uploadFile = new File(filePath);
		// // FileUtils.writeLines(uploadFile, "utf-8", realKeywords);
		// //
		// // if (uploadFile.exists()) {
		// // String dest = HDFS_URL + dateStr + "/" + requestId;
		// // FacebookHDFSUtil.uploadLocalFile2HDFS(filePath, dest);
		// // FileUtils.deleteQuietly(uploadFile);
		// // log.info("Upload file to HDFS successfully, source path: {}, dest path: {}", filePath, dest);
		// // }
		// // } catch (Throwable e) {
		// // log.error(e.getMessage(), e);
		// // }
		//
		// }
		//
		// }

		TaskRequest newRequest = new TaskRequest(request.getRequestId(), null, request.getType(), request.getSourceType(), newParams);

		// Emit document to sentiment.
		DocumentResult docResult = new DocumentResult(newRequest);
		Document doc = new Document();
		doc.setSourceType(SourceType.getSourceType(request.getSourceType()));
		doc.setId(request.getRequestId());
		doc.setUrl(newParams.get(SolrFieldDefinition.URL.getName()));
		doc.setCategory("7");
		doc.setCatTags(Arrays.asList("99"));
		doc.setTitle(title);
		doc.setContent(content);
		docResult.setDocument(doc);
		emit(input, docResult, "Document ready to sentiment, doc ID: " + doc.getId() + ", URL: " + doc.getUrl());
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in extract keyword", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

}
