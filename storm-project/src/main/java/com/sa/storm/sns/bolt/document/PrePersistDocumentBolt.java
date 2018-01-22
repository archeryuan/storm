package com.sa.storm.sns.bolt.document;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.common.json.JsonUtil;
import com.sa.redis.definition.RedisDefinition.CrawlTaskDef;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

public class PrePersistDocumentBolt extends BaseBolt {

	private static final long serialVersionUID = 4586670132080308429L;

	private static final Logger log = LoggerFactory.getLogger(PrePersistDocumentBolt.class);

	private RedisUtil redisUtil;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		try {
			redisUtil = RedisUtil.getInstance();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void process(Tuple input) throws Exception {
		DocumentResult docResult = (DocumentResult) getInputByClass(input, DocumentResult.class);
		Document doc = (Document) docResult.getDocument();

		if (null == doc) {
			TaskResult taskResult = new TaskResult(docResult, "Doc is empty ", TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
			return;
		}

		final Map<String, String> params = docResult.getParams();
		Map<String, String> newParams = new HashMap<String, String>();
		newParams.putAll(params);

		// newParams.put(SolrFieldDefinition.SENTIMENT_SCORE.getName(), String.valueOf(doc.getSentimentScore()));

		redisUtil.sadd(CrawlTaskDef.DOC_INFO, JsonUtil.getMapper().writeValueAsString(newParams));

		log.info("Pre-persist document, id: {}, url: {}", doc.getId(), doc.getUrl());

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in pre-persist", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

}
