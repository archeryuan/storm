/**
 * 
 */
package com.sa.storm.sns.bolt.analysis;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.analysis.sentiment.SemantriaAnalysisEngine;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

/**
 * @author Jason
 *
 */
@Deprecated
public class SemantriaAnalysisBolt extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private SemantriaAnalysisEngine engine;

	private static final Logger log = LoggerFactory.getLogger(SemantriaAnalysisEngine.class);

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		try {
			engine = new SemantriaAnalysisEngine();
		} catch (Exception e) {
			log.error("Error in prepare", e);
		}
	}

	@Override
	public void process(Tuple input) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		final Map<String, String> params = request.getParams();

		if (null == params || params.isEmpty()) {
			TaskResult taskResult = new TaskResult(request, "No params ", TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
			return;
		}

		String title = params.get(SolrFieldDefinition.TITLE.getName());
		String content = params.get(SolrFieldDefinition.CONTENT.getName());
		String id = request.getRequestId();
		DocumentResult docResult = new DocumentResult(request);

		Document doc = new Document();
		doc.setTitle(title);
		doc.setContent(content);
		doc.setId(id);

		// DocAnalyticData apiResult = engine.runSingleDetailedAnalysis(id, content); // This object contains all necessary detailed
		// analysis
		// // results
		// doc.setSentimentScore(new Float(apiResult.getSentimentScore()));
		// docResult.setDocument(doc);
		//
		// emit(input, docResult,
		// "Finished semantria analysis successfully, doc ID: " + apiResult.getId() + ", sentiment: " + apiResult.getSentimentScore()
		// + ", sentiment polarity: " + apiResult.getSentimentPolarity());

	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, DocumentResult.class);
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		DocumentResult docResult = (DocumentResult) getInputByClass(input, DocumentResult.class);
		TaskResult error = new TaskResult(docResult, "Error in sentiment analysis", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

}
