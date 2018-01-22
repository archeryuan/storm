/**
 * 
 */
package com.sa.storm.sns.bolt.analysis;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.analysis.sentiment.SemantriaAnalysisEngineBatch;
import social.hunt.analysis.sentiment.SolrInputDocUtil;
import social.hunt.solr.util.IndexDedupUtil;
import social.hunt.storm.domain.tuple.DeduplicationTaskResult;
import social.hunt.storm.domain.tuple.PersistTaskResult;
import social.hunt.storm.domain.tuple.SemantriaTaskResult;
import social.hunt.storm.domain.tuple.solr.SolrInputDocumentMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.TraditionalChineseTokenizer;
import com.sa.common.config.CommonConfig;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.definition.SourceType;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskResult;
import com.semantria.mapping.output.DocAnalyticData;

import hunt.social.sentiment.SentimentUtil;

/**
 * @author Jason
 *
 */
public class SemantriaBatchAnalysisBolt extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private SemantriaAnalysisEngineBatch engine;

	SolrInputDocUtil util;
	private final static String SENTIMENT_URL = "http://ch-prepro:9898/getSentimentResult";

	private int taskIndex;

	private static final Logger log = LoggerFactory.getLogger(SemantriaAnalysisEngineBatch.class);

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		try {
			CommonConfig commonConfig = CommonConfig.getInstance();
			util = new SolrInputDocUtil();
//			taskIndex = context.getThisTaskIndex();
//			if (commonConfig.getEnvironmentLabel().equals("local")) {
//				boolean useConfigPool = false;
//				// engine = new SemantriaAnalysisEngineBatch(useConfigPool);
//				engine = new SemantriaAnalysisEngineBatch();
//				log.info("The config pool IS NOT used.");
//			} else {
//				engine = new SemantriaAnalysisEngineBatch(taskIndex);
//			}

		} catch (Exception e) {
			log.error("Error in prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, SemantriaTaskResult.class);
	}

	@Override
	public void process(Tuple input) throws Exception {
		DeduplicationTaskResult taskResult = (DeduplicationTaskResult) getInputByClass(input,
				DeduplicationTaskResult.class);
		SolrInputDocumentMap inputDocMap = taskResult.getDocumentMap();
		SolrInputDocumentMap resultDocMap = new SolrInputDocumentMap();
		List<DocAnalyticData> resultDocs = new ArrayList<DocAnalyticData>();
		HashMap<String, String> sentimentResults = new HashMap<String, String>();
		int source = 0, processedCount = 0;

		log.info("Starting to loop all source type. With total sourceTyes: " + inputDocMap.keySet().size());
		for (SourceType sourceType : inputDocMap.keySet()) {
			log.info("Current sourceType: " + sourceType.name() + " with id = " + ++source);
			try {
				List<SolrInputDocument> solrDocList = IndexDedupUtil.preIndex(inputDocMap.get(sourceType));
				if (!solrDocList.isEmpty()) {

//					log.info("begin sentiment analysis ==============" + solrDocList.hashCode());
//					sentimentResults = engine.runSentimentAnalysis(solrDocList);
//					log.info("end sentiment analysis ==============" + solrDocList.hashCode());
//
//					for (SolrInputDocument ddd : inputDocMap.get(sourceType)) {
//						String pttUrl = (String) ddd.getFieldValue((SolrFieldDefinition.URL.getName()));
//						if (pttUrl.contains("tianya.cn")) {
//							Date d = new Date();
//							// RedisUtil.getInstance().rpush("end-semantria",
//							// pttUrl+"###"+d);
//							log.info(pttUrl + "###" + d);
//						}
//					}
//
//					log.info("Put results to resultDocMap");
					resultDocMap.put(sourceType, sentimentAnalyse(solrDocList));
					log.info("Put results to resultDocMap----------complete");
				}
			} catch (Exception e) {
				if (processedCount > 0) {
					log.info("One of the batch is having timeout and skipped.");
				} else {
					log.error(e.getMessage(), e);
					throw e;
				}
			}
		}

		log.info("Complete source type looping.");

		if (log.isInfoEnabled()) {
			log.info("Number of documents analysed are: {}", resultDocs.size());
			// log.info("Semantria tokens left: {}", engine.getTransctions());
		}

		final SemantriaTaskResult newRequest = new SemantriaTaskResult(taskResult, resultDocMap);
		log.info("SemantriaTaskResultId: " + newRequest.getRequestId());
		this.emitWithoutAnchor(newRequest, "Finished sentiment analysis with Semantria successfully.");
	}

	public List<SolrInputDocument> sentimentAnalyse(List<SolrInputDocument> docs) {
		List<SolrInputDocument> updatedList = new ArrayList<SolrInputDocument>();
		for (SolrInputDocument doc : docs) {
			String content = util.extractTitleContent(doc);
			
			
//			try {
//				log.info("Content of post request: " + util.extractTitleContent(doc));
//				String response = null;
//				response = getHttpPostRequest(content);
//				JsonObject updateData = new Gson().fromJson(response, JsonObject.class);
//
//				if (updateData != null) {
//					log.info("The conetent of json object: " + updateData.toString());
//
//					// Sentiment Score
//					if (updateData.get("polarity") != null) {
//						Float score = Float.valueOf(updateData.get("polarity").toString());
//						doc.setField(SolrFieldDefinition.SENTIMENT_SCORE.getName(), score);
//						log.info("doc with url: " + doc.getField("url").toString() + "'s new polarity is " + score);
//					}
//
//					// Keywords
//					Gson gson = new Gson();
//
//					HashSet<String> keywords = new HashSet<String>();
//					HashSet<String> posWords = new HashSet<String>();
//					HashSet<String> negWords = new HashSet<String>();
//
//					if (updateData.get("keywords") != null) {
//						keywords.addAll(gson.fromJson(updateData.get("keywords").toString(), HashSet.class));
//						log.info("Keywords found: " + keywords.toString());
//					}
//					if (updateData.get("pos") != null) {
//						posWords.addAll(gson.fromJson(updateData.get("pos").toString(), HashSet.class));
//						log.info("Positive words found: " + posWords.toString());
//					}
//					if (updateData.get("neg") != null) {
//						negWords.addAll(gson.fromJson(updateData.get("neg").toString(), HashSet.class));
//						log.info("Negative words found: " + negWords.toString());
//					}
//					if (updateData.get("neg_word") != null) {
//						negWords.addAll(gson.fromJson(updateData.get("neg_word").toString(), HashSet.class));
//						log.info("Neg_word words found: " + negWords.toString());
//					}
//					if (updateData.get("pos_word") != null) {
//						posWords.addAll(gson.fromJson(updateData.get("pos_word").toString(), HashSet.class));
//						log.info("Pos_word words found: " + posWords.toString());
//					}
//
//					if (keywords != null) {
//						if (posWords != null) {
//							for (String word : posWords) {
//								keywords.remove(word);
//							}
//						}
//						if (negWords != null) {
//							for (String word : negWords) {
//								keywords.remove(word);
//							}
//						}
//					}
//
//					doc.setField(SolrFieldDefinition.NEUTRAL_KEYWORDS.getName(), keywords);
//					log.info("Neutral words added: " + keywords.toString());
//					doc.setField(SolrFieldDefinition.POS_KEYWORDS.getName(), posWords);
//					log.info("Positive words added: " + posWords.toString());
//					doc.setField(SolrFieldDefinition.NEG_KEYWORDS.getName(), negWords);
//					log.info("Negative words added: " + negWords.toString());
//
//					log.info("Updating new sentiment and keywords are completed here.");
//
//				} else {
//					log.info("This content is null coming from sentiment");
//				}
//
//			} catch (Exception e) {
//
//			}

//			List<Term> termList = TraditionalChineseTokenizer.segment(content);
			
			Set<String> personsStr = new HashSet<String>();
			Set<String> companiesStr = new HashSet<String>();
			Set<String> placesStr = new HashSet<String>();
			Set<String> themes = new HashSet<String>();

			boolean ifTraditional = false;
			String sContent = HanLP.hk2s(content);
			if(!sContent.equalsIgnoreCase(content)){
				ifTraditional = true;
			}
			
			float score = SentimentUtil.getSentimentScore(sContent);
			if(score > 0) {
				score = 1;
			}else if(score < 0) {
				score = -1;
			}else {
				score = 0;
			}
			
			 List<String> keywordList = HanLP.extractKeyword(sContent, 25);
			 List<String> phraseList = HanLP.extractPhrase(sContent, 25);
		     
			 for(String theme : keywordList){
				 if(ifTraditional){
					 themes.add(HanLP.s2hk(theme));
				 }else{
					 themes.add(theme);
				 }
			 }
			 
			 for(String theme : phraseList){
				 if(ifTraditional){
					 themes.add(HanLP.s2hk(theme));
				 }else{
					 themes.add(theme);
				 }
			 }
			 
//			 
			 
			 doc.setField(SolrFieldDefinition.PERSONS.getName(), personsStr);
			 doc.setField(SolrFieldDefinition.COMPANIES.getName(), companiesStr);
			 doc.setField(SolrFieldDefinition.PLACES.getName(), placesStr);
			 doc.setField(SolrFieldDefinition.NEUTRAL_THEMES.getName(), themes);
			 doc.setField(SolrFieldDefinition.SENTIMENT_SCORE.getName(), score);
			 log.info("score==========="+doc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName()));
			
			updatedList.add(doc);
		}
		return updatedList;

	}

	private String getHttpPostRequest(String params) {
		log.info("Starting to call POST request to " + SENTIMENT_URL + ".");
		try {
			HttpClient client = new DefaultHttpClient();
			HttpPost post = new HttpPost(SENTIMENT_URL);
			List<BasicNameValuePair> parameters = new ArrayList<BasicNameValuePair>();
			parameters.add(new BasicNameValuePair("content", params));
			HttpEntity entity;
			entity = new UrlEncodedFormEntity(parameters, "UTF-8");
			post.setEntity(entity);
			log.info("About to execute post request with value: " + params);
			HttpResponse response = client.execute(post);
			log.info("Response Code : " + response.getStatusLine().getStatusCode());
			log.info("Post Parameters : " + params);
			String responseText = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
			System.out.println(responseText);
			log.info("Call POST request to " + SENTIMENT_URL + " is about to return successfully.");
			return responseText;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private void waitFor(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			log.info("Sleep is being interrupted.");
			Thread.interrupted();
		}
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		DeduplicationTaskResult request = (DeduplicationTaskResult) getInputByClass(input,
				DeduplicationTaskResult.class);
		TaskResult error = new TaskResult(request, "Error in analysis", e, TupleDefinition.Result.FAIL.getCode());
		// emit(input, error);
	}
}
