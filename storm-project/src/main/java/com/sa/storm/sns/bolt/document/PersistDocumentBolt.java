package com.sa.storm.sns.bolt.document;

import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.crawler.persist.Persistor;
import social.hunt.solr.util.IndexDedupUtil;
import social.hunt.storm.domain.tuple.PersistTaskResult;
import social.hunt.storm.domain.tuple.solr.SolrInputDocumentMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.definition.SourceType;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.util.WebSocketUtil;

public class PersistDocumentBolt extends BaseBolt {

	private static final Logger log = LoggerFactory.getLogger(PersistDocumentBolt.class);

	private PersistorMap persistorMap;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		persistorMap = new PersistorMap();
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {

	}

	@SuppressWarnings("unchecked")
	@Override
	public void process(Tuple input) throws Exception {
		// TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		// final Map<String, String> params = request.getParams();
		// final SolrInputDocumentMap inputDocMap = new SolrInputDocumentMap();
		//
		// if (null == params || params.isEmpty()) {
		// TaskResult taskResult = new TaskResult(request, "No params ", TupleDefinition.Result.FAIL.getCode());
		// emit(input, taskResult);
		// return;
		// }
		//
		// SolrFieldDefinition[] solrFieldDefinitions = SolrFieldDefinition.values();
		// if (null == solrFieldDefinitions || solrFieldDefinitions.length <= 0) {
		// log.error("No solr field definition");
		// return;
		// }
		//
		// String batchDocInfoStr = params.get(TupleDefinition.Param.BATCH_DOC_INFO.getCode());
		// if (StringUtils.isEmpty(batchDocInfoStr)) {
		// log.error("No batch doc info");
		// return;
		// }
		//
		// Set<String> batchDocInfo = JsonUtil.getMapper().readValue(batchDocInfoStr, Set.class);
		// if (null == batchDocInfo || batchDocInfo.isEmpty()) {
		// log.error("Read batch doc info error");
		// return;
		// }
		//
		// // List<String> urlIndexes = new ArrayList<String>();
		// // Map<String, Long> forumUrl2Page = new HashMap<String, Long>();
		//
		// for (String docInfo : batchDocInfo) {
		// Map<String, String> docParam = null;
		// try {
		// docParam = JsonUtil.getMapper().readValue(docInfo, Map.class);
		// } catch (Exception e) {
		// log.error("Read error docInfo: {}", docInfo);
		// log.error(e.getMessage(), e);
		// continue;
		// }
		//
		// SolrInputDocument solrDoc = new SolrInputDocument();
		// for (SolrFieldDefinition solrFieldDefinition : solrFieldDefinitions) {
		// String name = solrFieldDefinition.getName();
		// String value = docParam.get(name);
		//
		// if (!StringUtils.isEmpty(value)) {
		// try {
		// solrDoc.addField(name, JsonUtil.getMapper().readValue(value, solrFieldDefinition.getType()));
		// } catch (Exception e) {
		// Object finalValue;
		// try {
		// finalValue = JsonUtil.getMapper().readValue(value, String.class);
		// } catch (Exception ex) {
		// try {
		// Collection<String> col = JsonUtil.getMapper().readValue(value, Collection.class);
		// finalValue = value;
		// if (null != col && !col.isEmpty()) {
		// for (String str : col) {
		// finalValue = str;
		// break;
		// }
		// }
		//
		// } catch (Exception exc) {
		// finalValue = value;
		// }
		//
		// }
		// solrDoc.addField(name, finalValue);
		// log.info("Solr field type not matched, name: {}, value: {}, finalValue: {}",
		// new Object[] { name, value, finalValue });
		// // log.error(e.getMessage(), e);
		// }
		// }
		// }
		//
		// @SuppressWarnings("deprecation")
		// SourceType st = SourceType.getSourceType((Integer) solrDoc.getFieldValue(SolrFieldDefinition.SOURCE_TYPE.getName()));
		// // social.hunt.common.definition.SourceType newSourceType =
		//
		// // Special for others source type.
		// if (SourceType.OTHERS == st) {
		// Date now = new Date();
		//
		// // Set create date paging field.
		// String createDatePaging = getDatePaging(now);
		// solrDoc.addField(SolrFieldDefinition.CREATE_DATE_PAGE.getName(), createDatePaging);
		//
		// // Set publish date paging field.
		// Date publishDate = null;
		// Object value = solrDoc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());
		//
		// if (null != value && value instanceof Date) {
		// publishDate = (Date) value;
		// }
		//
		// if (null == publishDate) {
		// publishDate = now;
		// }
		// String publishDatePaging = getDatePaging(publishDate);
		// solrDoc.addField(SolrFieldDefinition.PUBLISH_DATE_PAGE.getName(), publishDatePaging);
		//
		// }
		//
		// // solrDoc.addField(SolrFieldDefinition.CONTENT_HASH.getName(), generateHashFromDoc(solrDoc));
		// inputDocMap.get(st).add(solrDoc);
		//
		// String pUrl = docParam.get(SolrFieldDefinition.PARENT_URL.getName());
		// String url = docParam.get(SolrFieldDefinition.URL.getName());
		//
		// log.info("Ready to persist document, url: {}, pUrl: {}", url, pUrl);
		// }

		PersistTaskResult taskResult = (PersistTaskResult) getInputByClass(input, PersistTaskResult.class);
		SolrInputDocumentMap inputDocMap = taskResult.getDocumentMap();

		try {
			for (SourceType sourceType : inputDocMap.keySet()) {
				// List<SolrInputDocument> docs = IndexDedupUtil.preIndex(inputDocMap.get(sourceType));
				List<SolrInputDocument> docs = inputDocMap.get(sourceType);
				
				for(SolrInputDocument ddd : docs){
					String pttUrl = (String)ddd.getFieldValue((SolrFieldDefinition.URL.getName()));
					if(pttUrl.contains("am730.com.hk")){
						log.info("persist doc ================================="+ddd.get(SolrFieldDefinition.URL.getName()));
					}
				}

				if (!docs.isEmpty()) {
					Persistor persistor = persistorMap.get(sourceType);
					if (persistor == null) {
						log.info("{} post is currently not supported by our indexer.", sourceType);
					} else {
						persistor.doPersist(docs);

						IndexDedupUtil.addRecords(docs.toArray(new SolrInputDocument[] {}));

						// persistor.doPersist(docs, clientManager);
						log.debug("Persist {} post: {}", sourceType, docs);
						log.info("Persist {} post, total: {}", sourceType, docs.size());

						// Publish keyword feed coming notification.
						if (sourceType == SourceType.OTHERS) {
							WebSocketUtil.publishProfileKeywordNotificationEvent(docs, null, null, 200);
						}
					}
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		}

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		PersistTaskResult request = (PersistTaskResult) getInputByClass(input, PersistTaskResult.class);
		TaskResult error = new TaskResult(request, "Error in persist", e, TupleDefinition.Result.FAIL.getCode());
		//emit(input, error);
	}

}
