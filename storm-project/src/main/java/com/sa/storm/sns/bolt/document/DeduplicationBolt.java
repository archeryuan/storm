/**
 * 
 */
package com.sa.storm.sns.bolt.document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.solr.definition.SolrCollection;
import social.hunt.solr.util.IndexDedupUtil;
import social.hunt.solr.util.SolrQueryUtil;
import social.hunt.storm.domain.tuple.DeduplicationTaskResult;
import social.hunt.storm.domain.tuple.PersistTaskResult;
import social.hunt.storm.domain.tuple.SemantriaTaskResult;
import social.hunt.storm.domain.tuple.solr.SolrInputDocumentMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.definition.SourceType;
import com.sa.common.json.JsonUtil;
import com.sa.common.util.StringUtils;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

/**
 * Class to discover already indexed document<BR>
 * To retrieve and merge document index if necessary.<BR>
 * 
 * @author lewis
 *
 */
@SuppressWarnings("deprecation")
public class DeduplicationBolt extends BaseBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 954702074938276714L;
	private static final Logger log = LoggerFactory.getLogger(DeduplicationBolt.class);

	/**
	 * 
	 */
	public DeduplicationBolt() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sa.storm.bolt.BaseBolt#prepare(java.util.Map,
	 * org.apache.storm.task.TopologyContext,
	 * org.apache.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		// persistorMap = new PersistorMap();
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, DeduplicationTaskResult.class);
		declareOutputByClass(declarer, SemantriaTaskResult.class);
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public void process(Tuple input) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		final Map<String, String> params = request.getParams();
		final SolrInputDocumentMap inputDocMap = new SolrInputDocumentMap();
		final SolrInputDocumentMap byPassDocMap = new SolrInputDocumentMap();

		if (null == params || params.isEmpty()) {
			TaskResult taskResult = new TaskResult(request, "No params ", TupleDefinition.Result.FAIL.getCode());
			// emit(input, taskResult);
			return;
		}

		SolrFieldDefinition[] solrFieldDefinitions = SolrFieldDefinition.values();
		if (null == solrFieldDefinitions || solrFieldDefinitions.length <= 0) {
			log.error("No solr field definition");
			return;
		}

		String batchDocInfoStr = params.get(TupleDefinition.Param.BATCH_DOC_INFO.getCode());
		if (StringUtils.isEmpty(batchDocInfoStr)) {
			log.error("No batch doc info");
			return;
		}

		Set<String> batchDocInfo = JsonUtil.getMapper().readValue(batchDocInfoStr, Set.class);
		if (null == batchDocInfo || batchDocInfo.isEmpty()) {
			log.error("Read batch doc info error");
			return;
		}

		for (String docInfo : batchDocInfo) {
			Map<String, String> docParam = null;
			try {
				docParam = JsonUtil.getMapper().readValue(docInfo, Map.class);
			} catch (Exception e) {
				log.error("Read error docInfo: {}", docInfo);
				log.error(e.getMessage(), e);
				continue;
			}

			SolrInputDocument solrDoc = new SolrInputDocument();
			for (SolrFieldDefinition solrFieldDefinition : solrFieldDefinitions) {
				String name = solrFieldDefinition.getName();
				String value = docParam.get(name);

				if (!StringUtils.isEmpty(value)) {
					try {
						solrDoc.addField(name, JsonUtil.getMapper().readValue(value, solrFieldDefinition.getType()));
					} catch (Exception e) {
						Object finalValue;
						try {
							finalValue = JsonUtil.getMapper().readValue(value, String.class);
						} catch (Exception ex) {
							try {
								Collection<String> col = JsonUtil.getMapper().readValue(value, Collection.class);
								finalValue = value;
								if (null != col && !col.isEmpty()) {
									for (String str : col) {
										finalValue = str;
										break;
									}
								}
							} catch (Exception exc) {
								finalValue = value;
							}

						}
						solrDoc.addField(name, finalValue);
						log.info("Solr field type not matched, name: {}, value: {}, finalValue: {}",
								new Object[] { name, value, finalValue });
						// log.error(e.getMessage(), e);
					}
				}
			}

			SourceType st = SourceType
					.getSourceType((Integer) solrDoc.getFieldValue(SolrFieldDefinition.SOURCE_TYPE.getName()));

			// Special for others source type.
			if (SourceType.OTHERS == st) {
				Date now = new Date();

				// Set create date paging field.
				String createDatePaging = getDatePaging(now);
				solrDoc.addField(SolrFieldDefinition.CREATE_DATE_PAGE.getName(), createDatePaging);

				// Set publish date paging field.
				Date publishDate = null;
				Object value = solrDoc.getFieldValue(SolrFieldDefinition.PUBLISH_DATE.getName());

				if (null != value && value instanceof Date) {
					publishDate = (Date) value;
				}

				if (null == publishDate) {
					publishDate = now;
				}
				String publishDatePaging = getDatePaging(publishDate);
				solrDoc.addField(SolrFieldDefinition.PUBLISH_DATE_PAGE.getName(), publishDatePaging);

			}

			inputDocMap.get(st).add(solrDoc);

			String pUrl = docParam.get(SolrFieldDefinition.PARENT_URL.getName());
			String url = docParam.get(SolrFieldDefinition.URL.getName());

			// log.info("Ready to persist document, url: {}, pUrl: {}", url,
			// pUrl);
		}

		log.info("Current batch contains records of {}", inputDocMap.keySet());

		try {
			for (SourceType sourceType : new ArrayList<SourceType>(inputDocMap.keySet())) {

				if (SourceType.OTHERS == sourceType || SourceType.NEWS == sourceType || SourceType.BLOG == sourceType
						|| SourceType.GOVERNMENT == sourceType) {
					List<SolrInputDocument> docs = IndexDedupUtil.preIndex(inputDocMap.get(sourceType));

					if (!docs.isEmpty()) {
						// Load and merge with existing index
						mergeIndexIfExists(docs);

						inputDocMap.put(sourceType, docs);
					} else {
						inputDocMap.get(sourceType).clear();
					}
				} else {
					// Dedup SNS separately, bypass it to PersistDocumentBolt
					// straight away if exists
					List<SolrInputDocument> snsDocs = inputDocMap.get(sourceType);

					if (!snsDocs.isEmpty()) {
						List<SolrInputDocument> oldSnsDocs = new ArrayList<SolrInputDocument>();
						List<SolrInputDocument> newSnsDocs = new ArrayList<SolrInputDocument>();

						Map<String, SolrDocument> existingDocMap = retrieveExistingIndex(snsDocs);
						if (!existingDocMap.isEmpty()) {
							for (SolrInputDocument doc : snsDocs) {
								String url = (String) doc.getFieldValue(SolrFieldDefinition.URL.getName());
								if (!StringUtils.isBlank(url)) {
									SolrDocument solrDoc = existingDocMap.get(url);
									if (solrDoc != null) {
										mergeSnsDoc(doc, solrDoc);

										if (doc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName()) != null && doc
												.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName()) != null) {
											if (solrDoc.getFieldValue(SolrFieldDefinition.READ_COUNT.getName()) != null
													&& solrDoc.getFieldValue(
															SolrFieldDefinition.COMMENT_COUNT.getName()) != null) {
												long docReadCnt = (long) doc
														.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
												long docCmtCnt = (long) doc
														.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
												long solrDocReadCnt = (long) solrDoc
														.getFieldValue(SolrFieldDefinition.READ_COUNT.getName());
												long solrDocCmtCnt = (long) solrDoc
														.getFieldValue(SolrFieldDefinition.COMMENT_COUNT.getName());
												if (docReadCnt > solrDocReadCnt || docCmtCnt > solrDocCmtCnt) {
													oldSnsDocs.add(doc);
													log.info("old SNS url: {}", (String) doc
															.getFieldValue(SolrFieldDefinition.URL.getName()));
												}
											} else {
												oldSnsDocs.add(doc);
												log.info("old SNS url: {}",
														(String) doc.getFieldValue(SolrFieldDefinition.URL.getName()));
											}
										}

									} else {
										log.info("new SNS url: {}",
												(String) doc.getFieldValue(SolrFieldDefinition.URL.getName()));
										newSnsDocs.add(doc);
									}
								}
							}
						} else {
							log.info("No existing solr, new sns docs size:{}", newSnsDocs.size());
							newSnsDocs.addAll(snsDocs);
						}

						log.info("totalsnsDocs: {},oldSNS docs: {}  newSNS docs: {}",
								new Object[] { snsDocs.size(), oldSnsDocs.size(), newSnsDocs.size() });

						if (!oldSnsDocs.isEmpty())
							byPassDocMap.put(sourceType, oldSnsDocs);
						if (!newSnsDocs.isEmpty())
							inputDocMap.put(sourceType, newSnsDocs);
					} else {
						inputDocMap.get(sourceType).clear();
					}
				}
//				for (SolrInputDocument ddd : inputDocMap.get(sourceType)) {
//					String pttUrl = (String) ddd.getFieldValue((SolrFieldDefinition.URL.getName()));
//					if (pttUrl.contains("instagram.com")) {
//						log.info("input doc ================================="
//								+ ddd.get(SolrFieldDefinition.URL.getName()));
//					}
//				}

//				for (SolrInputDocument ddd : inputDocMap.get(sourceType)) {
//					String pttUrl = (String) ddd.getFieldValue((SolrFieldDefinition.URL.getName()));
//					if (pttUrl.contains("am730.com.hk")) {
//						log.info("input doc ================================="
//								+ ddd.get(SolrFieldDefinition.URL.getName()));
//					}
//					float minX = -5.0f;
//					float maxX = 5.0f;
//					Random rand = new Random();
//					float finalX = rand.nextFloat() * (maxX - minX) + minX;
//					ddd.setField(SolrFieldDefinition.SENTIMENT_SCORE.getName(), finalX);
//				}

			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		}

		log.info("Bypassed Document size: {}", byPassDocMap.size());
		final SemantriaTaskResult byPassRequest = new SemantriaTaskResult(request, byPassDocMap);
		this.emitWithoutAnchor(byPassRequest, "Bypassed Result");

//		final SemantriaTaskResult firstPersistReq = new SemantriaTaskResult(request, inputDocMap);
//		this.emitWithoutAnchor(firstPersistReq, "firstPersistReq Result");

		log.info("Normal Stream Document size: {}", inputDocMap.size());
		final DeduplicationTaskResult newRequest = new DeduplicationTaskResult(request, inputDocMap);
		this.emitWithoutAnchor(newRequest, "Deduplicated result");
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in deduplication", e, TupleDefinition.Result.FAIL.getCode());
		// emit(input, error);
	}

	protected String getDatePaging(Date date) {
		if (null == date)
			return null;

		String dateStr = String.valueOf(Long.MAX_VALUE - date.getTime());
		Random rand = new Random();
		String randStr = String.valueOf(rand.nextInt(Integer.MAX_VALUE));

		StringBuilder sb = new StringBuilder();
		sb.append(StringUtils.leftPad(dateStr, 20, "0")).append("_").append(StringUtils.leftPad(randStr, 10, "0"));

		return sb.toString();

	}

	protected void mergeIndexIfExists(List<SolrInputDocument> docs) throws SolrServerException, IOException {
		Map<String, SolrDocument> existingDocMap = retrieveExistingIndex(docs);
		if (!existingDocMap.isEmpty()) {
			log.debug("Got data");

			for (SolrInputDocument doc : docs) {
				String url = (String) doc.getFieldValue(SolrFieldDefinition.URL.getName());
				if (!StringUtils.isBlank(url)) {
					SolrDocument solrDoc = existingDocMap.get(url);
					if (solrDoc != null) {
						// Do Merge
						mergeDoc(doc, solrDoc);
					}
				}
			}
		}
	}

	protected void mergeDoc(SolrInputDocument inputDoc, SolrDocument solrDoc) {
		log.debug("mergeDoc");

		mergeField(inputDoc, solrDoc, SolrFieldDefinition.CREATE_DATE);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PUBLISH_DATE);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.CREATE_DATE_PAGE);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PUBLISH_DATE_PAGE);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.KEYWORDS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.CONTENT);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.POS_KEYWORDS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.NEG_KEYWORDS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.NEUTRAL_KEYWORDS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.POS_THEMES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.NEG_THEMES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.NEUTRAL_THEMES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PATTERNS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PERSONS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PLACES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PRODUCTS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.JOB_TITLES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.COMPANIES);

		// Merge sentiment score if SourceType is SNS
		if (social.hunt.common.definition.SourceType.SNS
				.getSourceTypeId() == (int) inputDoc.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE.getName())) {

			mergeField(inputDoc, solrDoc, SolrFieldDefinition.SENTIMENT_SCORE);
		}
	}

	protected void mergeSnsDoc(SolrInputDocument inputDoc, SolrDocument solrDoc) {
		log.info("mergeDoc");

		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PUBLISH_DATE);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.CREATE_DATE_PAGE);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PUBLISH_DATE_PAGE);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.KEYWORDS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.CONTENT);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.POS_KEYWORDS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.NEG_KEYWORDS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.NEUTRAL_KEYWORDS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.POS_THEMES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.NEG_THEMES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.NEUTRAL_THEMES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PATTERNS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PERSONS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PLACES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.PRODUCTS);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.JOB_TITLES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.COMPANIES);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.SENTIMENT_SCORE);
		mergeField(inputDoc, solrDoc, SolrFieldDefinition.Region);

	}

	protected void mergeField(SolrInputDocument inputDoc, SolrDocument solrDoc, SolrFieldDefinition field) {
		log.debug("merge {} field", field.getName());

		if (solrDoc.containsKey(field.getName()))
			if (field.getType().equals(Collection.class)
					|| solrDoc.getFieldValue(field.getName()) instanceof Collection) {
				Collection<Object> newColl = inputDoc.getFieldValues(field.getName());
				inputDoc.setField(field.getName(), solrDoc.getFieldValues(field.getName()));
				if (newColl != null && !newColl.isEmpty()) {
					for (Object value : newColl) {
						if (inputDoc.getFieldValues(field.getName()) != null
								&& !inputDoc.getFieldValues(field.getName()).contains(value))
							inputDoc.addField(field.getName(), value);
					}
				}
			} else {
				inputDoc.setField(field.getName(), solrDoc.getFieldValue(field.getName()));
			}
	}

	protected Map<String, SolrDocument> retrieveExistingIndex(List<SolrInputDocument> docs)
			throws SolrServerException, IOException {
		Map<String, SolrDocument> outMap = new HashMap<String, SolrDocument>();
		Set<String> urls = new HashSet<String>();
		for (SolrInputDocument doc : docs) {
			String url = (String) doc.getFieldValue(SolrFieldDefinition.URL.getName());
			if (!StringUtils.isBlank(url)) {
				urls.add(StringUtils.doubleQuote(url));
			}
		}

		SolrQuery query = new SolrQuery();
		query.addField(SolrFieldDefinition.CREATE_DATE.getName());
		query.addField(SolrFieldDefinition.PUBLISH_DATE.getName());
		query.addField(SolrFieldDefinition.CREATE_DATE_PAGE.getName());
		query.addField(SolrFieldDefinition.PUBLISH_DATE_PAGE.getName());
		query.addField(SolrFieldDefinition.CONTENT.getName());
		query.addField(SolrFieldDefinition.SENTIMENT_SCORE.getName());
		query.addField(SolrFieldDefinition.KEYWORDS.getName());
		query.addField(SolrFieldDefinition.URL.getName());
		query.addField(SolrFieldDefinition.POS_KEYWORDS.getName());
		query.addField(SolrFieldDefinition.NEG_KEYWORDS.getName());
		query.addField(SolrFieldDefinition.NEUTRAL_KEYWORDS.getName());
		query.addField(SolrFieldDefinition.POS_THEMES.getName());
		query.addField(SolrFieldDefinition.NEG_THEMES.getName());
		query.addField(SolrFieldDefinition.NEUTRAL_THEMES.getName());
		query.addField(SolrFieldDefinition.PATTERNS.getName());
		query.addField(SolrFieldDefinition.PERSONS.getName());
		query.addField(SolrFieldDefinition.PLACES.getName());
		query.addField(SolrFieldDefinition.PRODUCTS.getName());
		query.addField(SolrFieldDefinition.JOB_TITLES.getName());
		query.addField(SolrFieldDefinition.COMPANIES.getName());
		query.addField(SolrFieldDefinition.Region.getName());
		query.setQuery(new StringBuilder("+").append(SolrFieldDefinition.URL.getName()).append(":(")
				.append(StringUtils.join(urls, " ")).append(")").toString());

		SolrQueryUtil util = new SolrQueryUtil();
		SolrDocumentList docList = util.query(query,
				SolrCollection.getCollectionString(SolrCollection.SOCIAL_MEDIA, SolrCollection.OTHERS));

		if (docList != null && !docList.isEmpty()) {
			for (SolrDocument doc : docList) {
				String url = (String) doc.getFieldValue(SolrFieldDefinition.URL.getName());
				outMap.put(url, doc);
			}
		}

		log.info("existed:{} input: {}", outMap.size(), docs.size());
		return outMap;
	}

}
