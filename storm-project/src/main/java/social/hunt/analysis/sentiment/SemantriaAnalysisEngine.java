package social.hunt.analysis.sentiment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.common.exception.SubmitBatchException;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.sa.common.definition.SolrFieldDefinition;
import com.semantria.CallbackHandler;
import com.semantria.Session;
import com.semantria.mapping.Document;
import com.semantria.mapping.configuration.Configuration;
import com.semantria.mapping.output.DocAnalyticData;
import com.semantria.mapping.output.DocEntity;
import com.semantria.mapping.output.DocPhrase;
import com.semantria.mapping.output.DocTheme;
import com.semantria.mapping.output.Subscription;
import com.semantria.mapping.output.TaskStatus;
import com.semantria.serializer.JsonSerializer;

/**
 * @author Jason
 *
 */
@Deprecated
public class SemantriaAnalysisEngine {

	private final static String KEY = "92eb3604-279b-4d2b-ae16-fc32fa39dca5";
	private final static String SECRET = "daffea1e-b55b-4488-a94b-7cb5fb6ce6e8";
	private final static long TIMEOUT_BEFORE_GETTING_RESPONSE = 150; // in millisec

	/**
	 * Maximum processing timeout for a batch in ms
	 */
	private final static long BATCH_PROCESS_TIMEOUT = 1000 * 150;
	private final static long SINGLE_PROCESS_TIMEOUT = 1000 * 8;
	private static final Logger log = LoggerFactory.getLogger(SemantriaAnalysisEngine.class);
	private List<String> jsonProfiles = new ArrayList<String>();

	private Session session = null;
	private static Subscription subscription = null;

	public SemantriaAnalysisEngine() throws IOException {
		this.session = Session.createSession(KEY, SECRET, new JsonSerializer());
		session.setCallbackHandler(new CallbackHandler());
		subscription = session.getSubscription();

		String jsonEn = IOUtils.toString(SemantriaAnalysisEngine.class.getResourceAsStream("/profiles/en")); // IOException
		String jsonCn = IOUtils.toString(SemantriaAnalysisEngine.class.getResourceAsStream("/profiles/zh-cn"));
		String jsonTw = IOUtils.toString(SemantriaAnalysisEngine.class.getResourceAsStream("/profiles/zh-tw"));
		jsonProfiles.add(jsonEn);
		jsonProfiles.add(jsonCn);
		jsonProfiles.add(jsonTw);
	}

	private String getSessionConfigId(String lang, boolean isOneSentence) {
		// 4 configurations: Chinese, English
		String configId = null;
		String configName = null;

		configName = lang;
		// if (isOneSentence) {
		// configName = lang + "Tweet";
		// } else {
		// configName = lang;
		// }

		for (Configuration config : session.getConfigurations()) {

			if ("Chinese".equalsIgnoreCase(lang)) {
				config.setCharsThreshold(0);
			}

			if (config.getName().equals(configName)) {
				configId = config.getId();
				// log.info("Semantria Configurations: {}" , config.getName());
			}
		}
		return configId;
	}

	/**
	 * Extract and concat. title & content from SolrInputDocument
	 * 
	 * @param doc
	 * @return
	 */
	private String extractTitleContent(SolrInputDocument doc) {
		StringBuilder sb = new StringBuilder();

		if (doc.containsKey(SolrFieldDefinition.TITLE.getName())) {
			sb.append(StringUtils.trimToEmpty((String) doc.getFieldValue(SolrFieldDefinition.TITLE.getName())));
			if (sb.length() > 0)
				sb.append(System.lineSeparator()).append(System.lineSeparator());
		}

		if (doc.containsKey(SolrFieldDefinition.CONTENT.getName())) {
			Object obj = doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
			if (obj != null) {
				if (obj instanceof Collection) {
					@SuppressWarnings("unchecked")
					Collection<String> coll = (Collection<String>) obj;
					for (String content : coll) {
						sb.append(StringUtils.trimToEmpty(content)).append(System.lineSeparator());
					}
				} else if (obj instanceof String) {
					if (StringUtils.isBlank((String) obj)) {
						log.info("Title:{}", sb.toString());
						// Feed do not contain content field
						return null;
					} else {
						sb.append(StringUtils.trimToEmpty((String) obj));
					}
				}
			} else {
				// Feed do not contain content field
				log.info("Title:{}", sb.toString());
				return null;
			}
		} else {
			// Feed do not contain content field
			log.info("Title:{}", sb.toString());
			return null;
		}

		return StringUtils.trimToEmpty(sb.toString());
	}

	/**
	 * Group list of SolrInputDocument into different batch by Language, currently Chinese and English only.
	 * 
	 * @param solrList
	 * @return
	 */
	private HashMap<String, List<SolrInputDocument>> batchSeparation(List<SolrInputDocument> solrList) {
		HashMap<String, List<SolrInputDocument>> batchMap = new HashMap<String, List<SolrInputDocument>>();
		String c1 = getSessionConfigId("Chinese", false);
		// String c2 = getSessionConfigId("Chinese", true);
		String c3 = getSessionConfigId("English", false);
		// String c4 = getSessionConfigId("English", true);

		List<SolrInputDocument> l1 = new ArrayList<SolrInputDocument>();
		// List<SolrInputDocument> l2 = new ArrayList<SolrInputDocument>();
		List<SolrInputDocument> l3 = new ArrayList<SolrInputDocument>();
		// List<SolrInputDocument> l4 = new ArrayList<SolrInputDocument>();

		try {
			for (SolrInputDocument solrDoc : solrList) {
				Float sScore = (Float) solrDoc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
				if (null == sScore || sScore.isNaN()) { // only calculate feed without sScore

					String content = null;
					int sourceType = (int) solrDoc.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());

					content = extractTitleContent(solrDoc);

					// if (SourceType.OTHERS.getSourceTypeId() == sourceType) {
					// StringBuilder sb = new StringBuilder();
					//
					// if (solrDoc.containsKey(SolrFieldDefinition.TITLE.getName())) {
					// sb.append(StringUtils.defaultString((String) solrDoc.getFieldValue(SolrFieldDefinition.TITLE.getName())).trim());
					//
					// // Add two line breaks if title is not blank
					// if (sb.length() > 0) {
					// sb.append(System.lineSeparator()).append(System.lineSeparator());
					// }
					// }
					//
					// if (solrDoc.containsKey(SolrFieldDefinition.CONTENT.getName())) {
					// Collection contents = solrDoc.getFieldValues(SolrFieldDefinition.CONTENT.getName());
					// if (null != contents && contents instanceof Collection) {
					// for (Iterator<String> valueIter = contents.iterator(); valueIter.hasNext();) {
					// String str = valueIter.next();
					// if (!StringUtils.isBlank(str)) {
					// sb.append(str).append(System.lineSeparator());
					// }
					// }
					// }
					// }

					// content = extractTitleContent(solrDoc);
					// }
					// if (SourceType.SNS.getSourceTypeId() == sourceType) {
					// StringBuilder sb = new StringBuilder();
					//
					// if (solrDoc.containsKey(SolrFieldDefinition.TITLE.getName())) {
					// sb.append(StringUtils.defaultString((String) solrDoc.getFieldValue(SolrFieldDefinition.TITLE.getName())).trim());
					//
					// // Add two line breaks if title is not blank
					// if (sb.length() > 0) {
					// sb.append(System.lineSeparator()).append(System.lineSeparator());
					// }
					// }
					//
					// if (solrDoc.containsKey(SolrFieldDefinition.CONTENT.getName())) {
					// Object value = solrDoc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
					// if (null != value && value instanceof String) {
					// sb.append(value);
					// }
					// }

					// content = extractTitleContent(solrDoc);
					// }

					// if (!StringUtils.isBlank(content)) {
					String lang = checkTextLanguage(content);
					if (lang.equals("Chinese")) {
						l1.add(solrDoc);
					}
					// if (lang.equals("Chinese") && isOneSentence(solrDoc)) {
					// l2.add(solrDoc);
					// }
					if (lang.equals("English")) {
						l3.add(solrDoc);
					}
					// if (lang.equals("English") && isOneSentence(solrDoc)) {
					// l4.add(solrDoc);
					// }
				}
			}
			// }
		} catch (Exception e) {
			// Log if any exception occurred.
			log.error(e.getMessage(), e);
		}

		batchMap.put(c1, l1);
		// batchMap.put(c2, l2);
		batchMap.put(c3, l3);
		// batchMap.put(c4, l4);

		return batchMap;
	}

	// public boolean isOneSentence(SolrInputDocument doc) {
	// int sourceType = (int) doc.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());
	// if (SourceType.SNS.getSourceTypeId() == sourceType) {
	// return true;
	// }
	// return false;
	// }

	/**
	 * Language detection: Use Language Detection API by Nakatani Shuyo
	 * 
	 * @param content
	 * @return
	 */
	public String checkTextLanguage(String content) {
		String lang = "Chinese"; // default
		try {
			DetectorFactory.loadProfile(jsonProfiles);
			Detector detector = DetectorFactory.create();
			detector.append(content);
			lang = detector.detect();
			log.info(lang);
			if (lang.equals("en") && lang != null) {

				// Check for char threshold for english
				if (getCharThreshold(content) < 0.8)
					return "";

				lang = "English";
			} else if (lang.equals("zh-cn") || lang.equals("zh-tw") && lang != null) {
				lang = "Chinese";
			}
		} catch (NullPointerException ne) {
			log.error("Error loading language detection library!");
			log.error(ne.getMessage(), ne);
			throw ne;
		} catch (LangDetectException e) {
			log.info("Cannot detect the language. Default (Chinese) used");
		} finally {
			DetectorFactory.clear(); // clear the loaded language profiles
		}

		return lang;
	}

	private float getCharThreshold(String inputStr) {
		if (StringUtils.isBlank(inputStr))
			return 0;

		String tmpStr = inputStr;
		String pattern = "[a-zA-Z0-9]*";
		String newString = tmpStr.replaceAll(pattern, "");

		return (inputStr.length() - newString.length()) / inputStr.length();
	}

	public List<DocAnalyticData> runSingleDetailedAnalysis(List<SolrInputDocument> initialDocs) throws SubmitBatchException {
		String zhConfig = getSessionConfigId("Chinese", false);
		String enConfig = getSessionConfigId("English", false);
		List<SolrInputDocument> filteredSolrDocs = new ArrayList<SolrInputDocument>();

		// Filter documents
		int docfilteredByContent = 0;
		// int docfilteredByCharThreshold = 0;
		for (SolrInputDocument solrDoc : initialDocs) {
			String text = extractTitleContent(solrDoc);
			if (!(null == text)) {
				// if (getCharThreshold(text) >= 0) {
				filteredSolrDocs.add(solrDoc);
				// } else {
				// docfilteredByCharThreshold++;
				// }
			} else
				docfilteredByContent++;
		}
		log.info("Docs without both title and content: {}", docfilteredByContent);
		// log.info("Doc can't meet char threshold: {}", docfilteredByCharThreshold);

		HashMap<String, List<SolrInputDocument>> langBatch = batchSeparation(filteredSolrDocs);
		List<DocAnalyticData> resultList = new ArrayList<DocAnalyticData>();

		// Chinese
		List<SolrInputDocument> zhBatch = langBatch.get(zhConfig);
		List<SolrInputDocument> enBatch = langBatch.get(enConfig);
		if (zhBatch == null) {
			zhBatch = ListUtils.EMPTY_LIST;
		}
		if (enBatch == null) {
			enBatch = ListUtils.EMPTY_LIST;
		}

		log.info("Total no. of documents ready to queue:{}", zhBatch.size() + enBatch.size());

		resultList = queueAndProcessSingleDoc(zhBatch, zhConfig, resultList);
		resultList = queueAndProcessSingleDoc(enBatch, enConfig, resultList);

		return resultList;
	}

	public List<DocAnalyticData> queueAndProcessSingleDoc(List<SolrInputDocument> batch, String configId, List<DocAnalyticData> resultList)
			throws SubmitBatchException {
		for (SolrInputDocument solrDoc : batch) {
			String docId = generateId(solrDoc);
			String content = extractTitleContent(solrDoc);

			// Skip document if title+content is blank

			// if (getCharThreshold(content) < 0.5)
			// continue;

			Document doc = new Document(docId, content);
			submitSingleDoc(doc, configId);

			long queueStartTime = System.currentTimeMillis();
			DocAnalyticData result = null;
			do {
				long l = System.currentTimeMillis() - queueStartTime;
				if (l < 1000) {
					waitFor(1000 - l);
				} else {
					waitFor(TIMEOUT_BEFORE_GETTING_RESPONSE);
				}

				result = session.getDocument(docId, configId);

				if (result != null) {
					if (TaskStatus.PROCESSED.equals(result.getStatus())) {
						resultList.add(result);
						break;
					} else if (TaskStatus.FAILED.equals(result.getStatus())) {
						log.error("Failed to process document by API for content: {}", content);
						break;
					}
				}

				if ((System.currentTimeMillis() - queueStartTime) > SINGLE_PROCESS_TIMEOUT) {
					if (result != null) {
						log.error("Process timeout with status: {}, content: {}", result.getStatus(), content);
					} else {
						log.error("Process timeout with content: {}", content);
					}
					break;
				}
			} while (null == result);
		}
		return resultList;
	}

	public List<DocAnalyticData> runDetailedBatchAnalysis(List<SolrInputDocument> initialBatch) throws SubmitBatchException {
		// Need to separate in batches because different configs are needed
		HashMap<String, List<SolrInputDocument>> batchMap = batchSeparation(initialBatch);
		List<DocAnalyticData> resultDocs = queueAndprocess(batchMap);
		return resultDocs;
	}

	private void waitFor(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			log.info("Sleep is being interrupted.");
			Thread.interrupted();
		}
	}

	public List<DocAnalyticData> queueAndprocess(HashMap<String, List<SolrInputDocument>> batchMap) throws SubmitBatchException {
		List<DocAnalyticData> result = new ArrayList<DocAnalyticData>();

		for (String configId : batchMap.keySet()) {
			if (!batchMap.get(configId).isEmpty()) {

				// Queue the batch
				HashMap<String, TaskStatus> docsTracker = queueByBatch(batchMap.get(configId), configId);
				long batchStartTime = System.currentTimeMillis();

				while (docsTracker.containsValue(TaskStatus.QUEUED)) {

					waitFor(TIMEOUT_BEFORE_GETTING_RESPONSE);

					// Requests processed results from Semantria service
					List<DocAnalyticData> processedDocs = session.getProcessedDocuments(configId);

					for (DocAnalyticData item : processedDocs) {
						if (docsTracker.containsKey(item.getId())) {
							result.add(item);
							docsTracker.put(item.getId(), TaskStatus.PROCESSED);
						}
					}

					// Check timeout
					if ((System.currentTimeMillis() - batchStartTime) > BATCH_PROCESS_TIMEOUT) {
						if (result.isEmpty()) {
							throw new SubmitBatchException("Submit batch timeout!");
						} else {
							break;
						}
					}
				}
			}
		}

		return result;
	}

	/**
	 * Submit documents to Semantria process queue, check out <a
	 * href="https://semantria.com/support/developer/references/glossary/#status">HTTP statuses</a> for possible status response.
	 * 
	 * @param docs
	 * @param configId
	 * @param docsTracker
	 * @return
	 */
	private Integer submitSingleBatch(List<Document> docs, String configId, HashMap<String, TaskStatus> docsTracker) {
		Integer val = session.queueBatch(docs, configId);
		switch (val) {
		case 202:
			log.info("{} documents queued successfully.", docs.size());
			for (Document doc : docs) {
				docsTracker.put(doc.getId(), TaskStatus.QUEUED);
			}
			docs.clear();
			break;
		case 402:
			log.error("Request is unauthorized. License is expired or the calls limit has been reached.");
			break;
		case 406:
			log.error("Batch/ collection limit or a configuration limit has been reached. Server responds with details.");
			break;
		default:
			log.error("queueBatch returned an unexpected value: {}", val);
		}
		return val;
	}

	private Integer submitSingleDoc(Document doc, String configId) {
		Integer val = session.queueDocument(doc, configId);
		switch (val) {
		case 202:
			log.info("1 document queued successfully.");
			break;
		case 402:
			log.error("Request is unauthorized. License is expired or the calls limit has been reached.");
			break;
		case 406:
			log.error("Batch/ collection limit or a configuration limit has been reached. Server responds with details.");
			break;
		default:
			log.error("queueBatch returned an unexpected value: {}", val);
		}
		return val;
	}

	private HashMap<String, TaskStatus> queueByBatch(List<SolrInputDocument> docBatch, String configId) throws SubmitBatchException {
		List<Document> outgoingBatch = new ArrayList<Document>(subscription.getBasicSettings().getBatchLimit());
		HashMap<String, TaskStatus> docsTracker = new HashMap<String, TaskStatus>();
		Integer status = null;

		for (SolrInputDocument solrDoc : docBatch) {
			String content = extractTitleContent(solrDoc);

			// Skip document if title+content is blank
			if (StringUtils.isBlank(content))
				continue;

			String jobId = generateId(solrDoc);
			Document contentDoc = new Document(jobId, content);
			outgoingBatch.add(contentDoc);

			if (outgoingBatch.size() > subscription.getBasicSettings().getBatchLimit()) {
				// We used up all the retries, throw exception with the last returned HTTP status

				throw new SubmitBatchException(new StringBuilder("Failed submitting batch to queue with returned HTTP status code: ")
						.append(status).toString());

			} else if (outgoingBatch.size() >= (subscription.getBasicSettings().getBatchLimit() - 3)) {
				// Purposely minus the max. batch limit by 3, allow us to have few rounds of retry.

				status = submitSingleBatch(outgoingBatch, configId, docsTracker);
			}
		}

		if (outgoingBatch.size() > 0) {
			submitSingleBatch(outgoingBatch, configId, docsTracker);
		}

		if (docsTracker.isEmpty() && docBatch.isEmpty()) {
			log.error("The tracker is empty");
		}
		return docsTracker;
	}

	public Long getTransctions() {
		if (subscription.getBillingSettings() != null && subscription.getBillingSettings().getDocsBalance() != null)
			return subscription.getBillingSettings().getDocsBalance();
		else
			return null;
	}

	/**
	 * Generate unique ID for Semantria call with document URL
	 * 
	 * @param sDoc
	 * @return
	 */
	private String generateId(SolrInputDocument sDoc) {
		String url = (String) sDoc.getFieldValue(SolrFieldDefinition.URL.getName());

		// The docId can only contains 32 chars
		return DigestUtils.md5Hex(url);
	}

	/**
	 * Retrieve and set the corresponding Sentiment Score in the SolrDoc
	 * 
	 * @param resultDocs
	 *            Result from Semantria API call
	 * @param solrDocList
	 *            Solr document list
	 */
	public void setFieldSolrDoc(List<DocAnalyticData> resultDocs, List<SolrInputDocument> solrDocList) {
		for (DocAnalyticData resultDoc : resultDocs) {
			for (SolrInputDocument sDoc : solrDocList) {
				String jobId = generateId(sDoc);

				if (resultDoc.getId().contains(jobId)) {
					// Sentiment Score
					Float score = resultDoc.getSentimentScore();
					sDoc.setField(SolrFieldDefinition.SENTIMENT_SCORE.getName(), score);

					// Keywords
					List<DocPhrase> keywords = resultDoc.getPhrases();
					Set<String> posKeywordsStr = new HashSet<String>();
					Set<String> negKeywordsStr = new HashSet<String>();
					Set<String> neuKeywordsStr = new HashSet<String>();

					if (null != keywords) {
						for (DocPhrase phrase : keywords) {
							if (phrase.getSentimentPolarity().equals("positive")) {
								posKeywordsStr.add(phrase.getTitle());
							}
							if (phrase.getSentimentPolarity().equals("negative")) {
								negKeywordsStr.add(phrase.getTitle());
							}
							if (phrase.getSentimentPolarity().equals("neutral")) {
								neuKeywordsStr.add(phrase.getTitle());
							}
						}
						sDoc.setField(SolrFieldDefinition.POS_KEYWORDS.getName(), posKeywordsStr);
						sDoc.setField(SolrFieldDefinition.NEG_KEYWORDS.getName(), negKeywordsStr);
						sDoc.setField(SolrFieldDefinition.NEUTRAL_KEYWORDS.getName(), neuKeywordsStr);
					}
					// Entities
					List<DocEntity> entities = resultDoc.getEntities();
					if (null != entities) {
						Set<String> personsStr = new HashSet<String>();
						Set<String> companiesStr = new HashSet<String>();
						Set<String> placesStr = new HashSet<String>();
						Set<String> patternsStr = new HashSet<String>();
						Set<String> productsStr = new HashSet<String>();
						Set<String> jobTitlesStr = new HashSet<String>();

						for (DocEntity entity : entities) {
							if (entity.getEntityType().equals("Person")) {
								personsStr.add(entity.getTitle());
							}
							sDoc.setField(SolrFieldDefinition.PERSONS.getName(), personsStr);
							if (entity.getEntityType().equals("Company")) {
								companiesStr.add(entity.getTitle());
							}
							sDoc.setField(SolrFieldDefinition.COMPANIES.getName(), companiesStr);
							if (entity.getEntityType().equals("Place")) {
								placesStr.add(entity.getTitle());
							}
							sDoc.setField(SolrFieldDefinition.PLACES.getName(), placesStr);
							if (entity.getEntityType().equals("Pattern")) {
								patternsStr.add(entity.getTitle());
							}
							sDoc.setField(SolrFieldDefinition.PATTERNS.getName(), patternsStr);
							if (entity.getEntityType().equals("Product")) {
								productsStr.add(entity.getTitle());
							}
							sDoc.setField(SolrFieldDefinition.PRODUCTS.getName(), productsStr);
							if (entity.getEntityType().equals("Job Title")) {
								jobTitlesStr.add(entity.getTitle());
							}
							sDoc.setField(SolrFieldDefinition.JOB_TITLES.getName(), jobTitlesStr);
						}
					}
					// Themes
					List<DocTheme> themes = resultDoc.getThemes();
					if (null != themes) { // Check if this line is needed
						List<String> posThemesStr = new ArrayList<String>();
						List<String> negThemesStr = new ArrayList<String>();
						List<String> neuThemesStr = new ArrayList<String>();
						for (DocTheme theme : themes) {
							if (theme.getSentimentPolarity().equals("positive")) {
								posThemesStr.add(theme.getTitle());
							}
							if (theme.getSentimentPolarity().equals("negative")) {
								negThemesStr.add(theme.getTitle());
							}
							if (theme.getSentimentPolarity().equals("neutral")) {
								neuThemesStr.add(theme.getTitle());
							}
						}
						sDoc.setField(SolrFieldDefinition.POS_THEMES.getName(), posThemesStr);
						sDoc.setField(SolrFieldDefinition.NEG_THEMES.getName(), negThemesStr);
						sDoc.setField(SolrFieldDefinition.NEUTRAL_THEMES.getName(), neuThemesStr);
					}
					break;
				}
			}
		}
	}
}
