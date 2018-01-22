/**
 * 
 */
package social.hunt.analysis.sentiment;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
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

import social.hunt.common.exception.SubmitBatchException;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.util.StringUtils;
import com.semantria.Session;
import com.semantria.mapping.Document;
import com.semantria.mapping.output.DocAnalyticData;
import com.semantria.mapping.output.DocEntity;
import com.semantria.mapping.output.DocPhrase;
import com.semantria.mapping.output.DocTheme;
import com.semantria.mapping.output.Subscription;
import com.semantria.mapping.output.TaskStatus;

/**
 * @author Jason
 *
 */
public class SemantriaAnalysisEngineBatch {
	private static final Logger log = LoggerFactory.getLogger(SemantriaAnalysisEngineBatch.class);

	private final static long TIMEOUT_BEFORE_GETTING_RESPONSE = 1300; // in millisec
	private final static BigDecimal BATCH_PROCESS_TIMEOUT = new BigDecimal(1000 * 30);

	private Session session = null;
	private Subscription subscription = null;
	private SemantriaService service = null;
	private SolrInputDocUtil util = null;
	private int inputDocNo = 0;
	private int filteredDocNo = 0;
	private int processedDocNo = 0;
	private int failedDocNo = 0;
	private int taskIndex = 0;

	private final static Integer BATCH_LIMIT = 100;
	
	private final static String SENTIMENT_URL = "http://ch-prepro:9898/getSentimentResult";

	public SemantriaAnalysisEngineBatch(int index) throws Exception {
		service = new SemantriaService();
		util = new SolrInputDocUtil();
		session = service.createSession();
		subscription = service.createSubscription(session);
		this.taskIndex = index;
	}

	public SemantriaAnalysisEngineBatch(boolean useConfigPool) throws Exception {
		service = new SemantriaService(useConfigPool);
		util = new SolrInputDocUtil();
		session = service.createSession();
		subscription = service.createSubscription(session);
	}
	
	public SemantriaAnalysisEngineBatch(){
		util = new SolrInputDocUtil();
//		session = service.createSession();
//		subscription = service.createSubscription(session);
	};

	/**
	 * Main flow
	 */
	public List<DocAnalyticData> runDetailedBatchAnalysis(List<SolrInputDocument> initialBatch) throws SubmitBatchException {
		inputDocNo = initialBatch.size();
		log.info("Input Doc: {}", inputDocNo);

		HashMap<String, List<SolrInputDocument>> batchMap = batchSeparation(initialBatch);
		log.info("Input Doc: {} Filtered Doc: {}", inputDocNo, filteredDocNo);

		List<DocAnalyticData> resultDocs = queueAndprocess(batchMap);
		log.info("Input Doc: {} Filtered Doc: {}", inputDocNo, filteredDocNo);
		log.info("Processed Doc: {}", processedDocNo);

		if ((processedDocNo + failedDocNo) != (inputDocNo - filteredDocNo)) {
			log.warn("Data Loss while processing! Please fix!");
		}

		return resultDocs;
	}
	
	/**
	 * New Main flow
	 */
	public HashMap<String, String> runSentimentAnalysis(List<SolrInputDocument> initialBatch) throws SubmitBatchException {
		inputDocNo = initialBatch.size();
		log.info("Input Doc: {}", inputDocNo);

		HashMap<String, List<SolrInputDocument>> batchMap = batchSeparation(initialBatch);
		log.info("Input Doc: {} Filtered Doc: {}", inputDocNo, filteredDocNo);

//		List<DocAnalyticData> resultDocs = queueAndprocess(batchMap);
//		log.info("Input Doc: {} Filtered Doc: {}", inputDocNo, filteredDocNo);
//		log.info("Processed Doc: {}", processedDocNo);
//
//		if ((processedDocNo + failedDocNo) != (inputDocNo - filteredDocNo)) {
//			log.warn("Data Loss while processing! Please fix!");
//		}
		
		HashMap<String, String> result = new HashMap<String, String>();
		for (Entry<String, List<SolrInputDocument>> entry: batchMap.entrySet()) {
			log.info("Going to process batch with key: " + entry.getKey());
			processDocumentByBatch(entry.getValue(), result);
			log.info("Documents processed: {} Documents to be processed: {}", result.size(), filteredDocNo);
		}
		log.info("Data is processed; returning...");
		return result;
	}
	
	public void processDocumentByBatch(List<SolrInputDocument> batch, HashMap<String, String> result) {
		long start;
		for (SolrInputDocument doc: batch) {
			start = System.currentTimeMillis();
			log.info("Content of post request: " + util.extractTitleContent(doc));
			String response = null;
			response = getHttpPostRequest(util.extractTitleContent(doc));
			while (response == null) {
				log.info(".");
				waitFor(1000);
				if (System.currentTimeMillis() - start > 30000) {
					response = "{\"Api error\":\"response timeout\"}";
					break;
				}
			}
			result.put(util.generateDocId(doc), response);
		}
	}
	
	public List<SolrInputDocument> setFieldSolrDoc(HashMap<String, String> sentimentResults, List<SolrInputDocument> solrDocList) {
		List<SolrInputDocument> updatedList = new ArrayList<SolrInputDocument>();
		
		for (SolrInputDocument sDoc : solrDocList) {
			float minX = -5.0f;
			float maxX = 5.0f;
			Random rand = new Random();
			float finalX = rand.nextFloat() * (maxX - minX) + minX;
			sDoc.setField(SolrFieldDefinition.SENTIMENT_SCORE.getName(), finalX);
		}
		
		for (SolrInputDocument doc: solrDocList) {
			String sentimentResult = null;
			if ((sentimentResult = sentimentResults.get(util.generateDocId(doc))) != null) {
				updatedList.add(doc);
				JsonObject updateData = new Gson().fromJson(sentimentResult, JsonObject.class);
				
				if (updateData != null) {
					log.info("The conetent of json object: " + updateData.toString());
					
					// Sentiment Score
					if (updateData.get("polarity") != null) {
						Float score = Float.valueOf(updateData.get("polarity").toString());
						doc.setField(SolrFieldDefinition.SENTIMENT_SCORE.getName(), score);
						log.info("doc with url: " + doc.getField("url").toString() + "'s new polarity is " + score);
					}
					
					//Keywords
					Gson gson = new Gson();
					
					HashSet<String> keywords = new HashSet<String>();
					HashSet<String> posWords = new HashSet<String>();
					HashSet<String> negWords = new HashSet<String>();
					
					if (updateData.get("keywords") != null) {
						keywords.addAll(gson.fromJson(updateData.get("keywords").toString(), HashSet.class));
						log.info("Keywords found: " + keywords.toString());
					}
					if (updateData.get("pos") != null) {
						posWords.addAll(gson.fromJson(updateData.get("pos").toString(), HashSet.class));
						log.info("Positive words found: " + posWords.toString());
					}
					if (updateData.get("neg") != null) {
						negWords.addAll(gson.fromJson(updateData.get("neg").toString(), HashSet.class));
						log.info("Negative words found: " + negWords.toString());
					}
					if (updateData.get("neg_word") != null) {
						negWords.addAll(gson.fromJson(updateData.get("neg_word").toString(), HashSet.class));
						log.info("Neg_word words found: " + negWords.toString());
					}
					if (updateData.get("pos_word") != null) {
						posWords.addAll(gson.fromJson(updateData.get("pos_word").toString(), HashSet.class));
						log.info("Pos_word words found: " + posWords.toString());
					}
					
					if (keywords != null) {
						if (posWords != null) {
							for (String word: posWords) {
								keywords.remove(word);
							}
						}
						if (negWords != null) {
							for (String word: negWords) {
								keywords.remove(word);
							}
						}
					}
					
					doc.setField(SolrFieldDefinition.NEUTRAL_KEYWORDS.getName(), keywords);
					log.info("Neutral words added: " + keywords.toString());
					doc.setField(SolrFieldDefinition.POS_KEYWORDS.getName(), posWords);
					log.info("Positive words added: " + posWords.toString());
					doc.setField(SolrFieldDefinition.NEG_KEYWORDS.getName(), negWords);
					log.info("Negative words added: " + negWords.toString());
					
					log.info("Updating new sentiment and keywords are completed here.");
				} else {
					log.info("This content is null coming from sentiment");
				}
			}
		}
		
		return updatedList;
	}
	
	private String getHttpPostRequest(String params) {
//		try {
//			URL url = new URL(urlStr);
//			params = "content=" + params;
//			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
//			connection.setRequestMethod("POST");
//			connection.setDoOutput(true);
//			connection.setRequestProperty( "Content-Type", "application/x-www-form-urlencoded"); 
//			connection.setRequestProperty( "charset", "utf-8");
//			connection.setRequestProperty( "Content-Length", Integer.toString(params.length()));
//			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
//			wr.write(params.getBytes("UTF-8"));
//			wr.flush();
//			wr.close();
//			int responseCode = connection.getResponseCode();
//			System.out.println("\nSending 'POST' request to URL : " + url);
//			System.out.println("Post parameters : " + params);
//			System.out.println("Response Code : " + responseCode);
//			if (responseCode == 200) {
//				BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
//				String inputLine;
//				StringBuffer response = new StringBuffer();
//
//				while ((inputLine = in.readLine()) != null) {
//					response.append(inputLine);
//				}
//				in.close();
//
//				return response.toString();
//			} else {
//				return "";
//			}
//		} catch (MalformedURLException e) {
//			log.info("The given url: " + urlStr + " is not structured properly");
//			e.printStackTrace();
//		} catch (IOException e) {
//			log.info("ioException caught while reading/writing http post request");
//			e.printStackTrace();
//		}
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

	/**
	 * Group list of SolrInputDocument into different batch by Language, currently Chinese and English only.
	 * 
	 * @param solrList
	 * @return
	 */
	private HashMap<String, List<SolrInputDocument>> batchSeparation(List<SolrInputDocument> solrList) {
		HashMap<String, List<SolrInputDocument>> batchMap = new HashMap<String, List<SolrInputDocument>>();
		String zhConfigId = service.getChineseConfigId(taskIndex);
		String enConfigId = service.getEnglishConfigId(taskIndex);
		log.info("Zh config Id: {}  English configId: {}", zhConfigId, enConfigId);
		
		
		List<SolrInputDocument> zhBatch = new ArrayList<SolrInputDocument>();
		List<SolrInputDocument> enBatch = new ArrayList<SolrInputDocument>();

		try {
			for (SolrInputDocument solrDoc : solrList) {
				Float sScore = (Float) solrDoc.getFieldValue(SolrFieldDefinition.SENTIMENT_SCORE.getName());
				if (null == sScore || sScore.isNaN()) { // only calculate feed without sScore

					String content = null;
					content = util.extractTitleContent(solrDoc);

					if (StringUtils.isChinese(content)) {
						zhBatch.add(solrDoc);
					} else {
						String[] split = StringUtils.split(content, "\\s+");
						if (split != null && split.length >= 10 && split.length < 1000) // less than 1000 words
							enBatch.add(solrDoc);
					}
				}
			}
		} catch (Exception e) {
			// Log if any exception occurred.
			log.error(e.getMessage(), e);
		}

		batchMap.put(zhConfigId, zhBatch);
		batchMap.put(enConfigId, enBatch);
		filteredDocNo = inputDocNo - (zhBatch.size() + enBatch.size());

		return batchMap;
	}

	private void waitFor(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			log.info("Sleep is being interrupted.");
			Thread.interrupted();
		}
	}

	protected List<DocAnalyticData> queueAndprocess(HashMap<String, List<SolrInputDocument>> batchMap) throws SubmitBatchException {
		List<DocAnalyticData> result = new ArrayList<DocAnalyticData>();
		int processedDocsCount = 0;
		int failedDocsCount = 0;

		for (String configId : batchMap.keySet()) {
			if (!batchMap.get(configId).isEmpty()) {

				HashMap<String, TaskStatus> docsTracker = queueByBatch(batchMap.get(configId), configId);
				long batchStartTime = System.currentTimeMillis();
				// while (docsTracker.containsValue(TaskStatus.QUEUED)) {
				//
				// waitFor(TIMEOUT_BEFORE_GETTING_RESPONSE);
				//
				// // Requests processed results from Semantria service
				// List<DocAnalyticData> processedDocs = session.getProcessedDocuments(configId);
				// log.info("Getting results..........");
				// for (DocAnalyticData item : processedDocs) {
				// if (docsTracker.containsKey(item.getId())) {
				// result.add(item);
				// docsTracker.put(item.getId(), TaskStatus.PROCESSED);
				// }
				// }
				//
				// // Check timeout
				// if ((System.currentTimeMillis() - batchStartTime) > BATCH_PROCESS_TIMEOUT) {
				// if (result.isEmpty()) {
				// throw new SubmitBatchException("Submit batch timeout!");
				// } else {
				// break;
				// }
				// }
				// }
				log.info("Size of document tracker: {}", docsTracker.size());

				long timeout = getTimeout(docsTracker.size());

				while (docsTracker.containsValue(TaskStatus.QUEUED)) {

					waitFor(TIMEOUT_BEFORE_GETTING_RESPONSE);

					// Requests processed results from Semantria service
					List<DocAnalyticData> processedDocs = session.getProcessedDocuments(configId);
					log.info("Return API size: {}", processedDocs.size());

					// Look for docs with FAILED status
					if ((processedDocs != null) && !processedDocs.isEmpty()) {
						for (DocAnalyticData item : processedDocs) {
							if (item.getStatus().equals(TaskStatus.PROCESSED) && docsTracker.containsKey(item.getId())) {
								result.add(item);
								docsTracker.put(item.getId(), TaskStatus.PROCESSED);
								processedDocsCount++;
							} else if (item.getStatus().equals(TaskStatus.FAILED) && docsTracker.containsKey(item.getId())) {
								failedDocsCount++;
								log.warn("Document failure summary: {} Content: {}", item.getSummary(), item.getSourceText());
								docsTracker.put(item.getId(), TaskStatus.FAILED);
								result.add(item);
							}
						}
					}

					// Check timeout
					if ((System.currentTimeMillis() - batchStartTime) > 20000) {
						break;
					}
				}
			}
		}
		log.info("failedDocCoount: {}", failedDocsCount);
		log.info("processedDocCoount: {}", processedDocsCount);
		failedDocNo = failedDocsCount;
		processedDocNo = processedDocsCount;
		log.info("Size of result: {}", result.size());
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

	private HashMap<String, TaskStatus> queueByBatch(List<SolrInputDocument> docBatch, String configId) throws SubmitBatchException {
		List<Document> outgoingBatch = new ArrayList<Document>(BATCH_LIMIT);
		HashMap<String, TaskStatus> docsTracker = new HashMap<String, TaskStatus>();
		Integer status = null;

		for (SolrInputDocument solrDoc : docBatch) {
			String content = util.extractTitleContent(solrDoc);

			// Skip document if title+content is blank
			if (StringUtils.isBlank(content))
				continue;

			String docId = util.generateDocId(solrDoc);

			// Make sure input content is < 8192 characters
			Document apiDoc = new Document(docId, StringUtils.left(content, 1000));
			outgoingBatch.add(apiDoc);

			if (outgoingBatch.size() > BATCH_LIMIT) {
				// We used up all the retries, throw exception with the last returned HTTP status

				throw new SubmitBatchException(new StringBuilder("Failed submitting batch to queue with returned HTTP status code: ")
						.append(status).toString());

			} else if (outgoingBatch.size() >= (BATCH_LIMIT - 3)) {
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

	/**
	 * Retrieve and set the corresponding Sentiment Score in the SolrDoc
	 * 
	 * @param resultDocs
	 *            Result from Semantria API call
	 * @param solrDocList
	 *            Solr document list
	 * @return
	 */
	public List<SolrInputDocument> setFieldSolrDoc(List<DocAnalyticData> resultDocs, List<SolrInputDocument> solrDocList) {
		List<SolrInputDocument> updatedList = new ArrayList<SolrInputDocument>();
		
		for (SolrInputDocument sDoc : solrDocList) {
			float minX = -5.0f;
			float maxX = 5.0f;
			Random rand = new Random();
			float finalX = rand.nextFloat() * (maxX - minX) + minX;
			sDoc.setField(SolrFieldDefinition.SENTIMENT_SCORE.getName(), finalX);
		}

		for (DocAnalyticData resultDoc : resultDocs) {
			for (SolrInputDocument sDoc : solrDocList) {
				String docId = util.generateDocId(sDoc);
				
				if (resultDoc.getId().contains(docId)) {
					updatedList.add(sDoc);
		
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

		return updatedList;
	}

	private long getTimeout(int recordNum) {
		int i = Math.max(new BigDecimal(recordNum).divide(new BigDecimal(20), RoundingMode.UP).intValue(), 1);
		return BATCH_PROCESS_TIMEOUT.multiply(new BigDecimal(i)).longValue();
	}

}
