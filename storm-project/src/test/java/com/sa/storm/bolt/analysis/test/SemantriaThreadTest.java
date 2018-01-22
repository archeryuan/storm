package com.sa.storm.bolt.analysis.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import social.hunt.analysis.sentiment.SemantriaAnalysisEngine;
import social.hunt.common.exception.SubmitBatchException;
import social.hunt.solr.definition.SolrCollection;
import social.hunt.solr.util.SolrQueryUtil;

import com.sa.common.definition.SolrFieldDefinition;
import com.semantria.mapping.output.DocAnalyticData;
import com.semantria.mapping.output.DocCategory;
import com.semantria.mapping.output.DocEntity;
import com.semantria.mapping.output.DocTheme;

/**
 * @author Jason
 *
 */
public class SemantriaThreadTest {

	static List<DocAnalyticData> result = new ArrayList<DocAnalyticData>();
	static List<SolrInputDocument> inputDocs1 = new ArrayList<SolrInputDocument>();
	static List<SolrInputDocument> inputDocs2 = new ArrayList<SolrInputDocument>();

	public static void main(String[] args) throws IOException, SolrServerException {
		final CountDownLatch startEngineLatch = new CountDownLatch(1);
		final CountDownLatch finishLatch = new CountDownLatch(2);
		final SemantriaAnalysisEngine engine = new SemantriaAnalysisEngine();
		inputDocs1 = load(setQuery("+\"電話\"", 50));
		inputDocs2 = load(setQuery("+\"港股\"", 50));

		// Make SolrQuery
		Thread t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					List<DocAnalyticData> temp = new ArrayList<DocAnalyticData>();
					// List<SolrInputDocument> inputDocs = load(setQuery("+\"電話\"", 10));
					startEngineLatch.await();
					temp = engine.runDetailedBatchAnalysis(inputDocs1);
					result.addAll(temp);
					finishLatch.countDown();
				} catch (InterruptedException | SubmitBatchException e) {
					e.printStackTrace();
				}
			}
		});

		Thread t2 = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					List<DocAnalyticData> temp = new ArrayList<DocAnalyticData>();
					// List<SolrInputDocument> inputDocs = load(setQuery("+\"港股\"", 10));
					startEngineLatch.await();
					Thread.sleep(2000);
					temp = engine.runDetailedBatchAnalysis(inputDocs2);
					result.addAll(temp);
					finishLatch.countDown();
				} catch (InterruptedException | SubmitBatchException e) {
					e.printStackTrace();
				}
			}
		});

		// Main threads start
		try {
			System.out.println("Preparing threads...");
			t1.start();
			t2.start();
			// Thread.sleep(3000);
			// Start the threads
			long startTime = System.currentTimeMillis();
			startEngineLatch.countDown();
			finishLatch.await();
			long time = new Date().getTime() - startTime;
			showDetailedResult(result);
			System.out.println("The time to queue and process the result is: " + time);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void showDetailedResult(List<DocAnalyticData> detailedResult) {
		for (DocAnalyticData doc : detailedResult) {
			System.out.println("Document:\n\tid: " + doc.getId() + "\n\tsentiment score: " + Float.toString(doc.getSentimentScore())
					+ "\n\tsentiment polarity: " + doc.getSentimentPolarity() + "\n\tdetected language: " + doc.getLanguage());
			System.out.println();
			if (doc.getAutoCategories() != null) {
				System.out.println("\tdocument categories:");
				for (DocCategory category : doc.getAutoCategories()) {
					System.out.println("\t\ttopic: " + category.getTitle() + " \n\t\tStrength score: "
							+ Float.toString(category.getStrengthScore()));
					System.out.println();
				}
			}
			if (doc.getThemes() != null) {
				System.out.println("\tdocument themes:");
				for (DocTheme theme : doc.getThemes()) {
					System.out.println("\t\ttitle: " + theme.getTitle() + " \n\t\tsentiment: " + Float.toString(theme.getSentimentScore())
							+ "\n\t\tsentiment polarity: " + theme.getSentimentPolarity());
					System.out.println();
				}
			}
			if (doc.getEntities() != null) {
				System.out.println("\tentities:");
				for (DocEntity entity : doc.getEntities()) {
					System.out.println("\t\ttitle: " + entity.getTitle() + "\n\t\tsentiment: " + Float.toString(entity.getSentimentScore())
							+ "\n\t\tsentiment polarity: " + entity.getSentimentPolarity() + "\n\t\tcount: " + entity.getEvidence());
					System.out.println();
				}
			}
		}
		System.out.println("The number of processed documents are: " + result.size());
	}

	protected static SolrQuery setQuery(String keywords, int rows) {
		SolrQuery query = new SolrQuery();
		query.addField(SolrFieldDefinition.CREATE_DATE.getName());
		query.addField(SolrFieldDefinition.PUBLISH_DATE.getName());
		query.addField(SolrFieldDefinition.CREATE_DATE_PAGE.getName());
		query.addField(SolrFieldDefinition.PUBLISH_DATE_PAGE.getName());
		query.addField(SolrFieldDefinition.CONTENT.getName());
		query.addField(SolrFieldDefinition.SENTIMENT_SCORE.getName());
		query.addField(SolrFieldDefinition.KEYWORDS.getName());
		query.addField(SolrFieldDefinition.CONTENT.getName());
		query.addField(SolrFieldDefinition.TITLE.getName());
		query.addField(SolrFieldDefinition.NEW_SOURCE_TYPE.getName());
		query.addField(SolrFieldDefinition.URL.getName());

		// query.setQuery(new StringBuilder("+").append(SolrFieldDefinition.URL.getName()).append(":(").append(StringUtils.join(urls, " "))
		// .append(")").toString());

		query.setQuery(keywords);
		query.setRows(rows);

		return query;
	}

	// protected static List<SolrInputDocument> load(String... urls) throws SolrServerException, IOException {
	protected static List<SolrInputDocument> load(SolrQuery query) throws SolrServerException, IOException {
		SolrQueryUtil util = new SolrQueryUtil();
		SolrDocumentList docList = util
				.query(query, SolrCollection.getCollectionString(SolrCollection.SOCIAL_MEDIA, SolrCollection.OTHERS));

		List<SolrInputDocument> outList = new ArrayList<SolrInputDocument>();
		for (SolrDocument doc : docList) {
			outList.add(ClientUtils.toSolrInputDocument(doc));
		}
		return outList;
	}

}