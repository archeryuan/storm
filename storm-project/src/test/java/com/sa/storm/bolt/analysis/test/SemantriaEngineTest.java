/**
 * 
 */
package com.sa.storm.bolt.analysis.test;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.analysis.sentiment.SemantriaAnalysisEngineBatch;
import social.hunt.common.exception.SubmitBatchException;
import social.hunt.solr.definition.SolrCollection;
import social.hunt.solr.util.SolrQueryUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.json.JsonUtil;
import com.semantria.mapping.output.DocAnalyticData;
import com.semantria.mapping.output.DocEntity;
import com.semantria.mapping.output.DocTheme;

/**
 * @author Jason
 *
 */
public class SemantriaEngineTest {
	private static final Logger log = LoggerFactory.getLogger(SemantriaEngineTest.class);

	public static void main(String[] args) throws Exception {
		// SemantriaAnalysisEngine engine = new SemantriaAnalysisEngine();
		SemantriaAnalysisEngineBatch batchEngine = new SemantriaAnalysisEngineBatch(false);

		List<DocAnalyticData> result = new ArrayList<DocAnalyticData>();
		// Make SolrQuery
		List<SolrInputDocument> inputDocs = new ArrayList<SolrInputDocument>();
		//
		try {
			// inputDocs = parseURLjSon();
			inputDocs = load();

			for (SolrInputDocument doc : inputDocs) {
				doc.setField(SolrFieldDefinition.SENTIMENT_SCORE.getName(), null);
				// doc.setField(
				// SolrFieldDefinition.CONTENT.getName(),
				// "Bom dia 🙌🙌🙌 #bodrum @topdestinospormarinamantega #topdestinospormarinamantega #lokapravoltar #valeapena #paradise #paraiso #recomendo #swissairlines #flyswiss #withmylove #love #ferias #vacation #fun #sun #letsgo #Turquia");
			}

		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		// System.out.println("Number of input documents: " + inputDocs.size());
		long startTime = System.currentTimeMillis();
		result = batchEngine.runDetailedBatchAnalysis(inputDocs);
		System.out.println("The time to queue and process the result is: " + Long.toString((new Date()).getTime() - startTime));

		showDetailedResult(result);
	}

	private static void showDetailedResult(List<DocAnalyticData> detailedResult) {
		for (DocAnalyticData doc : detailedResult) {
			System.out.println("Document:\n\tid: " + doc.getId() + "\n\tsentiment score: " + Float.toString(doc.getSentimentScore())
					+ "\n\tsentiment polarity: " + doc.getSentimentPolarity());
			System.out.println();
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
							+ "\n\t\tsentiment polarity: " + entity.getSentimentPolarity());
					System.out.println();
				}
			}
		}
	}

	protected static List<SolrInputDocument> load(String... urls) throws SolrServerException, IOException {
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

		query.setQuery("*:*");
		query.setSort("cDate", SolrQuery.ORDER.asc);
		query.setRows(10);

		SolrQueryUtil util = new SolrQueryUtil();
		SolrDocumentList docList = util
				.query(query, SolrCollection.getCollectionString(SolrCollection.SOCIAL_MEDIA, SolrCollection.OTHERS));

		List<SolrInputDocument> outList = new ArrayList<SolrInputDocument>();
		for (SolrDocument doc : docList) {
			outList.add(ClientUtils.toSolrInputDocument(doc));
		}
		return outList;
	}

	protected static List<SolrInputDocument> parseURLjSon() {
		List<SolrInputDocument> list = new ArrayList<SolrInputDocument>();
		try {
			String temp = "http://175.41.196.229:8574/solr/social-media_shard1_replica2/select?q=%2BcDate%3A%5BNOW-1HOUR+TO+NOW%5D&rows=10&wt=json&indent=true&collection=others";
			URL url = new URL(temp);
			JsonUtil util = new JsonUtil();
			JsonNode node = util.getMapper().readTree(url);
			List<JsonNode> titleNodes = node.findValues("title");
			List<JsonNode> contentNodes = node.findValues("content");
			List<JsonNode> urlNodes = node.findValues("url");
			// List<JsonNode> scoreNodes = node.findValues("sScore");
			System.out.println(node.toString());
			log.info("title: {} content: {}", titleNodes.size(), contentNodes.size());
			log.info("url: {}", urlNodes.size());
			if (titleNodes.size() == contentNodes.size()) {
				for (int i = 0; i < titleNodes.size(); i++) {
					String titleString = titleNodes.get(i).toString();
					String contentString = contentNodes.get(i).toString();
					String urlString = urlNodes.get(i).toString();
					SolrInputDocument solrDocument = new SolrInputDocument();
					solrDocument.setField(SolrFieldDefinition.TITLE.getName(), titleString);
					solrDocument.setField(SolrFieldDefinition.CONTENT.getName(), contentString);
					solrDocument.setField(SolrFieldDefinition.URL.getName(), urlString);
					list.add(solrDocument);
				}
			}
		}

		catch (Exception e) {
			e.printStackTrace();
		}
		log.info("list size: {}", list.size());
		return list;
	}
}