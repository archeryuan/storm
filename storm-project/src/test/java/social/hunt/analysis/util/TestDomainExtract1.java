package social.hunt.analysis.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import social.hunt.data.domain.DomainCategory;
import social.hunt.data.service.DomainCategoryService;
import social.hunt.solr.definition.SolrCollection;
import social.hunt.solr.util.SolrQueryUtil;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.storm.framework.App;

public class TestDomainExtract1 {
	private static final Logger log = LoggerFactory.getLogger(TestDomainExtract1.class);

//	private DomainCategoryService service;
//
//	protected ApplicationContext appContext;
//
//	Map<String, DomainCategory> dataMap;
//
//
//	//@Test
//	public void test() {
//		appContext = App.getInstance().getContext();
//		service = appContext.getBean(DomainCategoryService.class);
//		String url = "http://104.tn.edu.tw/JobList.aspx";
//
//		DomainCategory domain = service.extractDomain("http://bit.ly/1DzYyvu");
//		log.info(domain.toString());
//
//	}

	@Test
	public void load() throws SolrServerException, IOException {
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
		query.setRows(1);

		SolrQueryUtil util = new SolrQueryUtil();
		SolrDocumentList docList = util
				.query(query, SolrCollection.getCollectionString(SolrCollection.SOCIAL_MEDIA, SolrCollection.OTHERS));

		List<SolrInputDocument> outList = new ArrayList<SolrInputDocument>();
		for (SolrDocument doc : docList) {
			outList.add(ClientUtils.toSolrInputDocument(doc));
		}
		
		
		SolrInputDocument solrDocument = outList.get(0);
		log.info("sourceType: {}", (int) solrDocument.getFieldValue(SolrFieldDefinition.NEW_SOURCE_TYPE.getName()));
		
		log.info("isEqual: {}", (solrDocument.getFieldValue(SolrFieldDefinition.SNS_TYPE.getName())));
		
	}
}
