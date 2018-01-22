package com.sa.storm.sns.bolt.analysis;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.RegionException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import social.hunt.analysis.categorization.DomainCategorizationUtil;
import social.hunt.common.definition.Region;
import social.hunt.data.domain.DomainCategory;
import social.hunt.storm.domain.tuple.PersistTaskResult;
import social.hunt.storm.domain.tuple.SemantriaTaskResult;
import social.hunt.storm.domain.tuple.solr.SolrInputDocumentMap;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.common.definition.SourceType;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskResult;

public class DomainAnalysisBolt extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(DomainAnalysisBolt.class);

	// private DomainCategoryService service;

	protected ApplicationContext appContext;

	private DomainCategorizationUtil catUtil;


	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			// appContext = getSpringContext();
			// service = appContext.getBean(DomainCategoryService.class);
			catUtil = new DomainCategorizationUtil();

		} catch (Exception e) {
			log.error("Error in prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, PersistTaskResult.class);
	}

	@Override
	public void process(Tuple input) throws Exception {
		SemantriaTaskResult taskResult = (SemantriaTaskResult) getInputByClass(input, SemantriaTaskResult.class);
		SolrInputDocumentMap inputDocMap = taskResult.getDocumentMap();
		

		for (SourceType sourceType : inputDocMap.keySet()) {
			List<SolrInputDocument> docs = inputDocMap.get(sourceType);
			for(SolrInputDocument ddd : docs){
				String pttUrl = (String)ddd.getFieldValue((SolrFieldDefinition.URL.getName()));
				if(pttUrl.contains("tianya.cn")){
					log.info("tianya doc ================================="+ddd.get(SolrFieldDefinition.URL.getName()));
				}
			}
			int matchCount = 0;
			for (SolrInputDocument solrDoc : docs) {
				String domain = "";

				if (null != solrDoc.getField(SolrFieldDefinition.DOMAIN.getName())) {
					domain = (String) solrDoc.getFieldValue(SolrFieldDefinition.DOMAIN.getName());
					log.info("Domain: {}", domain);
					
					DomainCategory domainCategory = catUtil.getDomainCat(domain);

					// url = (String) solrDoc.getFieldValue(SolrFieldDefinition.URL.getName());
					// log.info("Url: {}", url);

					// DomainCategory domainCategory = null;
					// if (!StringUtils.isEmpty(domain)) {
					// domainCategory = service.extractDomainObject(url);
					// } else {
					// log.error("Unable to extract url from SolrInputDocument!");
					// }
					
					if (null != domainCategory) {
						matchCount++;
						Short level = domainCategory.getLevel();
						int regionId = domainCategory.getRegion();
						int sourceTypeId = domainCategory.getSourceType();
						log.info("Object info - level: {} regionId: {} sourceTypeId: {}", new Object[] { level, regionId, sourceTypeId });

						solrDoc.setField(SolrFieldDefinition.Region.getName(), regionId);
						solrDoc.setField(SolrFieldDefinition.NEW_SOURCE_TYPE.getName(), sourceTypeId);

						// Set level if not in social-media collection
						if (null == solrDoc.get(SolrFieldDefinition.SNS_TYPE.getName())) {
							solrDoc.setField(SolrFieldDefinition.LEVEL.getName(), level);
						}
					}else {
						// Set region to OTHERS(99) if not matched
						solrDoc.setField(SolrFieldDefinition.Region.getName(), 99);
					}
				}
			}
			log.info("{} out of {} domains are matched", matchCount, docs.size());
		}

		final PersistTaskResult newRequest = new PersistTaskResult(taskResult, inputDocMap);
		this.emitWithoutAnchor(newRequest, "Finished domain analysis successfully.");
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		SemantriaTaskResult request = (SemantriaTaskResult) getInputByClass(input, SemantriaTaskResult.class);
		TaskResult error = new TaskResult(request, "Error in Domain Analysis", e, TupleDefinition.Result.FAIL.getCode());
		//emit(input, error);
	}
}
