package com.sa.storm.sns.bolt.crawl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import social.hunt.crawler.domain.Post;
import social.hunt.crawler.exception.ExtractorNotExistException;
import social.hunt.crawler.exception.RuleNotExistException;
import social.hunt.crawler.extractor.HtmlExtractor;
import social.hunt.crawler.util.RequestUtil;
import social.hunt.data.domain.NExtractor;
import social.hunt.data.domain.NRulePair;
import social.hunt.data.domain.NSite;
import social.hunt.data.service.NSiteService;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

public class CrawlForumBolt extends BaseBolt {

	private static final long serialVersionUID = 4586670132080308429L;

	private static final Logger log = LoggerFactory.getLogger(CrawlForumBolt.class);
	
	private NSiteService siteService;
	
	private RequestUtil requestUtil;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		siteService = getSpringContext().getBean(NSiteService.class);
		requestUtil = getSpringContext().getBean(RequestUtil.class);
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, DocumentResult.class);
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

		String url = request.getParamByKey(TupleDefinition.Param.URL);
		if (StringUtils.isEmpty(url)) {
			TaskResult taskResult = new TaskResult(request, "URL is emtpy ", TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
			return;
		}
		
		NSite site = siteService.getMostPossibleSiteByUrl(url);
		if (null == site) {
			log.error("Not found site for url: {}", url);
			return;
		}

		Set<NRulePair> rulePairs = site.getRulePairs();

		if (null == rulePairs || rulePairs.isEmpty())
			throw new RuleNotExistException("Not found rule for site: " + site.getUrl());
		
		NRulePair rulePair = siteService.findUrlMostPossibleRulePair(url, rulePairs);
		if (null == rulePair) {
			throw new RuleNotExistException("Not found matched rule pair in site: " + site.getUrl() + " for url: " + url);
		}
		
		NExtractor extractor = rulePair.getExtractor();
		if (null == extractor || StringUtils.isEmpty(extractor.getFullClassPath()))
			throw new ExtractorNotExistException("Not found extractor in rule pair " + rulePair.getId() +  " in site: " + site.getUrl() + " for url: " + url);
		
		Class<?> clzExtractor = Class.forName(extractor.getFullClassPath());
		String clzName = clzExtractor.getSimpleName();
		String first = StringUtils.substring(clzName, 0, 1);
		String afterFirst = StringUtils.substring(clzName, 1);
		String beanName = StringUtils.lowerCase(first) + afterFirst;
		
		HtmlExtractor htmlExtractor = (HtmlExtractor) getSpringContext().getBean(beanName,  clzExtractor);	

	
		Post tweet = htmlExtractor.extract(url);
		log.debug("tweet: {}", tweet);

		if (null == tweet) {
			TaskResult taskResult = new TaskResult(request, "No tweet found for URL: " + url, TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
			return;
		}
		Set<Post> tweets = new HashSet<Post>();
		tweets.add(tweet);

		List<TaskRequest> taskRequests = requestUtil.post2TaskRequest(tweets);	
		if (null == taskRequests || taskRequests.isEmpty()) {
			TaskResult taskResult = new TaskResult(request, "No valid tweet found for URL: " + url, TupleDefinition.Result.FAIL.getCode());
			emit(input, taskResult);
			return;
		}
		
		requestUtil.submitForAnalysis(taskRequests);
		log.info("Reay to analysis tweet, URL: {}", url);

	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		String url = request.getParamByKey(TupleDefinition.Param.URL);
		TaskResult error = new TaskResult(request, "Error in crawl forum, URL: " + url, e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

}
