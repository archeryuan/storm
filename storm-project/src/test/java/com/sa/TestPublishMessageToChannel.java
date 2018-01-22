/**
 *Copyright(C) ©2015 Social Hunter. All rights reserved.
 *
 */
package com.sa;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;

import social.hunt.data.domain.DashboardProfile;

import com.sa.common.definition.SolrFieldDefinition;
import com.sa.redis.definition.RedisDefinition.ChannelDef;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.sns.util.WebSocketUtil;

/**
 * @author Luke
 *
 */
public class TestPublishMessageToChannel {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		List<DashboardProfile> profiles = new ArrayList<DashboardProfile>();

		DashboardProfile profile = new DashboardProfile();
		profile.setId(10l);
		Set<String> keywords = new HashSet<String>();
		keywords.add("hello");
		keywords.add("world");
		profile.setKeywords(keywords);

		profiles.add(profile);

		profile = new DashboardProfile();
		profile.setId(11l);
		keywords = new HashSet<String>();
		keywords.add("hong kong");
		keywords.add("science park");
		profile.setKeywords(keywords);
		profiles.add(profile);

		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField(SolrFieldDefinition.URL.getName(), "http://www.abc.com");
		doc.addField(SolrFieldDefinition.TITLE.getName(), "hello friend");
		doc.addField(SolrFieldDefinition.CONTENT.getName(), "glad to meet you in this world");
		doc.addField(SolrFieldDefinition.NEW_SOURCE_TYPE.getName(), 99);
		doc.addField(SolrFieldDefinition.PRIMARY_IMAGE.getName(), "http://www.abc.com/1.jpg");
		docs.add(doc);

		doc = new SolrInputDocument();
		doc.addField(SolrFieldDefinition.URL.getName(), "http://www.def.com");
		doc.addField(SolrFieldDefinition.TITLE.getName(), "hello god");
		doc.addField(SolrFieldDefinition.CONTENT.getName(), "see you in hong kong science park");
		doc.addField(SolrFieldDefinition.NEW_SOURCE_TYPE.getName(), 99);
		doc.addField(SolrFieldDefinition.PRIMARY_IMAGE.getName(), "http://www/def.com/2.jpg");
		docs.add(doc);

		WebSocketUtil.publishProfileKeywordNotificationEvent(docs, null, null, 200);
		
		RedisUtil.getFrontendInstance().publish(ChannelDef.CHANNEL_KEYWORD_NOTIFICATION, "C-7Eok5kE_Zz3pa7AAAF");
		

		System.out.println("done");

	}

}
