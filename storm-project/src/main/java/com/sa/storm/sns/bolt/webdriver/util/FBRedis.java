package com.sa.storm.sns.bolt.webdriver.util;


import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.sa.common.definition.SourceType;
import com.sa.common.domain.Profile;
import com.sa.common.json.JsonUtil;
import com.sa.crawler.definition.TaskType;
import com.sa.crawler.message.TaskRequestMessage;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.definition.RedisDefinition.MessageQueue;
import com.sa.redis.definition.RedisDefinition.Priority;
import com.sa.redis.service.RedisPubSubAckQueueService;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.TaskRequest;

public class FBRedis {
	Integer priority = 0;
	private static RedisPubSubAckQueueService queueService;
	private static RedisDefinition.MessageQueue queue;

	public static void main(String[] args) throws Exception {

		Integer priority = 0;
		long typeId = 2;

		Collection<TaskRequest> requests = new ArrayList<TaskRequest>();

		List<Document> docs = new ArrayList<Document>();
		Map<String, String> params = new HashMap<String, String>();
//		String urls[] = { "https://www.facebook.com/search/married/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/single/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/divorced/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/engaged/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/widowed/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/separated/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/in-open-relationship/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/its-complicated/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/in-civil-union/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/in-domestic-partnership/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/dating/users/males/15087023444/likers/intersect",
//				"https://www.facebook.com/search/married/users/females/15087023444/likers/intersect",
//				"https://www.facebook.com/search/single/users/females/15087023444/likers/intersect",
//				"https://www.facebook.com/search/divorced/users/females/15087023444/likers/intersect",
//				"https://www.facebook.com/search/engaged/users/females/15087023444/likers/intersect",
//				"https://www.facebook.com/search/widowed/users/females/15087023444/likers/intersect",
//				"https://www.facebook.com/search/separated/users/females/15087023444/likers/intersect",
//				"https://www.facebook.com/search/in-open-relationship/users/females/15087023444/likers/intersect",
//				"https://www.facebook.com/search/its-complicated/users/females/15087023444/likers/intersect",
//				"https://www.facebook.com/search/in-civil-union/users/females/15087023444/likers/intersect",
//				"https://www.facebook.com/search/in-domestic-partnership/users/females/15087023444/likers/intersect",
//				"https://www.facebook.com/search/dating/users/females/15087023444/likers/intersect" };

		 String urls[] = {
				 "https://www.facebook.com/search/str/men%2Bwho%2Blike%2Bapple%2Bwho%2Blive%2Bin%2Bhong%2Bkong/keywords_users",
				 "https://www.facebook.com/search/str/women%2Bwho%2Blike%2Bapple%2Bwho%2Blive%2Bin%2Bhong%2Bkong/keywords_users",
		 //"https://www.facebook.com/search/single/users/males/15087023444/likers/intersect"
				 };

//		String query[] = { "married,males", "single,males", "divorced,males", "engaged,males", "widowed,males", "separated,males",
//				"in-open-relationship,males", "its-complicated,males", "in-civil-union,males", "in-domestic-partnership,males",
//				"dating,males", "married,females", "single,females", "divorced,females", "engaged,females", "widowed,females",
//				"separated,females", "in-open-relationship,females", "its-complicated,females", "in-civil-union,females",
//				"in-domestic-partnership,females", "dating,females" };
		 String genders [] = {
				 "male",",female"
		 };
		int i = 0;
		for (String gender : genders) {
			Document newdoc = new Document(i + "", "2", SourceType.FACEBOOK);
			newdoc.setUserId("15087023444");
			newdoc.setUserName("adidas");
			newdoc.setTypeId(typeId);
			newdoc.setUrl(urls[i]);
			newdoc.setGender(gender);
			System.out.println(JsonUtil.getMapper().writeValueAsString(newdoc));
			docs.add(newdoc);
			i++;
		}

		if (docs != null && !docs.isEmpty()) {
			for (Document doc : docs) { 

				TaskRequest request = new TaskRequest("1", "12", TaskType.FB_FAN_PAGE.getCode(), "News", priority, params);
				request.addParam(TupleDefinition.Param.DOCUMENTS, JsonUtil.getMapper().writeValueAsString(doc));
				request.addParam(TupleDefinition.Param.HASHTAG_ID, "n");
				requests.add(request);
			}
		}

		queue = MessageQueue.CH_FINDER_REQUEST;
		queueService = new RedisPubSubAckQueueService(queue.getWorkQueue(), queue.getProcessingQueue(), queue.getChannel());

		// Submit task request message
		for (TaskRequest request : requests) {
			TaskRequestMessage requestMsg = request.toMessage();
			queueService.enqueue(JsonUtil.getMapper().writeValueAsString(requestMsg), Priority.NORMAL);

		}

	}

}
