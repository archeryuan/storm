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

public class FBRedis1 {
	Integer priority = 0;
	private static RedisPubSubAckQueueService queueService;
	private static RedisDefinition.MessageQueue queue;

	public static void main(String[] args) throws Exception {
		String queryUrl = args[0];
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
				 queryUrl,
		//		 "https://www.facebook.com/search/married/users/males/15087023444/likers/intersect",
		 //"https://www.facebook.com/search/single/users/males/15087023444/likers/intersect"
				 };

//		String query[] = { "married,males", "single,males", "divorced,males", "engaged,males", "widowed,males", "separated,males",
//				"in-open-relationship,males", "its-complicated,males", "in-civil-union,males", "in-domestic-partnership,males",
//				"dating,males", "married,females", "single,females", "divorced,females", "engaged,females", "widowed,females",
//				"separated,females", "in-open-relationship,females", "its-complicated,females", "in-civil-union,females",
//				"in-domestic-partnership,females", "dating,females" };
		 String query[] = { 
				 "married,males", 
				// "single,males"
				 };
		int i = 0;
		for (String url : urls) {
			String info[] = query[i].split(",");
			Document newdoc = new Document(i + "", "2", SourceType.FACEBOOK);
			newdoc.setUserId(args[1]);
			newdoc.setUserName(args[2]);
			newdoc.setTypeId(typeId);
			newdoc.setUrl(url);
			newdoc.setGender(info[1]);
			newdoc.setsContent(info[0]);
			System.out.println(JsonUtil.getMapper().writeValueAsString(newdoc));
			docs.add(newdoc);
			i++;
		}

		if (docs != null && !docs.isEmpty()) {
			for (Document doc : docs) {

				TaskRequest request = new TaskRequest("1", "12", TaskType.FB_FAN_PAGE.getCode(), "News", priority, params);
				request.addParam(TupleDefinition.Param.DOCUMENTS, JsonUtil.getMapper().writeValueAsString(doc));

				requests.add(request);
			}
		}

		queue = MessageQueue.FACEBOOK_TASK_REQUEST;
		queueService = new RedisPubSubAckQueueService(queue.getWorkQueue(), queue.getProcessingQueue(), queue.getChannel());

		// Submit task request message
		for (TaskRequest request : requests) {
			TaskRequestMessage requestMsg = request.toMessage();
			queueService.enqueue(JsonUtil.getMapper().writeValueAsString(requestMsg), Priority.NORMAL);

		}

	}

}
