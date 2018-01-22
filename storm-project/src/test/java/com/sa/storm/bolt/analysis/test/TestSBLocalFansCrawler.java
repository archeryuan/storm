package com.sa.storm.bolt.analysis.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.sa.common.json.JsonUtil;
import com.sa.crawler.definition.TaskType;
import com.sa.crawler.message.TaskRequestMessage;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.definition.RedisDefinition.MessageQueue;
import com.sa.redis.definition.RedisDefinition.Priority;
import com.sa.redis.service.RedisPubSubAckQueueService;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;

public class TestSBLocalFansCrawler {

	private static RedisPubSubAckQueueService queueService;
	private static RedisDefinition.MessageQueue queue;

	
	public static void main(String[] args) throws Exception {
		queue = MessageQueue.SB_LOCAL_FANS_REQUEST;
		queueService = new RedisPubSubAckQueueService(queue.getWorkQueue(), queue.getProcessingQueue(), queue.getChannel());
		Integer priority = 0;
		Map<String, String> params = new HashMap<String, String>();
		
		Collection<TaskRequest> requests = new ArrayList<TaskRequest>();

		TaskRequest request = new TaskRequest("1", "12", TaskType.SB_PAGE.getCode(), "", priority, params);
		request.addParam(TupleDefinition.Param.URL, "http://sb.com");
//		requests.add(request);
		
//		// Submit task request message
//		for (TaskRequest request : requests) {
			TaskRequestMessage requestMsg = request.toMessage();
			queueService.enqueue(JsonUtil.getMapper().writeValueAsString(requestMsg), Priority.NORMAL);
//
//		}


	}

}
