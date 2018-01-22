package com.sa.storm.sns.bolt.webdriver.test;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.sa.common.definition.SourceType;
import com.sa.common.json.JsonUtil;
import com.sa.crawler.definition.TaskType;
import com.sa.crawler.message.TaskRequestMessage;
import com.sa.redis.definition.RedisDefinition.MessageQueue;
import com.sa.redis.definition.RedisDefinition.Priority;
import com.sa.redis.service.RedisPubSubAckQueueService;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;


/**
 * @description
 * 
 * @author Luke
 * @created date 2015-1-2
 * @modification history<BR>
 *               No. Date Modified By <B>Why & What</B> is modified
 * 
 * @see
 */
public class SubmitRequestToMQ {

	/**
	 * @description
	 * 
	 * @param
	 * @return
	 * @throws Exception
	 * @throws
	 * 
	 * @author Luke
	 * @created date 2015-1-2
	 * @modification history<BR>
	 *               No. Date Modified By <BR>
	 *               Why & What</BR> is modified
	 * 
	 * @see
	 */
	public static void main(String[] args) throws Exception {
		String userId = "360196524097882";
		String pageId = "360196524097882";
		MessageQueue queue = MessageQueue.FACEBOOK_GROUP_TASK_REQUEST;
		RedisPubSubAckQueueService queueService = new RedisPubSubAckQueueService(queue.getWorkQueue(), queue.getProcessingQueue(),
				queue.getChannel());
		String requestId = UUID.randomUUID().toString();
		String parentRequestId = null;
		Map<String, String> params = new HashMap<String, String>();
		TaskRequest request = new TaskRequest(requestId, parentRequestId, TaskType.CA_PAGE.getCode(), SourceType.FACEBOOK.getSourceTypeStr(),
				Priority.NORMAL.getCode(), params);
		request.addParam(TupleDefinition.Param.IS_GROUP, "true");
		request.addParam(TupleDefinition.Param.PAGE_ID, pageId);
		request.addParam(TupleDefinition.Param.USER_ID, userId);

		TaskRequestMessage requestMsg = request.toMessage();
		queueService.enqueue(JsonUtil.getMapper().writeValueAsString(requestMsg), Priority.NORMAL);
		
		System.out.println("done");
	}

}
