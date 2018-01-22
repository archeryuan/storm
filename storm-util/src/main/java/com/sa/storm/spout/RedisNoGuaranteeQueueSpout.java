package com.sa.storm.spout;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.utils.Utils;

import com.sa.common.json.JsonUtil;
import com.sa.crawler.message.TaskRequestMessage;
import com.sa.redis.definition.RedisDefinition.Priority;
import com.sa.redis.domain.RedisQueueMessage;
import com.sa.redis.service.IRedisQueueService;
import com.sa.redis.service.RedisPubSubAckQueueService;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

/**
 * Read from a Redis queue and process any received message. Providing ack functionality.
 * 
 * @author Archer Yuan
 * 
 */
public class RedisNoGuaranteeQueueSpout extends SpringSpout {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(RedisNoGuaranteeQueueSpout.class);

	protected final int listenInterval;

	protected final String workQueue;

	protected final String processingQueue;

	protected final Set<Integer> acceptRequestTypes;

	protected IRedisQueueService queueService;

	public RedisNoGuaranteeQueueSpout(String workQueue, String processingQueue, int listenInterval, Set<Integer> acceptRequestTypes)
			throws Exception {
		this.workQueue = workQueue;
		this.processingQueue = processingQueue;
		this.listenInterval = listenInterval;
		this.acceptRequestTypes = acceptRequestTypes;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			super.open(conf, context, collector);
			queueService = new RedisPubSubAckQueueService(workQueue, processingQueue);
		} catch (Exception e) {
			log.error("Error in opening RedisPubSubSpout", e);
		}
	}

	@Override
	public void close() {
		try {
			queueService.shutdown();
		} catch (Exception e) {
			log.info("Error in closing RedisSpout", e);
		}
	}

	@Override
	public void nextTuple() {
		log.info("get message from spout--------------------------------------------\n\n\n\n\n");
		RedisQueueMessage message = null;
		try {
			//log.info("redis spout get message from redis ================================================");
			message = queueService.dequeueWithoutGuarantee();

			if (message != null) {
				// Read message content from queue
				TaskRequestMessage requestMsg = JsonUtil.getMapper().readValue(message.getContent(), TaskRequestMessage.class);

				// Transform to task request
				TaskRequest request = new TaskRequest(requestMsg);

				// Record the start time
				request.addContext(TupleDefinition.Context.START_PROCESS_TIME, String.valueOf(System.currentTimeMillis()));

				if (acceptRequestTypes != null && !acceptRequestTypes.contains(request.getType())) {
					log.warn("Skip processing message in type {}, only accept message in types: {}", request.getType(), acceptRequestTypes);

					// Filter message against acceptRequestTypes
					TaskResult error = new TaskResult(request, "Cannot accept message in type " + request.getType(),
							TupleDefinition.Result.FAIL.getCode());
					//emit(error, message.getContent());

				} else {
					String msgId = new StringBuffer(StringUtils.substring(message.getContent(), 0, 100)).toString();
					log.info("spout emit message ------------ {}", msgId);
					emit(request, "Received request message successfully", null);
				}
			} else {
				log.info("sleep for------------------------"+listenInterval);
				Utils.sleep(listenInterval);
			}
		} catch (Exception e) {
			log.error("Error in processing message from work queue", e);

			if (message != null) {
				// Ack the message since we can no longer handle it
				ack(StringUtils.substring(message.getContent(), 0, 100));
			}
		}
	}

	@Override
	public void ack(Object msgIdObj) {
		String msgId = null;
		try {
			//msgId = (String) msgIdObj;
			log.info("Ack message at spout successfully, id");
		} catch (Exception e) {
			log.error("Error in ack message at spout, id: ", e);
		}
	}

	@Override
	public void fail(Object msgIdObj) {
		String msgId = null;
		try {
			msgId = (String) msgIdObj;
			TaskRequestMessage requestMsg = JsonUtil.getMapper().readValue(msgId, TaskRequestMessage.class);
			Priority priority = Priority.getPriority(requestMsg.getPriority());
			queueService.requeue(msgId, priority);
			log.info("Re-queue message at spout successfully, id: {}, priority: {}", msgId, priority.getCode());
		} catch (Exception e) {
			// Ack the message since we can no longer handle it
			log.error("Error in fail message at spout, id: " + msgId, e);
			ack(msgId);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareFieldByClass(declarer, TaskRequest.class);
	}

	private String getLogMessageId(String msgId) {
		return new StringBuffer(StringUtils.substring(msgId, 0, 100)).append("...").toString();
	}
}
