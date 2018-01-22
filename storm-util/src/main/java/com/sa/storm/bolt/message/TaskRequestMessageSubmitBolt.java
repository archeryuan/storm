package com.sa.storm.bolt.message;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.common.json.JsonUtil;
import com.sa.crawler.message.TaskRequestMessage;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.definition.RedisDefinition.Priority;
import com.sa.redis.service.RedisPubSubAckQueueService;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

/**
 * Submit a task request to a specific queue. Possible use case include pagination or subsequent request after handling a crawl request
 * 
 * @author Kelvin Wong
 * 
 */
public class TaskRequestMessageSubmitBolt extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(TaskRequestMessageSubmitBolt.class);

	private RedisDefinition.MessageQueue queue;

	private RedisPubSubAckQueueService queueService;

	public TaskRequestMessageSubmitBolt(RedisDefinition.MessageQueue queue) {
		this.queue = queue;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {

		super.prepare(stormConf, context, collector);

		try {
			queueService = new RedisPubSubAckQueueService(queue.getWorkQueue(), queue.getProcessingQueue(), queue.getChannel());
		} catch (Exception e) {
			log.error("Error in prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void process(Tuple input) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		// Submit task request message
		TaskRequestMessage requestMsg = request.toMessage();
		queueService.enqueue(JsonUtil.getMapper().writeValueAsString(requestMsg), Priority.getPriority(request.getPriority()));

		// Emit the result to monitor bolt
		TaskResult result = new TaskResult(request, TupleDefinition.Result.SUCCESS.getCode());
		emit(input, result, "Successfully submit task request message to " + queue.getWorkQueue());
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in submitting task request", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

	/**
	 * @return the queueService
	 */
	protected RedisPubSubAckQueueService getQueueService() {
		return queueService;
	}

	/**
	 * @return the queue
	 */
	protected RedisDefinition.MessageQueue getQueue() {
		return queue;
	}

}
