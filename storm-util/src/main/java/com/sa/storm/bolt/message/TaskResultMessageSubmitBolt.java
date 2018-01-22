package com.sa.storm.bolt.message;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.common.json.JsonUtil;
import com.sa.crawler.message.TaskResultMessage;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.service.RedisPubSubAckQueueService;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;

/**
 * Submit a task result to a specific queue. For example send the result back to
 * crawler manager
 * 
 * @author Kelvin Wong
 * 
 */
public class TaskResultMessageSubmitBolt extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory
			.getLogger(TaskResultMessageSubmitBolt.class);

	private RedisDefinition.MessageQueue queue;

	private RedisPubSubAckQueueService redisService;

	public TaskResultMessageSubmitBolt(RedisDefinition.MessageQueue queue) {
		this.queue = queue;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {

		super.prepare(stormConf, context, collector);

		try {
			redisService = new RedisPubSubAckQueueService(queue.getWorkQueue(),
					queue.getProcessingQueue(), queue.getChannel());
		} catch (Exception e) {
			log.error("Error in prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void process(Tuple input) throws Exception {
		TaskResult taskResult = (TaskResult) getInputByClass(input,
				TaskResult.class);

		TaskResultMessage taskResultMessage = taskResult.toMessage();
		final String resultJson = JsonUtil.getMapper().writeValueAsString(
				taskResultMessage);

		// Submit task result message
		redisService.publish(resultJson);
		log.info("Submitted task result: {}", resultJson);

		// Emit the result to monitor bolt
		emit(input,
				taskResult,
				"Successfully published task result message to "
						+ queue.getChannel());
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input,
				TaskRequest.class);
		TaskResult error = new TaskResult(request,
				"Error in submit task result message", e,
				TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}
}
