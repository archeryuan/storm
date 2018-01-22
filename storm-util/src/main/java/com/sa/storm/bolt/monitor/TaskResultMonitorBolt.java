package com.sa.storm.bolt.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.TrackLog;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.service.TrackLogService;

public class TaskResultMonitorBolt extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(TaskResultMonitorBolt.class);

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		// This should be the finish point and no tuple are emitted
	}

	@Override
	public void process(Tuple input) throws Exception {
		TaskResult result = (TaskResult) getInputByClass(input, TaskResult.class);
		TrackLog trackLog = new TrackLog(result);

		if (result.getResultCode() == TupleDefinition.Result.FAIL.getCode()) {
			// Persists log to Redis
			TrackLogService trackLogService = new TrackLogService();
			trackLogService.saveLog(trackLog);
			log.info("Persists failed tracklog into Redis:\n{}", trackLog);
		} else {
			log.info("Do not persists successful tracklog into Redis:\n{}", trackLog);
		}
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		log.error("Error in handling result", e);
	}
}
