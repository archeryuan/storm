package com.sa.storm.sns.bolt.python;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.util.UserPoolUtil;

public class PythonLoginBolt extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(PythonLoginBolt.class);

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {

		super.prepare(stormConf, context, collector);

		try {
			// Extract phantom templates to local directories
			//PhythonCommandUtil.getInstance().deployTemplates(getGlobalTaskId());
		} catch (Exception e) {
			log.error("Error in initializing", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void process(Tuple input) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskRequest phantomRequest = new TaskRequest(request);

		boolean isSuccess = UserPoolUtil.getInstance().login(phantomRequest, getGlobalTaskId());

		TaskResult result = null;
		if (isSuccess) {
			result = new TaskResult(request, "Login successful", TupleDefinition.Result.SUCCESS.getCode());
		} else {
			result = new TaskResult(request, "Login fail", TupleDefinition.Result.FAIL.getCode());
		}

		emit(input, result);
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskRequest phantomRequest = new TaskRequest(request);

		UserPoolUtil.getInstance().loginFail(phantomRequest);

		TaskResult error = new TaskResult(request, "Internal error, marked the user fail", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}
}
