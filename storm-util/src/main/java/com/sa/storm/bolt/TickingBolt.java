package com.sa.storm.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.common.definition.SourceType;
import com.sa.crawler.definition.TaskType;
import com.sa.storm.domain.tuple.TaskRequest;

import org.apache.storm.Config;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class TickingBolt extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(TickingBolt.class);

	protected TaskType taskType;

	protected Set<SourceType> sourceTypes;

	protected final int tickIntervalInSec;

	public TickingBolt(TaskType taskType, Set<SourceType> sourceTypes, int tickIntervalInSec) {
		this.taskType = taskType;
		this.sourceTypes = sourceTypes;
		this.tickIntervalInSec = tickIntervalInSec;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = super.getComponentConfiguration();
		if (conf == null) {
			conf = new HashMap<String, Object>();
		}
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickIntervalInSec);
		return conf;
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, TaskRequest.class);
	}

	@Override
	public void process(Tuple input) throws Exception {
		if (isTickTuple(input)) {
			log.info("Tick tuple received");

			for (SourceType sourceType : sourceTypes) {
				TaskRequest request = new TaskRequest(UUID.randomUUID().toString(), null, taskType.getCode(),
						sourceType.getSourceTypeStr(), new HashMap<String, String>());
				emit(input, request, "Emit a request by ticking");
			}
		}
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		log.error("Error in processing tick tuple", e);
	}
}
