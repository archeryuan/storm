package com.sa.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;



/**
 * A basic storm topology
 * 
 * @author Kelvin Wong
 * 
 */
public abstract class BaseTopology {

	abstract public String getName() throws Exception;

	abstract public TopologyBuilder getBuilder() throws Exception;

	abstract public Config getConfig() throws Exception;

	public void submit() throws Exception {
		StormSubmitter.submitTopology(getName(), getConfig(), getBuilder().createTopology());
	}

	protected int getParallelism(int numWorker, int minParaellelism, int maxParaellelism) {
		final int min = Math.min(numWorker, minParaellelism);
		return Math.max(min, maxParaellelism);
	}
}
