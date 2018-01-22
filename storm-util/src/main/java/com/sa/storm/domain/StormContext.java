package com.sa.storm.domain;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;


public class StormContext {

	private StormTopology topology;

	private Config config;

	private String name;

	public StormContext() {

	}

	public StormTopology getTopology() {
		return topology;
	}

	public void setTopology(StormTopology topology) {
		this.topology = topology;
	}

	public Config getConfig() {
		return config;
	}

	public void setConfig(Config config) {
		this.config = config;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
