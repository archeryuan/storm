package com.sa.storm.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class CommonStormConfiguration {

	private static CommonStormConfiguration instance;

	private PropertiesConfiguration config;

	private CommonStormConfiguration() throws ConfigurationException {
		super();
		this.config = new PropertiesConfiguration("common-storm.properties");
	}

	public static CommonStormConfiguration getInstance() throws ConfigurationException {
		if (instance == null) {
			synchronized (CommonStormConfiguration.class) {
				if (instance == null)
					instance = new CommonStormConfiguration();
			}
		}
		return instance;
	}

	public int getTrackLogTimeout() {
		return config.getInt("tracklog.timeout");
	}
}
