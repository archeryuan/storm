package com.sa.storm.sns.util;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ChCrawlerConfiguration {

	private static ChCrawlerConfiguration instance;

	private PropertiesConfiguration config;

	private ChCrawlerConfiguration() throws ConfigurationException {
		super();
		this.config = new PropertiesConfiguration("ch-crawler.properties");
	}

	public static ChCrawlerConfiguration getInstance() throws ConfigurationException {
		if (instance == null) {
			synchronized (ChCrawlerConfiguration.class) {
				if (instance == null)
					instance = new ChCrawlerConfiguration();
			}
		}
		return instance;
	}

	
	public String getPythonRuntime() {
		return config.getString("python.runtimePath");
	}

	
	public long getPythonTimeout() {
		return config.getLong("python.timeout");
	}

	
	public String getPythonResourcesPattern() {
		return config.getString("python.resourcesPattern");
	}

	public String getPythonTemplatesPath(String uniqueTaskId) {
		return new StringBuffer(config.getString("python.templatesPath")).append(uniqueTaskId).append(File.separator).toString();
	}
	
	public String getPythonCookiesPath(String uniqueTaskId) {
		return new StringBuffer(config.getString("python.cookiesPath")).append(uniqueTaskId).append(File.separator).toString();
	}

	public int getPythonMaxRetryCount() {
		return config.getInt("python.maxRetryCount");
	}
	
	
	public long getPythonRetryInterval() {
		return config.getInt("python.retryInterval");
	}

	public String getResultTemporaryFilePath(String uniqueTaskId) {
		return new StringBuffer(config.getString("result.temporaryFilePath")).append(uniqueTaskId).append(File.separator).toString();
	}


}
