package util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class CHCrawlerConfiguration {

	private static CHCrawlerConfiguration instance;

	private PropertiesConfiguration config;

	private CHCrawlerConfiguration() throws ConfigurationException {
		super();
		this.config = new PropertiesConfiguration("ch-crawler.properties");
	}

	public static CHCrawlerConfiguration getInstance() throws ConfigurationException {
		if (instance == null) {
			synchronized (CHCrawlerConfiguration.class) {
				if (instance == null)
					instance = new CHCrawlerConfiguration();
			}
		}
		return instance;
	}

	public String getFinderFansHDFSPath() {
		return config.getString("finder.hdfs.path");
	}

	public String getRelationHDFSPath() {
		return config.getString("realtion.hdfs.path");
	}

	public String getFinderParserPath() {
		return config.getString("finder.parser.path");
	}

	public String getRelationParserPath() {
		return config.getString("relation.parser.path");
	}

	public String getInterestParserPath() {
		return config.getString("interest.parser.path");
	}

	public String getGraphDBPersistenceUrl() {
		return config.getString("graphdb.persistence.rest.url");
	}
	
	public String getGraphDBDetailPersistenceUrl() {
		return config.getString("graphdb.detail.persistence.rest.url");
	} 
	public String getGraphDBUploadUrl() {
		return config.getString("graphdb.upload.rest.url");
	}
	
	public String getRestTomcatPath() {
		return config.getString("rest.tomcat.path");
	}
}
