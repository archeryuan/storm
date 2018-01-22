/**
 * 
 */
package com.sa.storm.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.google.common.collect.Maps;
import com.sa.common.dao.config.CommonDaoConfig;

/**
 * @author lewis
 * 
 */
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = { "com.sa.common.dao", "com.sa.competitorhunter.dao", "social.hunt.data.dao" })
@ComponentScan(basePackages = { "com.sa.competitorhunter.service", "com.sa.common.service", "social.hunt.data.service" })
public class DaoConfig extends CommonDaoConfig {
	private static final Logger log = LoggerFactory.getLogger(DaoConfig.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sa.common.dao.config.CommonDaoConfig#entityManagerFactory()
	 */
	@Override
	public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
		LocalContainerEntityManagerFactoryBean factoryBean = null;

		try {
			factoryBean = new LocalContainerEntityManagerFactoryBean();

			List<String> s = new ArrayList<String>();
			s.add("com.sa.common.domain");
			s.add("com.sa.competitorhunter.domain");
			s.add("social.hunt.data.domain");

			// factoryBean.setDataSource(dataSource());
			factoryBean.setPackagesToScan(s.toArray(new String[0]));
			factoryBean.setJpaVendorAdapter(jpaVendorAdapter());

			Map<String, Object> jpaPropertyMap = Maps.newHashMap();
			jpaPropertyMap.put("hibernate.dialect", hibernateDialect == null ? "org.hibernate.dialect.MySQL5InnoDBDialect"
					: hibernateDialect);
			jpaPropertyMap.put("hibernate.format_sql", hibernateFormatSql);
			jpaPropertyMap.put("hibernate.connection.charset", hibernateConnectionCharset);
			jpaPropertyMap.put("hibernate.hbm2ddl.auto", hibernateHbm2ddlAuto);

			if (hibernateConnectionDataSource == null || hibernateConnectionDataSource.length() == 0) {
				factoryBean.setDataSource(dataSource());
			} else {
				jpaPropertyMap.put("hibernate.connection.datasource", hibernateConnectionDataSource);
			}

			factoryBean.setJpaPropertyMap(jpaPropertyMap);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}

		return factoryBean;
	}

}
