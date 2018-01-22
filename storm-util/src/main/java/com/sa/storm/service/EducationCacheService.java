package com.sa.storm.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.sa.common.db.util.MySqlUtil;
import com.sa.common.json.JsonUtil;
import com.sa.storm.domain.cache.Education;
import com.sa.storm.util.MemoryCacheManager;

public class EducationCacheService {

	private static final Logger log = LoggerFactory.getLogger(EducationCacheService.class);

	public static final String CACHE_KEY = "education";

	public static void initCache() throws Exception {
		MemoryCacheManager.save(EducationCacheService.CACHE_KEY, getFromDB());
		log.info("Complete caching education data from MySQL");
	}

	private static String getFromDB() throws Exception {
		Connection conn = null;
		List<Education> educations = new ArrayList<Education>();
		String eduJson = "";

		try {
			// Get education data from MySQL table
			MySqlUtil mySqlUtil = MySqlUtil.getInstance();
			conn = mySqlUtil.getConnection();

			PreparedStatement ps = conn.prepareStatement("select id, name, detail from Education");
			ResultSet rs = ps.executeQuery();

			while (rs.next()) {
				educations.add(new Education(rs));
			}

			// Obtain the json string
			eduJson = JsonUtil.getMapper().writeValueAsString(educations);
		} catch (Exception e) {
			log.error("Error in get data for EducationCacheService", e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}

		return eduJson;
	}

	public static Education getEducation(String eduSearchStr) throws Exception {

		if (eduSearchStr == null || eduSearchStr.isEmpty()) {
			return null;
		}

		String eduJson = MemoryCacheManager.load(EducationCacheService.CACHE_KEY);

		if (eduJson != null) {
			TypeFactory typeFactory = TypeFactory.defaultInstance();
			List<Education> educations = JsonUtil.getMapper().readValue(eduJson,
					typeFactory.constructCollectionType(List.class, Education.class));

			for (Education edu : educations) {
				if (eduSearchStr.matches(edu.getDetail())) {
					return edu;
				}
			}
		}

		return null;
	}
}
