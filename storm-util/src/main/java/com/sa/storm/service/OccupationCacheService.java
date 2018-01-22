package com.sa.storm.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.common.db.util.MySqlUtil;
import com.sa.storm.util.MemoryCacheManager;

public class OccupationCacheService {

	private static final Logger log = LoggerFactory.getLogger(OccupationCacheService.class);

	public static void initCache() throws Exception {
		MemoryCacheManager.save("occupation", getData());
	}

	/***
	 * region cache format:jobDetail_id;jobDetail_id
	 * 
	 * @throws Exception
	 ***/
	private static String getData() throws Exception {
		Connection conn = null;
		MySqlUtil mySqlUtil = MySqlUtil.getInstance();
		String occupationData = "";

		try {
			// Load user pool from MySQL
			conn = mySqlUtil.getConnection();
			PreparedStatement ps = conn.prepareStatement("select id, jobDetail,en_jobDetail from Occupation");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String record = rs.getString("jobDetail") + rs.getString("en_jobDetail") + "_" + rs.getString("id");
				occupationData += record + ";";
			}
			log.info("Loaded occupation data from database");
		} catch (Exception e) {
			log.error("Error in loading occupation data");
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
		return occupationData;

	}

	public static Long getJobInfo(String jobStr) {
		if (jobStr == null || jobStr.isEmpty()) {
			return null;
		}

		String jobDetail = MemoryCacheManager.load("occupation");
		int strPos = jobDetail.indexOf(jobStr);
		Long result = 1l;
		if (strPos >= 0) {
			result = Long.parseLong(jobDetail.substring(strPos).split(";")[0].split("_")[1]);
			log.info(jobDetail.substring(strPos).split(";")[0]);
		}
		return result;

	}
}
