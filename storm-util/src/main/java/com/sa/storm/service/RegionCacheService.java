package com.sa.storm.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.common.db.util.MySqlUtil;
import com.sa.storm.domain.persistence.Region;
import com.sa.storm.util.MemoryCacheManager;

public class RegionCacheService {
	private static final Logger log = LoggerFactory.getLogger(RegionCacheService.class);

	private static final String REGION_DATA = "region";

	public static void initCache() throws Exception {
		MemoryCacheManager.save(REGION_DATA, getData());
	}

	/***
	 * region cache format:regionName_countryCode_regionid_parentId; regionName_countryCode_regionid_parentId
	 * 
	 * @throws Exception
	 ***/
	private static String getData() throws Exception {
		Connection conn = null;
		MySqlUtil mySqlUtil = MySqlUtil.getInstance();

		// Load user pool from MySQL
		conn = mySqlUtil.getConnection();
		StringBuffer regionData = new StringBuffer("");
		PreparedStatement ps = conn.prepareStatement("select regionChiName, countryCode, id, parentId from Region");
		ResultSet rs = ps.executeQuery();
		try {
			while (rs.next()) {
				regionData.append(formatRegion(rs));
			}
			log.info("Loaded region data from database");
		} catch (Exception e) {
			log.error("Error in loading region data");
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
		}

		return regionData.toString();
	}

	public static Region getRegionInfo(long regionId) {
		String regionStr = MemoryCacheManager.load(REGION_DATA);
		int matchPos = regionStr.indexOf("_" + regionId + "_");

		if (matchPos >= 0) {
			String postfix = regionStr.substring(matchPos).split(";")[0];
			String[] prefixArray = regionStr.substring(0, matchPos).split(";");
			String prefix = prefixArray[prefixArray.length - 1];

			String[] regionArrp = (prefix + postfix).split("_");
			Region region = new Region();
			region.setRegionName(regionArrp[0]);
			region.setId(Long.parseLong(regionArrp[2]));
			region.setParentId(Long.parseLong(regionArrp[3]));
			return region;
		}

		return null;
	}

	public static Region getRegionInfo(String regionNameStr) {
		if (regionNameStr == null || regionNameStr.isEmpty()) {
			return null;
		}
		
		String regionStr = MemoryCacheManager.load(REGION_DATA);
		String regionName = "";
		regionName = regionNameStr.replaceAll("市", "").replaceAll("省", "");
		int pos = regionName.length() - 2;
		String result = "";
		Region region = new Region();
		while (pos >= 0) {
			String queryStr = regionName.substring(pos--, regionName.length());
			int strPos = regionStr.indexOf(queryStr);
			if (strPos >= 0) {
				result = regionStr.substring(strPos).split(";")[0];
				return parseRegion(result);
			}
		}

		pos = 2;
		while (pos <= regionName.length()) {
			String queryStr = regionName.substring(0, pos++);
			int strPos = regionStr.indexOf(queryStr);
			if (strPos >= 0) {
				result = regionStr.substring(strPos).split(";")[0];
				return parseRegion(result);
			}
		}

		region.setRegionName("");
		region.setParentId(-1l);
		region.setId(-1l);
		return region;
	}

	private static String formatRegion(ResultSet rs) throws Exception {
		return new StringBuffer(rs.getString("regionChiName")).append("_").append(rs.getString("countryCode")).append("_")
				.append(rs.getString("id")).append("_").append(rs.getString("parentId")).append(";").toString();
	}

	private static Region parseRegion(String regionStr) {
		if (regionStr == null) {
			return null;
		}

		String[] regionStrs = regionStr.split("_");
		if (regionStrs.length != 4) {
			return null;
		}

		Region region = new Region();
		region.setRegionName(regionStrs[0]);
		region.setCountryCode(regionStrs[1]);
		region.setId(Long.parseLong(regionStrs[2]));
		region.setParentId(Long.parseLong(regionStrs[3]));

		return region;
	}

	public static void main(String[] args) throws Exception {
		RegionCacheService.initCache();
		Region region = RegionCacheService.getRegionInfo("Washington");
		System.out.println(region.getId() + " " + region.getParentId() + " " + region.getRegionName() + " " + region.getCountryCode());
	}
}
