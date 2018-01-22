package com.sa.storm.domain.persistence;

import java.sql.ResultSet;

public class Region {

	private Long id;

	private Long parentId;

	private String regionName;

	private String countryCode;

	public Region() {

	}

	public Region(ResultSet rs) throws Exception {
		this.fromResultSet(rs);
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getParentId() {
		return parentId;
	}

	public void setParentId(Long parentId) {
		this.parentId = parentId;
	}

	public String getRegionName() {
		return regionName;
	}

	public void setRegionName(String regionName) {
		this.regionName = regionName;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public void fromResultSet(ResultSet rs) throws Exception {
		this.id = rs.getLong("id");
		this.parentId = rs.getLong("parentId");
		this.regionName = rs.getString("regionChiName");
		this.setCountryCode(rs.getString("countryCode"));
	}

	public com.sa.common.domain.Region toRegionDomain() {
		com.sa.common.domain.Region region = new com.sa.common.domain.Region();
		region.setId(id);
		region.setParentId(parentId);
		region.setCountryCode(countryCode);

		return region;
	}
}
