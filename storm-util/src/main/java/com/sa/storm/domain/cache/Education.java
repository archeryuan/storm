package com.sa.storm.domain.cache;

import java.sql.ResultSet;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Education {

	private Long id;

	private String name;

	private String detail;

	public Education() {

	}

	public Education(ResultSet rs) throws Exception {
		this.id = rs.getLong("id");
		this.name = rs.getString("name");
		this.detail = rs.getString("detail");
	}

	@JsonProperty("id")
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@JsonProperty("name")
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@JsonProperty("detail")
	public String getDetail() {
		return detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

}
