package com.sa.storm.domain.persistence;

import java.util.Date;

import com.sa.common.domain.SourceType;
import com.sa.storm.definition.CADefinition.CounterType;

public class CADailyCounter {

	private SourceType sourceType;

	private String competitorId;

	private CounterType type;

	private Date effectiveDate;

	private Long count;

	public SourceType getSourceType() {
		return sourceType;
	}

	public void setSourceType(SourceType sourceType) {
		this.sourceType = sourceType;
	}

	public String getCompetitorId() {
		return competitorId;
	}

	public void setCompetitorId(String competitorId) {
		this.competitorId = competitorId;
	}

	public CounterType getType() {
		return type;
	}

	public void setType(CounterType type) {
		this.type = type;
	}

	public Date getEffectiveDate() {
		return effectiveDate;
	}

	public void setEffectiveDate(Date effectiveDate) {
		this.effectiveDate = effectiveDate;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

}
