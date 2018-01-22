package com.sa.storm.domain.persistence;

import com.sa.common.domain.SourceType;
import com.sa.storm.definition.CADefinition.CounterRollingRange;

public class CARollingRangeCounter {

	private SourceType sourceType;

	private String competitorId;

	private Integer counterType;

	private CounterRollingRange counterRange;

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

	public Integer getCounterType() {
		return counterType;
	}

	public void setCounterType(Integer counterType) {
		this.counterType = counterType;
	}

	public CounterRollingRange getCounterRange() {
		return counterRange;
	}

	public void setCounterRange(CounterRollingRange counterRange) {
		this.counterRange = counterRange;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

}
