package org.corps.bi.datacenter.server.statistics.core;

import org.corps.bi.metrics.Meta;

public class MetricNumIncrement {
	
	private final Meta metricDayMeta;
	
	private final String secondField;
	
	private final long incrNum;

	public MetricNumIncrement(Meta metricDayMeta, String secondField, long incrNum) {
		super();
		this.metricDayMeta = metricDayMeta;
		this.secondField = secondField;
		this.incrNum = incrNum;
	}

	public Meta getMetricDayMeta() {
		return metricDayMeta;
	}

	public String getSecondField() {
		return secondField;
	}

	public long getIncrNum() {
		return incrNum;
	}

}
