package org.corps.bi.datacenter.server.statistics.service;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.corps.bi.metrics.Meta;

public interface MetricStatisticService{
	
	/**
	 * eg:secondField(20:08:15)
	 * @param metricDayMeta
	 * @param secondField
	 * @param incrNum
	 */
	void incrMetricNum(Meta metricDayMeta, String secondField,long incrNum);
	
	void incrMetricNum(TimeUnit timeUnit,int window,Meta metricDayMeta,long incrNum);
	
	void incrMetricNum(TimeUnit timeUnit,int window,Date date,Meta metricDayMeta,long incrNum);
	
	Long getMetricNum(Meta metricDayMeta, String secondField);
	
	Map<String,Long> getMetricNum(Meta metricDayMeta);

}