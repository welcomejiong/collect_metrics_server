package org.corps.bi.datacenter.server.statistics.repo;

import java.util.List;
import java.util.Map;

import org.corps.bi.datacenter.server.statistics.core.MetricNumIncrement;
import org.corps.bi.metrics.Meta;

public interface MetricStatisticRepo{
	
	/**
	 * eg:secondField(20:08:15)
	 * @param metricDayMeta
	 * @param secondField
	 * @param incrNum
	 */
	Long incrMetricNum(Meta metricDayMeta, String secondField,long incrNum);
	
	void incrMetricNum(List<MetricNumIncrement> metricNumIncrements);
	
	Long getMetricNum(Meta metricDayMeta, String secondField);
	
	Map<String,Long> getMetricNum(Meta metricDayMeta);

}