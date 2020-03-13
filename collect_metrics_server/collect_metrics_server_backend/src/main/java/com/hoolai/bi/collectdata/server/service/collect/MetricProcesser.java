package com.hoolai.bi.collectdata.server.service.collect;

import java.util.List;
import java.util.Map;

import org.corps.bi.core.MetricRequestParams;

public interface MetricProcesser {

	public boolean process(MetricRequestParams params);
	
	
	public Map<MetricRequestParams,Boolean> process(List<MetricRequestParams> paramsList);
	
	boolean process(String metric,byte[] metricKey,byte[] metricVal);
	
	
}
