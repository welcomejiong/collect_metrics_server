package org.corps.bi.datacenter.core;

import java.util.HashMap;
import java.util.Map;

public enum DataCenterTopics {
	
	DAU("dau"),
	
	INSTALL("install"),
	
	COUNTER("counter"),
	
	ECONOMY("economy"),
	
	GAMEINFO("gameinfo"),
	
	MILESTONE("milestone"),
	
	PAYMENT("payment"),
	
	ADTRACKING("adtracking"),
	
	CUSTOMBINARYBODYMETRIC("custombinarybodymetric");
	
	private static final Map<String,DataCenterTopics> TOPIC_METRIC_MAP=new HashMap<String,DataCenterTopics>();
	
	private final String metric;

	private DataCenterTopics(String metric) {
		this.metric = metric;
	}

	public String getMetric() {
		return metric;
	}
	
	static {
		for (DataCenterTopics dataCenterTopic : DataCenterTopics.values()) {
			TOPIC_METRIC_MAP.put(dataCenterTopic.metric, dataCenterTopic);
		}
	}
	
	public static DataCenterTopics parseFromName(String metric) {
		if(TOPIC_METRIC_MAP.containsKey(metric)) {
			return TOPIC_METRIC_MAP.get(metric);
		}
		return null;
	}
	
	

}
