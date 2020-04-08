package org.corps.bi.datacenter.connect.core;

import java.util.HashMap;
import java.util.Map;

public enum MetricLoggerControl {
	
	DAU("dau",1),
	
	INSTALL("install",1),
	
	COUNTER("counter",100),
	
	ECONOMY("economy",20),
	
	GAMEINFO("gameinfo",10),
	
	MILESTONE("milestone",10),
	
	PAYMENT("payment",1),
	
	ADTRACKING("adtracking",10),
	
	CUSTOMBINARYBODYMETRIC("custombinarybodymetric",10);
	
	private static final Map<String,MetricLoggerControl> TOPIC_METRIC_MAP=new HashMap<String,MetricLoggerControl>();
	
	private final String metric;
	
	/**
	 * 控制每多少条打印一次
	 */
	private final int perNum;

	private MetricLoggerControl(String metric,int perNum){
		this.metric = metric;
		this.perNum=perNum;
	}

	public String getMetric() {
		return metric;
	}
	

	public int getPerNum() {
		return perNum;
	}


	static {
		for (MetricLoggerControl dataCenterTopic : MetricLoggerControl.values()) {
			TOPIC_METRIC_MAP.put(dataCenterTopic.metric, dataCenterTopic);
		}
	}
	
	public static MetricLoggerControl parseFromName(String metric) {
		if(TOPIC_METRIC_MAP.containsKey(metric)) {
			return TOPIC_METRIC_MAP.get(metric);
		}
		return null;
	}
	
	

}
