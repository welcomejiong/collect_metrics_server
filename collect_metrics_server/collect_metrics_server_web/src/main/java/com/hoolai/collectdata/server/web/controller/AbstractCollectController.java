package com.hoolai.collectdata.server.web.controller;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.MutablePair;
import org.corps.bi.datacenter.core.DataCenterTopics;


public abstract class AbstractCollectController extends AbstractPanelController{

	protected final Map<String,MutablePair<AtomicLong, AtomicLong>> metricProcessNumMap;
	
	public AbstractCollectController() {
		super();
		this.metricProcessNumMap=new ConcurrentHashMap<String,MutablePair<AtomicLong, AtomicLong>>(DataCenterTopics.values().length);
		this.init();
	}
	
	private void init() {
		for (DataCenterTopics dataCenterTopic : DataCenterTopics.values()) {
			// left:请求次数  right:请求处理的数据数
			MutablePair<AtomicLong, AtomicLong> mutablePair=new MutablePair<AtomicLong, AtomicLong>(new AtomicLong(0),new AtomicLong(0));
			this.metricProcessNumMap.put(dataCenterTopic.getMetric(), mutablePair);
		}
	}
	
	
}
