package com.hoolai.bi.collectdata.server.service.collect;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.corps.bi.core.MetricRequestParams;
import org.corps.bi.datacenter.core.DataCenterTopics;
import org.corps.bi.datacenter.producer.TopicProducerManager;
import org.corps.bi.metrics.converter.MetricEntityConverterManager;
import org.corps.bi.services.RecordingServices.MetaExtraBuilder;
import org.corps.bi.tools.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricProcesserKafkaImpl implements MetricProcesser{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricProcesserKafkaImpl.class);
	
	private TopicProducerManager topicProducerManager;
	
	private AtomicLong processNum=new AtomicLong(0);
	
	private String localMachineIp="";

	public MetricProcesserKafkaImpl() {
		super();
		this.topicProducerManager=TopicProducerManager.getInstance();
		this.localMachineIp=NetUtils.getLocalMachineIp();
	}

	@Override
	public boolean process(MetricRequestParams params) {
		MetricEntityConverterManager metricEntityConvert=MetricEntityConverterManager.parseFromName(params.getMetric());
		if(metricEntityConvert==null) {
			LOGGER.warn("metric:"+params.getMetric()+" jsonData:"+params.getJsonData()+" is not match converter.");
			return false;
		}
		
		DataCenterTopics dataCenterTopics=DataCenterTopics.parseFromName(params.getMetric());
		
		if(dataCenterTopics==null) {
			LOGGER.warn("topicMetric:"+params.getMetric()+" jsonData:"+params.getJsonData()+" is not exist topic.");
			return false;
		}
		
		byte[] metricBody=metricEntityConvert.toProtobufBytes(params.getJsonData());
		
		MetaExtraBuilder metaExtraBuilder=new MetaExtraBuilder();
		metaExtraBuilder.addExtra("ip", this.localMachineIp);
		metaExtraBuilder.addExtra("pid", this.processNum.get());
		
		byte[] metricKey=MetricEntityConverterManager.keyProtobufBytes(params.getMetric(), "1", params.getGameId(), params.getDs(),metaExtraBuilder.getExtra());
		
		long processedNum=this.processNum.incrementAndGet();
		
		if(processedNum%1000==0) {
			LOGGER.info("have already processed to kafka of the record nums is {}",processedNum);
		}
		
		return this.topicProducerManager.send(dataCenterTopics.getMetric(), metricKey,metricBody);
		
	}

	@Override
	public Map<MetricRequestParams,Boolean> process(List<MetricRequestParams> paramsList) {
		if(paramsList==null||paramsList.isEmpty()) {
			return Collections.emptyMap();
		}
		Map<MetricRequestParams,Boolean> ret=new HashMap<MetricRequestParams, Boolean>();
		for (MetricRequestParams metricRequestParams : paramsList) {
			boolean res=this.process(metricRequestParams);
			ret.put(metricRequestParams, res);
		}
		return ret;
	}

	@Override
	public boolean process(String metric, byte[] metricKey, byte[] metricBody) {
		
		DataCenterTopics dataCenterTopics=DataCenterTopics.parseFromName(metric);
		
		if(dataCenterTopics==null) {
			LOGGER.warn("topicMetric:{} metricKey:{} metricBody:{} is not exist topic.",metric,metricKey,metricBody);
			return false;
		}
		
		return this.topicProducerManager.send(dataCenterTopics.getMetric(), metricKey,metricBody);
	}
	


	
}
