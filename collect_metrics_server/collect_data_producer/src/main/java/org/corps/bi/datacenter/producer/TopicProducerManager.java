package org.corps.bi.datacenter.producer;

import java.util.concurrent.ConcurrentHashMap;

import org.corps.bi.datacenter.core.DataCenterTopics;
import org.corps.bi.datacenter.producer.core.Constant;

public class TopicProducerManager {
	
	private static TopicProducerManager TOPIC_PRODUCER_MANAGER;
	
	private final ConcurrentHashMap<String, DataCenterTopicProducer<byte[], byte[]>> producerCache;

	private TopicProducerManager() {
		super();
		this.producerCache = new ConcurrentHashMap<String, DataCenterTopicProducer<byte[],byte[]>>();
		this.initDiffTopicProducer();
	}
	
	private void initDiffTopicProducer() {
		for (DataCenterTopics dataCenterTopics : DataCenterTopics.values()) {
			DataCenterTopicProducer<byte[], byte[]> producer=this.instanceDataCenterTopicProducer(dataCenterTopics.getMetric());
			this.producerCache.put(dataCenterTopics.getMetric(), producer);
		}
	}
	
	private DataCenterTopicProducer<byte[], byte[]> instanceDataCenterTopicProducer(String topic){
		DataCenterTopicProducer<byte[], byte[]> producer=new DataCenterTopicProducer<byte[], byte[]>(topic,Constant.getInstance().getKafkaProducerConfigProperties());
		return producer;
	}
	
	public static TopicProducerManager getInstance() {
		if(TOPIC_PRODUCER_MANAGER==null) {
			TOPIC_PRODUCER_MANAGER=new TopicProducerManager();
		}
		return TOPIC_PRODUCER_MANAGER;
	}
	
	
	public boolean send(String topic,byte[] key,byte[] body){
		DataCenterTopicProducer<byte[], byte[]> producer=this.producerCache.get(topic);
		if(producer==null) {
			return false;
		}
		return producer.send(key, body);
		
	}
	
	
	
	

}
