package org.corps.bi.datacenter.producer;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataCenterTopicProducer<K,V> {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(DataCenterTopicProducer.class);
	
	private final String topic;
	
	private final Producer<K, V> producer;
	
	private final AtomicLong  sendNum;
	 
	public DataCenterTopicProducer(String topic,Properties properties) {
		super();
		this.topic=topic;
		this.producer=new KafkaProducer<K,V>(properties);
		this.sendNum=new AtomicLong(0);
	}
	
	public boolean send(K key,V value){
		
		try {
			ProducerRecord<K,V> record=new ProducerRecord<K,V>(this.topic,key,value);
			
			Future<RecordMetadata> future=this.producer.send(record, new DataCenterTopicCallback(this.sendNum.incrementAndGet()));
			
//			if(this.sendNum.get()%2000==0) {
//				LOGGER.info("the topic:{} sending the {} message!",topic,this.sendNum.get());
//			}
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
			return false;
		}
	
	}
	
	private class DataCenterTopicCallback implements Callback{
		
		private final long sendIndex;
		
		public DataCenterTopicCallback(long sendIndex) {
			super();
			this.sendIndex = sendIndex;
		}

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			if(exception != null) {
				LOGGER.error(exception.getMessage(),exception);
            } else {
            	if(sendIndex%2000==0) {
            		LOGGER.info("the topic:{} have already sended the {} message!",topic,this.sendIndex);
            	}
            }
		}
		
	}
	
	


}
