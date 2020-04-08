package org.corps.bi.datacenter.connect.consume;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.datacenter.connect.sink.MetricPersister;
import org.corps.bi.datacenter.connect.sink.cassandra.MetricPersisterCassandraImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

public class MetricTopicConsumer implements Runnable{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricTopicConsumer.class);
	
	private final AtomicBoolean closed = new AtomicBoolean(false);
	
	private final Properties metricProperties;
	
	private final ConnectMetricConfig connectMetricConfig;
	
	private final Session session;
	
	private KafkaConsumer<byte[], byte[]> consumer;
	
	private MetricPersister metricPersister;

	public MetricTopicConsumer(ConnectMetricConfig connectMetricConfig,Properties metricProperties,Session session) {
		super();
		this.connectMetricConfig=connectMetricConfig;
		this.metricProperties=metricProperties;
		this.session=session;
		this.consumer=new KafkaConsumer<byte[], byte[]>(metricProperties);
		
	}
	
	@Override
	public void run() {
		this.subscribe();
	}
	
	private void subscribe(){
		
		try {
			
			LOGGER.info("metric:{} consume start...",this.connectMetricConfig.getTopic());
			
			this.consumer.subscribe(Arrays.asList(connectMetricConfig.getTopic()));
			
			this.metricPersister=new MetricPersisterCassandraImpl(connectMetricConfig,this.session);
			
			
			while (!closed.get()) {

			     try {
					ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(10000));
					 
					// 为了防止没有数据，不会触发检测过期的persister,因此需要在每次获取数据后（不论是否有数据，都触发之后操作),因此不做数据null检测
					/*
					 if(records==null||records.isEmpty()) {
						 continue;
					 }
					*/
					 boolean res=this.metricPersister.persist(records);
					 
					 if(res) {
						 this.consumer.commitSync(Duration.ofMillis(10000));
					 }
					 
				} catch (Exception e) {
					LOGGER.error(e.getMessage(),e);
				}

			 }
		} catch (WakeupException e) {
			// Ignore exception if closing
            if (!closed.get()) {
            	LOGGER.error(e.getMessage(),e);
            	throw e;
            }
            	
		}catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}finally {
			this.consumer.unsubscribe();
            this.consumer.close();
        }
	}
	
	
	// Shutdown hook which can be called from a separate thread
    public void shutdown() {
        try {
			closed.set(true);
			consumer.wakeup();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
    }

}
