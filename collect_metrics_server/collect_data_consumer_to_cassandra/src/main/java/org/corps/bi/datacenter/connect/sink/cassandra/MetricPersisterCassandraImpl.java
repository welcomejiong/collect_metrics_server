package org.corps.bi.datacenter.connect.sink.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.datacenter.connect.sink.MetricPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class MetricPersisterCassandraImpl implements MetricPersister{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricPersisterCassandraImpl.class);
	
	private final AtomicBoolean shutdowned = new AtomicBoolean(false);
	
	private final ConnectMetricConfig connectMetricConfig;
	
	private final Session session;
	
	private final PreparedStatement preparedStatement;
	
	private final AtomicLong perpistedRecordNum=new AtomicLong(0);
	
	public MetricPersisterCassandraImpl(ConnectMetricConfig connectMetricConfig,Session session) {
		super();
		this.connectMetricConfig = connectMetricConfig;
		this.session=session;
		this.preparedStatement=this.generatePreparedStatement();
	}
	
	private PreparedStatement generatePreparedStatement() {
		MetricCassandraCqls topicCassandraCqls=MetricCassandraCqls.parseFromName(this.connectMetricConfig.getTopic());
		PreparedStatement preparedStatement = this.session.prepare(topicCassandraCqls.getCassandraCql());
		return preparedStatement;
	}
	
	
	public boolean persist(ConsumerRecords<byte[], byte[]> records) throws Exception {
		
		if(this.shutdowned.get()) {
			return false;
		}
		
		if(records==null||records.isEmpty()) {
			LOGGER.warn("metric:{} persist the records size is zero!",this.connectMetricConfig.getTopic());
			return true;
		}
		
		List<ConsumerRecord<byte[], byte[]>> sinkRecords = new ArrayList<ConsumerRecord<byte[], byte[]>>();
		for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
			sinkRecords.add(consumerRecord);
		}
		// 如果数据量特别大的话，考虑使用线程池的方案
		MetricToCassandraRunnable metricToCassandraRunnable=new MetricToCassandraRunnable(this.connectMetricConfig.getTopic(),this.session,this.preparedStatement,this.perpistedRecordNum,sinkRecords);
		boolean ret=metricToCassandraRunnable.call();
		
		return ret;
	}
	
	
	public void shutdown() {
		if(this.shutdowned.compareAndSet(false, true)) {
			
		}
	}
	

}
