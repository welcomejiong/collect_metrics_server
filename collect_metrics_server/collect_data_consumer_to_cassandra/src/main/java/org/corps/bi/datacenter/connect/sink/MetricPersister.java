package org.corps.bi.datacenter.connect.sink;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface MetricPersister {
	
	boolean persist(ConsumerRecords<byte[], byte[]> records)throws Exception ;
	
	void shutdown();

}
