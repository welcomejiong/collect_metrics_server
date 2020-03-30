package com.hoolai.bi.collectdata.server.service.transport.fetchdata;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.MutablePair;
import org.corps.bi.core.MetricLoggerControl;
import org.corps.bi.dao.rocksdb.MetricRocksdbColumnFamilys;
import org.corps.bi.dao.rocksdb.RocksdbGlobalManager;
import org.corps.bi.transport.MetricsTransporterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hoolai.bi.collectdata.server.service.collect.MetricProcesser;

public abstract class AbstractFetchDataThread implements Runnable{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(AbstractFetchDataThread.class);
	
	protected final MetricRocksdbColumnFamilys metricRocksdbColumnFamily;
	
	protected final MetricsTransporterConfig transporterConfig;
	
	protected final String metric;
	
	protected final int batchSize;
	
	protected final AtomicLong fetchDataTimes;
	
	protected final AtomicLong processedRecordNum;
	
	protected final MetricProcesser metricProcesserKafka;
	
	protected final int metricLoggerPerNum;
	
	public AbstractFetchDataThread(final MetricRocksdbColumnFamilys metricRocksdbColumnFamily,final MetricsTransporterConfig transporterConfig,final MutablePair<AtomicLong, AtomicLong> processedRecordNumPair,final MetricProcesser metricProcesserKafka) {
		super();
		this.metricRocksdbColumnFamily=metricRocksdbColumnFamily;
		this.transporterConfig=transporterConfig;
		this.fetchDataTimes=processedRecordNumPair.getLeft();
		this.processedRecordNum=processedRecordNumPair.getRight();
		this.metric=this.metricRocksdbColumnFamily.getMetric();
		this.batchSize = this.transporterConfig.getBatchSize();
		this.metricProcesserKafka=metricProcesserKafka;
		this.metricLoggerPerNum=MetricLoggerControl.parseFromName(this.metric).getPerNum();
	}

	@Override
	public void run() {
		boolean isLock=false;
		try {
			isLock=RocksdbGlobalManager.getInstance().tryLockProcessed(this.metric);
			if(!isLock) {
				//LOGGER.info("metric:{} fetch data try lock is fail!",this.metric);
				return ;
			}
			this.pollMetrics();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}finally {
			if(isLock) {
				RocksdbGlobalManager.getInstance().unLockProcessed(this.metric);
			}
		}
	}
	
	protected abstract void pollMetrics();

}
