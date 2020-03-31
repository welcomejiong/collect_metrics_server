package com.hoolai.bi.collectdata.server.service.transport.fetchdata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.MutablePair;
import org.corps.bi.dao.rocksdb.MetricRocksdbColumnFamilys;
import org.corps.bi.dao.rocksdb.RocksdbCleanedGlobalManager;
import org.corps.bi.dao.rocksdb.RocksdbGlobalManager;
import org.corps.bi.dao.rocksdb.RocksdbManager;
import org.corps.bi.metrics.IMetric;
import org.corps.bi.metrics.converter.MetaConverter;
import org.corps.bi.metrics.converter.MetricEntityConverterManager;
import org.corps.bi.protobuf.KVEntity;
import org.corps.bi.protobuf.LongEntity;
import org.corps.bi.tools.util.JSONUtils;
import org.corps.bi.transport.MetricsTransporterConfig;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hoolai.bi.collectdata.server.service.collect.MetricProcesser;

public class FetchDataToKafkaThreadV2 extends AbstractFetchDataThread {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(FetchDataToKafkaThreadV2.class);

	public FetchDataToKafkaThreadV2(MetricRocksdbColumnFamilys metricRocksdbColumnFamily,
			MetricsTransporterConfig transporterConfig, MutablePair<AtomicLong, AtomicLong> processedRecordNumPair,
			MetricProcesser metricProcesserKafka) {
		super(metricRocksdbColumnFamily, transporterConfig, processedRecordNumPair, metricProcesserKafka);
	}

	@Override
	protected void pollMetrics() {
		try {
			
			long fdt=this.fetchDataTimes.incrementAndGet();
			
			long begin=System.currentTimeMillis();
			int succ=0;
			RocksDB rockdb=RocksdbManager.getInstance().getRocksdb();
			long processedId=RocksdbGlobalManager.getInstance().getProcessedId(this.metric);
			long currentMetricId=RocksdbGlobalManager.getInstance().getCurrentId(this.metric);
			
			ColumnFamilyHandle metricColumnFamilyHandle=RocksdbManager.getInstance().getColumnFamilyHandle(this.metric);
			List<ColumnFamilyHandle> queryCfList=new ArrayList<ColumnFamilyHandle>();
			final List<byte[]> keys = new ArrayList<>();
			long beginKeyId=processedId+1;
			long endKeyId=processedId+this.batchSize;
			if(endKeyId>currentMetricId) {
				endKeyId=currentMetricId;
			}
			if(endKeyId<beginKeyId) {
				return ;
			}
			for(long j=beginKeyId;j<=endKeyId;j++) {
				LongEntity keyEnity=new LongEntity(j);
				keys.add(keyEnity.toByteArray());
				queryCfList.add(metricColumnFamilyHandle);
			}
			//LOGGER.info("metric:{} processedId:{} currentMetricId:{} beginKeyId:{}  endKeyId:{} expectValues:{} begining...",this.metric,processedId,currentMetricId,beginKeyId,endKeyId,(endKeyId-beginKeyId));
			
			Map<byte[], byte[]> values = rockdb.multiGet(queryCfList,keys);
			
			if(values==null||values.isEmpty()){
				// 如果beginKeyId到endKeyId，没有值存在，则继续把序号往前推进，一直到当前指标的值（currentMetricId）
				RocksdbGlobalManager.getInstance().saveProcessedId(this.metric, endKeyId);
				LOGGER.warn("metric:{} fetchDataTimes:{} processedId:{} currentMetricId:{} beginKeyId:{}  endKeyId:{} expectValues:{} the key of values is empty!...",this.metric,fdt,processedId,currentMetricId,beginKeyId,endKeyId,(endKeyId-beginKeyId));
				return ;
			}
			
			long currentMaxProcessedId=0;
			int addTimes=0;
			boolean isSucc=false;
			
			List<Long> processedIdList=new ArrayList<Long>();
			for (Entry<byte[], byte[]> entry: values.entrySet()) {
				
				LongEntity keyEnity=new LongEntity(entry.getKey());
				
				if(currentMaxProcessedId<keyEnity.getValue()) {
					currentMaxProcessedId=keyEnity.getValue();
				}
				
				KVEntity kvEntity=new KVEntity(entry.getValue());
				

				isSucc=this.metricProcesserKafka.process(this.metric, kvEntity.getK(), kvEntity.getV());
				
				if(isSucc) {
					addTimes++;
					
					processedIdList.add(keyEnity.getValue());
				}else {
					LOGGER.warn("metric:{} key:{} currentKeyId:{} needProcessKeyId:{} add to kafka processer isSucc:{}.",this.metric,entry.getKey(),currentMaxProcessedId,entry.getKey(),isSucc);	
				}
				
				if(LOGGER.isDebugEnabled()) {
					
					MetricEntityConverterManager metricEntityConverterManager=MetricEntityConverterManager.parseFromName(this.metric);
					
					IMetric imetric=metricEntityConverterManager.parseMetricEntityFromBytes(kvEntity.getV());
					
					MetaConverter metaConverter=new MetaConverter(kvEntity.getK());
					
					LOGGER.debug("metric:{} key:{} metricMeta:{} metricData:{}",this.metric,keyEnity.getValue(),JSONUtils.toJSON(metaConverter.getEntity()),JSONUtils.toJSON(imetric));
				}
			}
			
			long tmpTriggerProcessedNum=this.processedRecordNum.addAndGet(addTimes);
			
			if(isSucc) {
				RocksdbGlobalManager.getInstance().saveProcessedId(this.metric, currentMaxProcessedId);
				RocksdbCleanedGlobalManager.getInstance().addNeedCleanIds(this.metric,processedIdList);
			}else {
				LOGGER.warn("metric:{} isSucc:{} processedId:{} currentMetricId:{} beginKeyId:{}  endKeyId:{} currentMaxProcessedId:{} triggerProcessedNum:{} currentProcessSize:{} failed. try next time!",this.metric,isSucc,processedId,currentMetricId,beginKeyId,endKeyId,currentMaxProcessedId,tmpTriggerProcessedNum,addTimes);
			}
			
			long end=System.currentTimeMillis();
			
			if(fdt%100==0) {
				LOGGER.info("metric:{} fetchDataTimes:{} isSucc:{} processedId:{} currentMetricId:{} beginKeyId:{}  endKeyId:{} currentMaxProcessedId:{} triggerProcessedNum:{} currentProcessSize:{} spendMills:({})",this.metric,fdt,isSucc,processedId,currentMetricId,beginKeyId,endKeyId,currentMaxProcessedId,tmpTriggerProcessedNum,addTimes,(end-begin));
			}
			
			
			if(LOGGER.isDebugEnabled()){
				LOGGER.debug(" transport end... lastKeyId:{} succRecordsNum:{} spendMills:({})",currentMaxProcessedId,succ,(end-begin));
			}
		}  catch (RocksDBException e) {
			LOGGER.error(e.getMessage(),e);
		}
	}

}
