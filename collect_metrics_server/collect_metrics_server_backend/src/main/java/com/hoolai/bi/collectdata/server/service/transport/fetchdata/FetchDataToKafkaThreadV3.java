package com.hoolai.bi.collectdata.server.service.transport.fetchdata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.MutablePair;
import org.corps.bi.dao.rocksdb.MetricRocksdbColumnFamilys;
import org.corps.bi.dao.rocksdb.RocksdbCleanedGlobalManagerV2;
import org.corps.bi.dao.rocksdb.RocksdbGlobalManager;
import org.corps.bi.dao.rocksdb.RocksdbManager;
import org.corps.bi.protobuf.KVEntity;
import org.corps.bi.protobuf.LongEntity;
import org.corps.bi.transport.MetricsTransporterConfig;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hoolai.bi.collectdata.server.service.collect.MetricProcesser;

public class FetchDataToKafkaThreadV3 extends AbstractFetchDataThread {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(FetchDataToKafkaThreadV3.class);

	public FetchDataToKafkaThreadV3(MetricRocksdbColumnFamilys metricRocksdbColumnFamily,
			MetricsTransporterConfig transporterConfig, MutablePair<AtomicLong, AtomicLong> processedRecordNumPair,
			MetricProcesser metricProcesserKafka) {
		super(metricRocksdbColumnFamily, transporterConfig, processedRecordNumPair, metricProcesserKafka);
	}

	@Override
	protected void pollMetrics() {
		try {
			
			long fdt=this.fetchDataTimes.incrementAndGet();
			
			long begin=System.currentTimeMillis();
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
			
			List<byte[]> rowValues=rockdb.multiGetAsList(queryCfList,keys);
			
			if(rowValues==null||rowValues.isEmpty()){
				// 如果beginKeyId到endKeyId，没有值存在，则继续把序号往前推进，一直到当前指标的值（currentMetricId）
				RocksdbGlobalManager.getInstance().saveProcessedId(this.metric, endKeyId);
				LOGGER.warn("metric:{} fetchDataTimes:{} processedId:{} currentMetricId:{} beginKeyId:{}  endKeyId:{} expectValues:{} the key of values is empty!...",this.metric,fdt,processedId,currentMetricId,beginKeyId,endKeyId,(endKeyId-beginKeyId));
				return ;
			}
			
			int addTimes=0;
			int valueIsNullNum=0;
			boolean isSucc=false;
			// 再次设定processedId，如果默认为0，如果后续处理失败，则会重新传输数据
			long succMaxProcessedId=processedId;
			int keySize=keys.size();
			for (int i = 0; i < keySize; i++) {
				
				byte[] value= rowValues.get(i);
				if(value==null) {
					valueIsNullNum++;
					continue;
				}
				
				byte[] key= keys.get(i);
				LongEntity keyEnity=new LongEntity(key);
				
				KVEntity kvEntity=new KVEntity(value);

				isSucc=this.metricProcesserKafka.process(this.metric, kvEntity.getK(), kvEntity.getV());
				
				if(isSucc) {
					if(succMaxProcessedId<keyEnity.getValue()) {
						succMaxProcessedId=keyEnity.getValue();
					}
					addTimes++;
				}else {
					LOGGER.warn("metric:{} fetchDataTimes:{} processedId:{} currentMetricId:{} beginKeyId:{}  endKeyId:{} addTimes:{} forIdx:{} failedKeyId:{} add to kafka  res:{} failed!...",
								this.metric,fdt,processedId,currentMetricId,beginKeyId,endKeyId,addTimes,i,keyEnity.getValue(),isSucc);
					// 如果有任何一个失败了，则之后的重新发送
					break;
				}
			}
			
			long tmpTriggerProcessedNum=this.processedRecordNum.addAndGet(addTimes);
			
			if(isSucc) {
				RocksdbGlobalManager.getInstance().saveProcessedId(this.metric, endKeyId);
				RocksdbCleanedGlobalManagerV2.getInstance().addNeedCleanIds(this.metric,beginKeyId,endKeyId);
			}else {
				// 如果所有值都是NULL，那就正常按照批量KEY的大小推进
				if(valueIsNullNum==keySize) {
					RocksdbGlobalManager.getInstance().saveProcessedId(this.metric, endKeyId);
					LOGGER.warn("metric:{} isSucc:{} processedId:{} currentMetricId:{} beginKeyId:{}  endKeyId:{}  succMaxProcessedId:{} triggerProcessedNum:{} currentProcessSize:{} the value all null. go normal next time!",
							this.metric,isSucc,processedId,currentMetricId,beginKeyId,endKeyId,succMaxProcessedId,tmpTriggerProcessedNum,addTimes);
				}else {
					RocksdbGlobalManager.getInstance().saveProcessedId(this.metric, succMaxProcessedId);
					RocksdbCleanedGlobalManagerV2.getInstance().addNeedCleanIds(this.metric,beginKeyId,succMaxProcessedId);
					
					LOGGER.warn("metric:{} isSucc:{} processedId:{} currentMetricId:{} beginKeyId:{}  endKeyId:{}  succMaxProcessedId:{} triggerProcessedNum:{} currentProcessSize:{} sub failed. try next time!",
								this.metric,isSucc,processedId,currentMetricId,beginKeyId,endKeyId,succMaxProcessedId,tmpTriggerProcessedNum,addTimes);
				}
				
			}
			
			long end=System.currentTimeMillis();
			
			if(fdt%super.metricLoggerPerNum==0||(addTimes>=this.batchSize)) {
				LOGGER.info("metric:{} fetchDataTimes:{} isSucc:{} processedId:{} currentMetricId:{} beginKeyId:{}  endKeyId:{} triggerProcessedNum:{} currentProcessSize:{} spendMills:({})",this.metric,fdt,isSucc,processedId,currentMetricId,beginKeyId,endKeyId,tmpTriggerProcessedNum,addTimes,(end-begin));
			}
			
			
			LOGGER.debug(" transport end... beginKeyId:{}  endKeyId:{} succRecordsNum:{} spendMills:({})",beginKeyId,endKeyId,(end-begin));
			
		}  catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
	}

}
