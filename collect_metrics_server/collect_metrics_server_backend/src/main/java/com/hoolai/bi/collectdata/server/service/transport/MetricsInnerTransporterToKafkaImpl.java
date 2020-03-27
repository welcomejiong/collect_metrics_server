package com.hoolai.bi.collectdata.server.service.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.corps.bi.recording.clients.rollfile.RollFileClient.SystemThreadFactory;
import org.corps.bi.recording.exception.TrackingException;
import org.corps.bi.tools.util.JSONUtils;
import org.corps.bi.transport.MetricsInnerTransporter;
import org.corps.bi.transport.MetricsTransporterConfig;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hoolai.bi.collectdata.server.core.Constant;
import com.hoolai.bi.collectdata.server.service.collect.MetricProcesser;
import com.hoolai.bi.collectdata.server.service.collect.MetricProcesserKafkaImpl;

public class MetricsInnerTransporterToKafkaImpl implements MetricsInnerTransporter {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricsInnerTransporterToKafkaImpl.class);
	
	private AtomicBoolean isTransporting=new AtomicBoolean(false);
	
	private  MetricsTransporterConfig transporterConfig;
	
	private  ScheduledExecutorService transporterIntervalService;
	
	private  TriggerThread triggerThread;
	
	private  MetricProcesser metricProcesserKafka;
	
	public MetricsInnerTransporterToKafkaImpl() {
		super();
		this.transporterConfig=MetricsTransporterConfig.getInstance();
		
	}
	
	private synchronized boolean start() {
		if(!this.isTransporting.compareAndSet(false, true)) {
			LOGGER.warn("is transporting now!");
			return false;
		}
		int rollInterval = this.transporterConfig.getTransportInterval();
		if (rollInterval <= 0) {
			throw new TrackingException("RollInterval is error !");
		}
		
		if(Constant.IS_FINAL_DATACENTER) {
			this.metricProcesserKafka=new MetricProcesserKafkaImpl();
		}
		
		transporterIntervalService = Executors.newScheduledThreadPool(1,new SystemThreadFactory("metric-inner-transporter-scheduled"));
		
		this.triggerThread=new TriggerThread(this.transporterConfig,this.metricProcesserKafka);
		
		this.transporterIntervalService.scheduleAtFixedRate(triggerThread,rollInterval, rollInterval, TimeUnit.MILLISECONDS);
		
		return true;
	}
	
	private class TriggerThread implements Runnable{
		
		private final ThreadPoolExecutor threadPoolExecutor;
		
		private final MetricsTransporterConfig transporterConfig;
		
		private  final MetricProcesser metricProcesserKafka;
		
		private final Map<String,MutablePair<AtomicLong, AtomicLong>> metricProcessedRecordNumMap=new ConcurrentHashMap<String, MutablePair<AtomicLong, AtomicLong>>();
		
		
		public TriggerThread(MetricsTransporterConfig transporterConfig,MetricProcesser metricProcesserKafka) {
			super();
			this.transporterConfig=transporterConfig;
			this.metricProcesserKafka=metricProcesserKafka;
			this.threadPoolExecutor = new ThreadPoolExecutor(
					this.transporterConfig.getThreadCoreSize()>1?(this.transporterConfig.getThreadCoreSize()-1):1,		//指的是保留的线程池大小
					this.transporterConfig.getMaxThreadSize(), 	//最大线程池， 指的是线程池的最大大小
					100, 	//指的是空闲线程结束的超时时间
					TimeUnit.SECONDS, 	//表示 keepAliveTime 的单位
					new LinkedBlockingQueue<Runnable>(100000),
					new SystemThreadFactory("metric-inner-transporter-executor"),
					new ThreadPoolExecutor.DiscardPolicy() //直接放弃当前任务
			);
		}

		@Override
		public void run() {
			try {
				if(!this.transporterConfig.isTransportOn()) {
					LOGGER.warn("the config properties is_tranpsort_on was updated by hand through the controller or other.");
					return ;
				}
				for (MetricRocksdbColumnFamilys metricRocksdbColumnFamily : MetricRocksdbColumnFamilys.values()) {
					this.processMetric(metricRocksdbColumnFamily);
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(),e);
			}
		}
		
		private void processMetric(MetricRocksdbColumnFamilys metricRocksdbColumnFamily) {
			
			String metric=metricRocksdbColumnFamily.getMetric();
			
			MutablePair<AtomicLong, AtomicLong> processedRecordNumPair=this.getMetricProcessedRecordNum(metric);
			
			this.threadPoolExecutor.submit(new FetchDataThread(metricRocksdbColumnFamily,this.transporterConfig,processedRecordNumPair,this.metricProcesserKafka));
		}
		
		private MutablePair<AtomicLong, AtomicLong> getMetricProcessedRecordNum(String metric) {
			if(!this.metricProcessedRecordNumMap.containsKey(metric)) {
				// left:处理次数  right:处理的记录数
				MutablePair<AtomicLong, AtomicLong> mutablePair=new MutablePair<AtomicLong, AtomicLong>(new AtomicLong(0),new AtomicLong(0));
				this.metricProcessedRecordNumMap.put(metric, mutablePair);
			}
			
			return this.metricProcessedRecordNumMap.get(metric);
		}
		
		public boolean shutdown() {
			try {
				this.threadPoolExecutor.shutdown();
				return true;
			} catch (Exception e) {
				LOGGER.error(e.getMessage(),e);
				return false;
			}
			
		}
		
	}

	
	private class FetchDataThread implements Runnable{
		
		private final MetricRocksdbColumnFamilys metricRocksdbColumnFamily;
		
		private final MetricsTransporterConfig transporterConfig;
		
		private final String metric;
		
		private final int batchSize;
		
		private final AtomicLong fetchDataTimes;
		
		private final AtomicLong processedRecordNum;
		
		private  final MetricProcesser metricProcesserKafka;

		public FetchDataThread(final MetricRocksdbColumnFamilys metricRocksdbColumnFamily,final MetricsTransporterConfig transporterConfig,final MutablePair<AtomicLong, AtomicLong> processedRecordNumPair,final MetricProcesser metricProcesserKafka) {
			super();
			this.metricRocksdbColumnFamily=metricRocksdbColumnFamily;
			this.transporterConfig=transporterConfig;
			this.fetchDataTimes=processedRecordNumPair.getLeft();
			this.processedRecordNum=processedRecordNumPair.getRight();
			this.metric=this.metricRocksdbColumnFamily.getMetric();
			this.batchSize = this.transporterConfig.getBatchSize();
			this.metricProcesserKafka=metricProcesserKafka;
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
				this.pollMetricsV2();
			} catch (Exception e) {
				LOGGER.error(e.getMessage(),e);
			}finally {
				if(isLock) {
					RocksdbGlobalManager.getInstance().unLockProcessed(this.metric);
				}
			}
		}
		
		/**
		 * 通过自增id的模式，批量获取，至少可以保证接收到的顺序和发出的顺序是一致的
		 */
		private void pollMetricsV2(){
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


	@Override
	public boolean transport() {
		if(!this.transporterConfig.isTransportOn()) {
			LOGGER.info("transport the trigger of tracking on is off.please config the transprot.on to true");
			return false;
		}
		return this.start();
	}

	@Override
	public boolean shutdown() {
		try {
			if(!this.isTransporting.compareAndSet(true, false)) {
				this.isTransporting.set(false);
				LOGGER.warn("there is the other thread set transporting to true.");
				return false;
			}
			this.transporterIntervalService.shutdown();
			this.triggerThread.shutdown();
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
			return false;
		}
	}


	

}
