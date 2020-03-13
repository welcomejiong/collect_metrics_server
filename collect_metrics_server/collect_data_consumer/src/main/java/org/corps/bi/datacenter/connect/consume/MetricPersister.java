package org.corps.bi.datacenter.connect.consume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.corps.bi.datacenter.connect.consume.persister.FilePersister;
import org.corps.bi.datacenter.connect.consume.persister.Persister;
import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.datacenter.connect.core.Constant;
import org.corps.bi.metrics.Meta;
import org.corps.bi.metrics.converter.MetaConverter;
import org.corps.bi.utils.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricPersister {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricPersister.class);
	
	private final AtomicBoolean shutdowned = new AtomicBoolean(false);
	
	private ConnectMetricConfig connectMetricConfig;
	
	private ConcurrentHashMap<String, Persister> partitionPersisterMap;
	
	private long lastCheckExpirePersisteMills=System.currentTimeMillis();

	public MetricPersister(ConnectMetricConfig connectMetricConfig) {
		super();
		this.connectMetricConfig = connectMetricConfig;
		this.partitionPersisterMap=new ConcurrentHashMap<String, Persister>();
	}
	
	
	public boolean persist(ConsumerRecords<byte[], byte[]> records) {
		if(this.shutdowned.get()) {
			return false;
		}
		// 为了防止没有数据，不会触发检测过期的persister,因此需要提前检测
		this.checkExpiredPersister();
		
		if(records==null||records.isEmpty()) {
			LOGGER.warn("metric:{} persist the records size is zero!",this.connectMetricConfig.getTopic());
			return true;
		}
		
		
		Map<String,KV<Meta,List<byte[]>>> partitionDatas=new HashMap<String,KV<Meta,List<byte[]>>>();
		for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
			
			try {
				MetaConverter metaConverter=new MetaConverter(consumerRecord.key());
				Meta meta=metaConverter.getEntity();
				if(StringUtils.isEmpty(meta.getSnId())||StringUtils.isEmpty(meta.getGameId())||StringUtils.isEmpty(meta.getDs())) {
					continue;
				}
				String keyStr=this.generatePartitionKey(metaConverter.getEntity());
				
				KV<Meta,List<byte[]>> tmpKV=null;
				if(partitionDatas.containsKey(keyStr)) {
					tmpKV=partitionDatas.get(keyStr);
				}else {
					tmpKV=new KV<Meta, List<byte[]>>(metaConverter.getEntity(), new ArrayList<byte[]>());
					partitionDatas.put(keyStr, tmpKV);
				}
				tmpKV.getV().add(consumerRecord.value());
			} catch (Exception e) {
				LOGGER.error(e.getMessage(),e);
			}
		}
		
		for (Entry<String,KV<Meta,List<byte[]>>> partitionData : partitionDatas.entrySet()) {
			boolean tmp=this.persistPartitionData(partitionData.getKey(), partitionData.getValue().getK(),partitionData.getValue().getV());
			if(!tmp) {
				return false;
			}
		}
		
		return true;
	}
	
	private boolean persistPartitionData(String keyStr,Meta partitionMeta,List<byte[]> dataList) {
		
		if(!this.partitionPersisterMap.containsKey(keyStr)) {
			this.initPartitionFilePersister(keyStr,partitionMeta);
		}
		Persister persister=this.partitionPersisterMap.get(keyStr);
		
		return persister.persist(connectMetricConfig.getTopic(), dataList);
	}
	
	private synchronized void initPartitionFilePersister(String keyStr,Meta partitionMeta) {
		
		Persister persister=new FilePersister(connectMetricConfig, partitionMeta);
		this.partitionPersisterMap.put(keyStr, persister);
	}
	
	private void checkExpiredPersister() {
		if(!this.isNeedCheckExpired()) {
			return ;
		}
		this.lastCheckExpirePersisteMills=System.currentTimeMillis();
		List<String> removeWapperKeyList=new ArrayList<String>();
		for (Entry<String,Persister> entry : this.partitionPersisterMap.entrySet()) {
			Persister tmPersister=entry.getValue();
			if(tmPersister.isExpired()) {
				removeWapperKeyList.add(entry.getKey());
			}
		}
		if(removeWapperKeyList.isEmpty()) {
			return ;
		}
		for (String key : removeWapperKeyList) {
			Persister persister=this.partitionPersisterMap.get(key);
			persister.close();
			this.partitionPersisterMap.remove(key);
		}
	}
	
	private boolean isNeedCheckExpired() {
		return System.currentTimeMillis()-this.lastCheckExpirePersisteMills>=Constant.THIRTY_MINUTES_MILLS;
	}
	
	private String generatePartitionKey(Meta partitionMeta) {
		StringBuilder sb=new StringBuilder(partitionMeta.getMetric());
		sb.append("_").append(partitionMeta.getSnId());
		sb.append("_").append(partitionMeta.getGameId());
		sb.append("_").append(partitionMeta.getDs());
		return sb.toString();
	}
	
	public void shutdown() {
		if(this.shutdowned.compareAndSet(false, true)) {
			for (Entry<String,Persister> entry : this.partitionPersisterMap.entrySet()) {
				Persister tmPersister=entry.getValue();
				tmPersister.close();
			}
		}
	}
	

}
