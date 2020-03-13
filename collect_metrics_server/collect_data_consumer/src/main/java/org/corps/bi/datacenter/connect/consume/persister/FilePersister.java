package org.corps.bi.datacenter.connect.consume.persister;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.datacenter.connect.core.Constant;
import org.corps.bi.metrics.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePersister  implements Persister{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(FilePersister.class);
	
	private final AtomicBoolean closed = new AtomicBoolean(false);
	
	private final long createTimeMills;
	
	private final AtomicInteger fileIndex;
	
	private final ConnectMetricConfig connectMetricConfig;
	
	private final Meta partitionMeta;
	
	private FileOperator currentFileOperator;
	
	private FileOperator nextFileOperator;
	
	private AtomicLong lastActiveTimeMills=new AtomicLong(System.currentTimeMillis());

	public FilePersister(ConnectMetricConfig connectMetricConfig,Meta partitionMeta) {
		super();
		this.connectMetricConfig=connectMetricConfig;
		this.partitionMeta=partitionMeta;
		this.createTimeMills=System.currentTimeMillis();
		this.fileIndex=new AtomicInteger(0);
		
		this.initFileOperator();
		
	}
	
	private  void initFileOperator(){
		this.currentFileOperator=new FileOperator(this.connectMetricConfig,this.partitionMeta,this.getNextPersistFilePrefic());
		this.nextFileOperator=new FileOperator(this.connectMetricConfig,this.partitionMeta,this.getNextPersistFilePrefic());
	}
	
	private String getNextPersistFilePrefic() {
		String prefix=Thread.currentThread().getId()+"_"+this.createTimeMills+"_"+this.fileIndex.incrementAndGet();
		return prefix;
	}
	
	
	private synchronized void checkFileOperator(){
		if(!this.currentFileOperator.isNeedRotate()){
			return ;
		}
		this.currentFileOperator.rotate();
		this.destoryCurrentFileOperator();
		this.exchangeCurrentFileOperator();
	}
	
	private void destoryCurrentFileOperator(){
		//this.currentFileOperator.flush();
		this.currentFileOperator.destory();
	}
	
	private synchronized void exchangeCurrentFileOperator(){
		this.currentFileOperator=this.nextFileOperator;
		this.nextFileOperator=new FileOperator(this.connectMetricConfig,this.partitionMeta,this.getNextPersistFilePrefic());
	}

	@Override
	public boolean persist(String metric,List<byte[]> metricDatas) {
		if(this.closed.get()) {
			return false;
		}
		this.active();
		this.checkFileOperator();
		return this.currentFileOperator.append(metricDatas);
	}

	@Override
	public boolean persist(String metric,byte[] metricData) {
		if(this.closed.get()) {
			return false;
		}
		this.active();
		this.checkFileOperator();
		return this.currentFileOperator.append(metricData);
	}
	
	private void active() {
		this.lastActiveTimeMills.set(System.currentTimeMillis());
	}

	@Override
	public boolean isExpired() {
		return System.currentTimeMillis()-this.lastActiveTimeMills.get()>=Constant.THIRTY_MINUTES_MILLS;
	}

	@Override
	public void flush() {
		this.currentFileOperator.flush();
	}

	@Override
	public void close() {
		try {
			if(this.closed.compareAndSet(false, true)) {
				this.currentFileOperator.destory();
				this.nextFileOperator.destory();
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
	}
	
	

}
