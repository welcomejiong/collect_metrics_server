package org.corps.bi.datacenter.connect.consume.persister;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.datacenter.connect.core.Constant;
import org.corps.bi.metrics.Meta;
import org.corps.bi.protobuf.BytesList;
import org.corps.bi.protobuf.common.SimpleListProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileOperator {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(FileOperator.class);
	
	private final ConnectMetricConfig connectMetricConfig;
	
	private final Meta partitionMeta;
	
	private final String filePrefix;
	
	private final File currentFile;
	
	private final long createTimeMills;
	
	private final boolean appendNewline;
	
	// 换行符
	private final byte[] lineTerminateBy;
	
	private final OutputStream outputStream;
	
	private long writeBytes;
	
	private final AtomicBoolean shouldRotate;
	
	private final AtomicBoolean destoryed;
	/**
	 * 是否正在持久化
	 */
	private final AtomicBoolean isPersistenting;
	
	/**
	 * 什么时候开始正式接收数据，进行数据的持久化
	 */
	private long beginPerpistMills=0l;
	
	public FileOperator(ConnectMetricConfig connectMetricConfig,Meta partitionMeta,String filePrefix) {
		super();
		try {
			this.connectMetricConfig=connectMetricConfig;
			this.partitionMeta=partitionMeta;
			this.filePrefix=filePrefix;
			this.createTimeMills=System.currentTimeMillis();
			this.appendNewline=this.connectMetricConfig.isAppendNewline();
			this.lineTerminateBy=this.connectMetricConfig.getLineTerminateBy().getBytes(Constant.DEFAULT_CHARSET);
			this.shouldRotate=new AtomicBoolean(false);
			this.destoryed=new AtomicBoolean(false);
			this.isPersistenting=new AtomicBoolean(false);
		
			//文件名增加一个随机数，防止多个项目采集数据时生成相同的文件名
			StringBuilder targetFilePath=new StringBuilder(this.connectMetricConfig.getDataDir());
			targetFilePath.append(File.separator);
			targetFilePath.append(this.partitionMeta.getMetric()).append("_").append(this.partitionMeta.getSnId()).append("_");
			targetFilePath.append(this.partitionMeta.getGameId()).append("_").append(this.partitionMeta.getDs()).append("_");
			targetFilePath.append(this.filePrefix).append(".proto.buffer");
			
			File targetFile=new File(targetFilePath.toString());
			if(!targetFile.getParentFile().exists()) {
				targetFile.getParentFile().mkdirs();
			}
			if(!targetFile.exists()){
				targetFile.createNewFile();
				LOGGER.info("threadId:{} createNewFile for path:{}",Thread.currentThread().getId(),targetFile.getPath());
			}else{
				LOGGER.info("instance output for path:{}",targetFile.getPath());
			}
			
			this.currentFile=targetFile;
			this.outputStream = new BufferedOutputStream(new FileOutputStream(targetFile,true));
			this.writeBytes=0l;
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
			throw new RuntimeException("initOutput meet exception"+e.getMessage());
		}
	}
	
	/**
	 * 重命名为flume认识的log文件名
	 */
	private void renameCurrentFileToFlumeLog(){
		if(this.currentFile==null){
			return ;
		}
		String currPath=this.currentFile.getPath();
		String fileNameSubfix=currPath.substring(0, currPath.lastIndexOf("."));
		File dest = new File(fileNameSubfix + ".rowdata");
		boolean renamed = this.currentFile.renameTo(dest);
	    if (renamed) {
	    	LOGGER.info("Successfully rolled file {} to {}",this.currentFile.getPath(),dest.getPath());
	    }
	    /**
	     * 重命名把当前实例的所有置空
	     */
	    this.writeBytes=0l;
	}
	

	public boolean append(String content) {
		try {
			byte[] bytes=content.getBytes(Constant.DEFAULT_CHARSET);
			if(bytes==null){
				return false;
			}
			
			return this.append(bytes);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
		return false;
	}
	
	public boolean append(byte[] bytes) {
		if(this.destoryed.get()) {
			return false;
		}
		if(bytes==null) {
			return true;
		}
		if(!this.isCanReceiveContent()){
			return false;
		}
		boolean allowPersistent=this.applyPersistent();
		try {
			if(!allowPersistent){
				LOGGER.debug("{} isExpire:{} createTime:{} is persistenting ...",this.currentFile.getAbsolutePath(),this.isExpire(),this.createTimeMills);
				return false;
			}
			
			this.checkBeginPersistMills();
			
			BytesList bytesList=new BytesList();
			bytesList.add(bytes);
			//this.outputStream.write(bytes);
			SimpleListProto simpleListProto=bytesList.copyTo();
			simpleListProto.writeDelimitedTo(this.outputStream);
			this.writeBytes+=simpleListProto.getSerializedSize();
			/*
			if (this.appendNewline && this.lineTerminateBy!=null) {
				this.outputStream.write(this.lineTerminateBy);
			}
			*/
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}finally{
			if(allowPersistent){
				this.relievePersistent();
			}
		}
		return false;
	}
	
	public boolean append(List<byte[]> datas) {
		if(this.destoryed.get()) {
			return false;
		}
		if(datas==null||datas.isEmpty()){
			// 不需要处理所以返回true
			return true;
		}
		if(!this.isCanReceiveContent()){
			return false;
		}
		boolean allowPersistent=this.applyPersistent();
		try {
			if(!allowPersistent){
				LOGGER.debug("{} isExpire:{} createTime:{} is persistenting ...",this.currentFile.getAbsolutePath(),this.isExpire(),this.createTimeMills);
				return false;
			}
			
			this.checkBeginPersistMills();
			
			BytesList bytesList=new BytesList();
			bytesList.addAll(datas);
			SimpleListProto simpleListProto=bytesList.copyTo();
			simpleListProto.writeDelimitedTo(this.outputStream);
			this.writeBytes+=simpleListProto.getSerializedSize();
			/*
			for (byte[] bytes : datas) {
				if(bytes==null){
					return false;
				}
				this.outputStream.write(bytes);
				this.writeBytes+=bytes.length;
				if (this.appendNewline && this.lineTerminateBy!=null) {
					this.outputStream.write(this.lineTerminateBy);
				}
			}
			*/
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}finally{
			if(allowPersistent){
				this.relievePersistent();
			}
		}
		return false;
	}
	/**
	 * 第一次接收到数据，初始化开始持久化的时间
	 */
	private void checkBeginPersistMills() {
		if(this.beginPerpistMills==0l) {
			this.beginPerpistMills=System.currentTimeMillis();
		}
	}
	
	public void flush(){
		try {
			this.outputStream.flush();
			LOGGER.info("{} flush....",this.currentFile.getAbsolutePath());
			
		} catch (IOException e) {
			LOGGER.error(e.getMessage(),e);
		}
	}
	
	private void close(){
		try {
			int waitTimes=0;
			int sleepMills=500;
			while(this.isPersistenting()) {
				TimeUnit.MILLISECONDS.sleep(sleepMills);
				waitTimes++;
				LOGGER.info("waiting the file:{} for times:{} alreadyWaitMills:{} ",this.currentFile.getAbsoluteFile(),waitTimes,(waitTimes*sleepMills));
				// 最多等10秒
				if(waitTimes>20) {
					break;
				}
			}
			this.flush();
			this.outputStream.close();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
		LOGGER.info("{} close....",this.currentFile.getAbsolutePath());
	}
	
	public void destory(){
		boolean succ=this.destoryed.compareAndSet(false, true);
		if(!succ){
			LOGGER.info("triggerDestory:current instance have already destory!");
			return ;
		}
		//2：关系流
		this.close();
		// 重命名文件
		this.renameCurrentFileToFlumeLog();
	}
	
	/**
	 * 通知需要切换文件
	 * @return
	 */
	public boolean rotate(){
		return this.shouldRotate.compareAndSet(false, true);
	}
	/**
	 * 修改不需奥切换文件
	 * @return
	 */
	public boolean unRotate(){
		return this.shouldRotate.compareAndSet(true, false);
	}
	
	public boolean isNeedRotate(){
		if((this.isExpire()||this.isReachedMaxFileSize())&&this.writeBytes>0){
			return true;
		}
		return false;
	}
	
	public boolean isReachedMaxFileSize(){
		return this.writeBytes>=this.connectMetricConfig.getMaxDataFileSize();
	}
	
	/**
	 * 是否过期，默认在内存中存放不超过一天
	 * @return
	 */
	public boolean isExpire(){
		return (System.currentTimeMillis()-this.beginPerpistMills)/1000>=this.connectMetricConfig.getRotateInterval();
		//return (System.currentTimeMillis()-this.createTimeMills)/1000>=this.connectMetricConfig.getRotateInterval();
	}
	
	private boolean isCanReceiveContent(){
		return !this.shouldRotate.get();
	}
	
	/**
	 * 申请持久化
	 * @return
	 */
	private boolean applyPersistent(){
		return this.isPersistenting.compareAndSet(false, true);
	}
	/**
	 * 解除持久化
	 * @return
	 */
	private boolean relievePersistent(){
		return this.isPersistenting.compareAndSet(true, false);
	}
	/**
	 * 是否正在持久化
	 * @return
	 */
	public boolean isPersistenting(){
		return this.isPersistenting.get();
	}
}
