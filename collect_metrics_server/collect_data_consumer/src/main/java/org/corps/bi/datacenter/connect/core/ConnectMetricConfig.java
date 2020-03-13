package org.corps.bi.datacenter.connect.core;

import java.io.File;
import java.io.FilenameFilter;

import org.corps.bi.datacenter.connect.consume.MetricTopicConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectMetricConfig {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricTopicConsumer.class);
	
	private String name;
	
	private String topic;
	
	private  String dataDir;
	
	private  int rotateInterval;
	
	private  int maxDataFileSize;
	
	private  boolean appendNewline;
	
	private  String lineTerminateBy;
	
	private String hdfsUrl;
	
	private String warehousePath;
	
	private int maxCombineDataFileSize;
	
	private int metricSinkTriggeRrotateIntervalSeconds;
	
	private int metricTransferThreadCoreSize;
	
	private int metricTransferThreadMaxSize;
	
	private boolean hiveIntegration;

	public ConnectMetricConfig() {
		super();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getDataDir() {
		return dataDir;
	}

	public void setDataDir(String dataDir) {
		this.dataDir = dataDir;
	}

	public int getRotateInterval() {
		return rotateInterval;
	}

	public void setRotateInterval(int rotateInterval) {
		this.rotateInterval = rotateInterval;
	}

	public int getMaxDataFileSize() {
		return maxDataFileSize;
	}

	public void setMaxDataFileSize(int maxDataFileSize) {
		this.maxDataFileSize = maxDataFileSize;
	}

	public boolean isAppendNewline() {
		return appendNewline;
	}

	public void setAppendNewline(boolean appendNewline) {
		this.appendNewline = appendNewline;
	}

	public String getLineTerminateBy() {
		return lineTerminateBy;
	}

	public void setLineTerminateBy(String lineTerminateBy) {
		this.lineTerminateBy = lineTerminateBy;
	}
	
	
	public String getHdfsUrl() {
		return hdfsUrl;
	}

	public void setHdfsUrl(String hdfsUrl) {
		this.hdfsUrl = hdfsUrl;
	}

	public String getWarehousePath() {
		return warehousePath;
	}

	public void setWarehousePath(String warehousePath) {
		this.warehousePath = warehousePath;
	}

	public int getMaxCombineDataFileSize() {
		return maxCombineDataFileSize;
	}

	public void setMaxCombineDataFileSize(int maxCombineDataFileSize) {
		this.maxCombineDataFileSize = maxCombineDataFileSize;
	}

	public int getMetricSinkTriggeRrotateIntervalSeconds() {
		return metricSinkTriggeRrotateIntervalSeconds;
	}

	public void setMetricSinkTriggeRrotateIntervalSeconds(int metricSinkTriggeRrotateIntervalSeconds) {
		this.metricSinkTriggeRrotateIntervalSeconds = metricSinkTriggeRrotateIntervalSeconds;
	}

	public int getMetricTransferThreadCoreSize() {
		return metricTransferThreadCoreSize;
	}

	public void setMetricTransferThreadCoreSize(int metricTransferThreadCoreSize) {
		this.metricTransferThreadCoreSize = metricTransferThreadCoreSize;
	}

	public int getMetricTransferThreadMaxSize() {
		return metricTransferThreadMaxSize;
	}

	public void setMetricTransferThreadMaxSize(int metricTransferThreadMaxSize) {
		this.metricTransferThreadMaxSize = metricTransferThreadMaxSize;
	}

	public boolean isHiveIntegration() {
		return hiveIntegration;
	}

	public void setHiveIntegration(boolean hiveIntegration) {
		this.hiveIntegration = hiveIntegration;
	}

	/**
	 * 由于重启等原因，造成.buffer文件没有正确切换为.rowdata文件
	 * 一台机器上部署多个实例，则必须创建不同的目录
	 */
	public void repairDataFile(){
		File[] bufferFile=new File(this.dataDir).listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if(name.contains(".buffer")){
					return true;
				}
				return false;
			}
		});
		if(bufferFile==null){
			return ;
		}
		for (File file : bufferFile) {
			this.renameToRowData(file);
		}
	}
	
	private void renameToRowData(File file){
		try {
			String currPath=file.getPath();
			String fileNameSubfix=currPath.substring(0, currPath.lastIndexOf("."));
			File dest = new File(fileNameSubfix + ".rowdata");
			boolean renamed = file.renameTo(dest);
			if (renamed) {
				LOGGER.debug("Successfully rolled file:{} to :{}",file.getPath(),dest.getPath());
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
	

}
