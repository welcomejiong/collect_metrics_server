package org.corps.bi.datacenter.connect.sink.hdfs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;
import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.metrics.Meta;
import org.corps.bi.protobuf.BytesList;
import org.corps.bi.protobuf.common.SimpleListProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricRawdataCombiner implements Runnable{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricRawdataCombiner.class);
	
	private final ConnectMetricConfig connectMetricConfig;
	
	private final CountDownLatch countDownLatch;
	
	private final Meta partitionMeta;
	
	private final List<File> partitionFiles;
	
	private MetricRawdataToHdfsTransfer metricRawdataToHdfsTransfer;
	
	public MetricRawdataCombiner(ConnectMetricConfig connectMetricConfig,CountDownLatch countDownLatch, Meta partitionMeta,
			List<File> partitionFiles) throws Exception {
		super();
		this.connectMetricConfig = connectMetricConfig;
		this.countDownLatch=countDownLatch;
		this.partitionMeta = partitionMeta;
		this.partitionFiles = partitionFiles;
		this.metricRawdataToHdfsTransfer=new MetricRawdataToHdfsTransfer(connectMetricConfig, partitionMeta);
		this.metricRawdataToHdfsTransfer.start();
	}

	@Override
	public void run() {
		try {
			
			if(this.partitionFiles==null||this.partitionFiles.isEmpty()) {
				return ;
			}
			
			long begin=System.currentTimeMillis();
			
			int recordSum=0;
			List<File> succFileList=new ArrayList<File>();
			List<File> failFileList=new ArrayList<File>();
			for (File file : partitionFiles) {
				if(FileUtils.sizeOf(file)==0) {
					succFileList.add(file);
					continue;
				}
				int tmpNum=this.readRawdataFile(file);
				if(tmpNum>=0) {
					recordSum+=tmpNum;
					succFileList.add(file);
				}else {
					failFileList.add(file);
				}
			}
			this.metricRawdataToHdfsTransfer.close();
			this.metricRawdataToHdfsTransfer.submit();
			
			this.processTransferSuccFile(succFileList);
			this.processTransferFailFile(failFileList);
			LOGGER.info("metric:{} snid:{} gameid:{} ds:{} batchFiles:{} success processTotalRecordsNum:{} spendMills:{}",this.partitionMeta.getMetric(),this.partitionMeta.getSnId(),this.partitionMeta.getGameId(),this.partitionMeta.getDs(),this.partitionFiles.size(),recordSum,(System.currentTimeMillis()-begin));
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}finally {
			this.countDownLatch.countDown();
			
		}
		
	}
	
	
	
	private int readRawdataFile(File file) {
		try {
			
			if(file==null) {
				return 0;
			}
			
			long begin=System.currentTimeMillis();
			
			InputStream inputStream=FileUtils.openInputStream(file);
			
			SimpleListProto simpleListProto=SimpleListProto.parseDelimitedFrom(inputStream);
			
			this.processRawdata(simpleListProto);
			
			int count=1;
			
			while(inputStream.available()!=0) {
				
				try {
					simpleListProto=SimpleListProto.parseDelimitedFrom(inputStream);
					
					if(simpleListProto==null||simpleListProto.getElementsCount()<=0) {
						continue;
					}
					
					count+=this.processRawdata(simpleListProto);
					
				} catch (Exception e) {
					LOGGER.error(e.getMessage(),e);
				}
			}
			LOGGER.info("the fileName:{} metric:{} snid:{} gameid:{} ds:{} processed success processRecordsNum:{} spendMills:{}",file.getName(),this.partitionMeta.getMetric(),this.partitionMeta.getSnId(),this.partitionMeta.getGameId(),this.partitionMeta.getDs(),count,(System.currentTimeMillis()-begin));
			return count;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
		return -1;
		
	}
	
	private int processRawdata(SimpleListProto simpleListProto) {
		if(simpleListProto==null||simpleListProto.getElementsCount()<=0) {
			return 0;
		}
		BytesList bytesList=new BytesList();
		bytesList.copyFrom(simpleListProto);
		if(bytesList.size()<=0) {
			return 0;
		}
		int nums=0;
		for (byte[] bs : bytesList) {
			boolean tmp=this.processMetricData(bs);
			if(tmp) {
				nums++;
			}
		}
		return nums;
	}
	
	private boolean processMetricData(byte[] metricData) {
		
		return this.metricRawdataToHdfsTransfer.append(metricData);
		
	}
	
	private void processTransferSuccFile(List<File> files) {
		if(files==null||files.isEmpty()) {
			return ;
		}
		File succDirFile=new File(this.connectMetricConfig.getDataDir()+File.separator+"succed"+File.separator+this.partitionMeta.getMetric()+"_"+this.partitionMeta.getSnId()+"_"+this.partitionMeta.getGameId()+File.separator+this.partitionMeta.getDs());
		if(!succDirFile.exists()) {
			succDirFile.mkdirs();
		}
		for (File file : files) {
			try {
				FileUtils.moveFileToDirectory(file, succDirFile, true);
			}catch(FileExistsException fee) {
				this.processFileConflict(file, succDirFile);
			}catch (Exception e) {
				LOGGER.error(e.getMessage(),e);
			}
		}
	}
	
	private void processFileConflict(File conflictFile,File targetDir) {
		try {
			int split=conflictFile.getName().indexOf(".");
			File destFile=new File(targetDir,conflictFile.getName().substring(0, split)+"_"+System.currentTimeMillis()+conflictFile.getName().substring(split));
			FileUtils.moveFile(conflictFile, destFile);
		} catch (IOException e) {
			LOGGER.error(e.getMessage(),e);
		}
	}
	
	private void processTransferFailFile(List<File> files) {
		if(files==null||files.isEmpty()) {
			return ;
		}
		File failedDirFile=new File(this.connectMetricConfig.getDataDir()+File.separator+"failed"+File.separator+this.partitionMeta.getMetric()+"_"+this.partitionMeta.getSnId()+"_"+this.partitionMeta.getGameId()+File.separator+this.partitionMeta.getDs());
		if(!failedDirFile.exists()) {
			failedDirFile.mkdirs();
		}
		for (File file : files) {
			try {
				FileUtils.moveFileToDirectory(file, failedDirFile, true);
			} catch (IOException e) {
				LOGGER.error(e.getMessage(),e);
			}
		}
	}

}
