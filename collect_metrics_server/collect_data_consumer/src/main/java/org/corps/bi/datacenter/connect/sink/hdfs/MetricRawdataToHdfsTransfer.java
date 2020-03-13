package org.corps.bi.datacenter.connect.sink.hdfs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.datacenter.connect.sink.SinkTransfer;
import org.corps.bi.metrics.Meta;
import org.corps.bi.metrics.converter.MetricEntityConverterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessageV3;

public class MetricRawdataToHdfsTransfer implements SinkTransfer{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricRawdataToHdfsTransfer.class);
	
	public static final CompressionCodecName COMPRESSION_CODEC_NAME = CompressionCodecName.SNAPPY;
	
    public static final int BLOCK_SIZE = 256 * 1024 * 1024;
    
    public static final int PAGE_SIZE = 64 * 1024;
    
    private static final ConcurrentHashMap<String, List<Descriptors.FieldDescriptor>> METRIC_PROTO_FIELD_DESCRIPTORS=new ConcurrentHashMap<String, List<FieldDescriptor>>();
	
	private final AtomicBoolean started = new AtomicBoolean(false);
	
	private final ConnectMetricConfig connectMetricConfig;
	
	private final Meta partitionMeta;
	
	private final Configuration conf;
	
	private final FileSystem fs;
	
	private final String combinePrefix;
	
	private int combineIdx=0;
	
	private Path currentCombinePath;
	
	private int currentAlreadyWriteRecordNum;
	
	private long currentAlreadyWriteSize;
	
	private Schema recordSchema;
	
	private ParquetWriter<GenericData.Record> currentCombineParquetWriter;
	
	private List<Path> presubmitCombinePaths=new ArrayList<Path>();

	public MetricRawdataToHdfsTransfer(ConnectMetricConfig connectMetricConfig, Meta partitionMeta) throws Exception {
		super();
		this.connectMetricConfig = connectMetricConfig;
		this.partitionMeta = partitionMeta;
		this.conf=new Configuration();
		this.fs= FileSystem.get(new URI(connectMetricConfig.getWarehousePath()), this.conf);  
		this.combinePrefix=this.partitionMeta.getMetric()+"_"+DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
	}

	@Override
	public boolean start() {
		
		try {
			
			if(this.started.get()) {
				LOGGER.warn("already started...");
				return false;
			}
			
			this.initSchema();
			
			this.rollNext();
			
			if(!this.started.compareAndSet(false, true)) {
				return false;
			}
			
			return true;
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
			return false;
		}
		
		
	}
	
	@Override
	public boolean stop() {
		
		return this.started.compareAndSet(true, false);
		
	}

	@Override
	public void flush() {
		
		LOGGER.warn(this.currentCombineParquetWriter.getClass().getName()+" is unsupport flush.");
		
	}

	@Override
	public void close() {
		
		try {
			this.stop();
			
			this.closeCombineWriter();
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
		
	}
	
	private void initSchema() throws Exception {
		String schemaPath="metricsSchemas"+File.separator+this.partitionMeta.getMetric().toUpperCase()+"_RECORD_SCHEMA.json";
		InputStream is=MetricRawdataToHdfsTransfer.class.getClassLoader().getResourceAsStream(schemaPath);
		String schemaContent=IOUtils.toString(is);
		this.recordSchema=new Schema.Parser().parse(schemaContent);
	}

	@Override
	public boolean append(byte[] metricData) {
		try {
			if(!this.started.get()) {
				LOGGER.error("metric:{} snid:{} gameid:{} ds:{} is not start.",this.partitionMeta.getMetric(),this.partitionMeta.getSnId(),this.partitionMeta.getGameId(),this.partitionMeta.getDs());
				throw new RuntimeException("metric transfer is not start")  ;
			}
			MetricEntityConverterManager metricEntityConvert=MetricEntityConverterManager.parseFromName(this.partitionMeta.getMetric());
			if(metricEntityConvert==null) {
				LOGGER.warn("MetricEntityConverterManager metric:"+this.partitionMeta.getMetric()+" is not match converter.");
				return false;
			}
			GeneratedMessageV3 generatedMessageV3=metricEntityConvert.parseProtoFromBytes(metricData);
			GenericData.Record record=this.parseRecordFormProto(generatedMessageV3);
			return this.writeToCombineFile(record);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
		return false;
	}
	
	private GenericData.Record parseRecordFormProto(GeneratedMessageV3 generatedMessageV3){
		GenericData.Record ret=new GenericData.Record(this.recordSchema);
		final List<Descriptors.FieldDescriptor> fieldDescriptorList = this.getProtoFieldDescriptor(generatedMessageV3);
	    for (Descriptors.FieldDescriptor descriptor : fieldDescriptorList) {
	    	Object fieldValue=generatedMessageV3.getField(descriptor);
	    	ret.put(descriptor.getName(), fieldValue);
	    }
		return ret;
	}
	
	private List<Descriptors.FieldDescriptor> getProtoFieldDescriptor(GeneratedMessageV3 generatedMessageV3){
		String protoClassName=generatedMessageV3.getClass().getName();
		if(!METRIC_PROTO_FIELD_DESCRIPTORS.containsKey(protoClassName)) {
			METRIC_PROTO_FIELD_DESCRIPTORS.put(protoClassName, generatedMessageV3.getDescriptorForType().getFields());
		}
		return METRIC_PROTO_FIELD_DESCRIPTORS.get(protoClassName);
	}
	
	private synchronized boolean writeToCombineFile(GenericData.Record record) throws Exception {
		
		if(record==null) {
			return false;
		}
		
		this.currentCombineParquetWriter.write(record);
		this.currentAlreadyWriteRecordNum++;
		this.currentAlreadyWriteSize=this.currentCombineParquetWriter.getDataSize();
		
		if(this.currentCombineParquetWriter.getDataSize()>=this.connectMetricConfig.getMaxCombineDataFileSize()) {
			this.rollNext();
		}
		
		
		return true;
	}
	
	private String generateMetricPartitionPath() {
		StringBuilder sb=new StringBuilder(this.partitionMeta.getMetric());
		sb.append(File.separator).append("snid=").append(this.partitionMeta.getSnId());
		sb.append(File.separator).append("gameid=").append(this.partitionMeta.getGameId());
		sb.append(File.separator).append("ds=").append(this.partitionMeta.getDs());
		
		return sb.toString();
	}
	
	private synchronized void rollNext() throws Exception {
		
		Path combineFilePath=null;
		
		try {
			
			this.closeCombineWriter();
			
			this.currentAlreadyWriteRecordNum=0;
			this.currentAlreadyWriteSize=0;
			
			combineFilePath=new Path(new URI(this.connectMetricConfig.getWarehousePath()+File.separator+this.generateMetricPartitionPath()+File.separator+"."+this.combinePrefix+"_"+(this.combineIdx++)+".parquet.tmp"));
			if(this.fs.exists(combineFilePath)) {
				this.fs.delete(combineFilePath,false);
			}else {
				if(!this.fs.exists(combineFilePath.getParent())) {
					this.fs.mkdirs(combineFilePath.getParent());
				}
			}
			
			AvroParquetWriter.Builder<GenericData.Record> writeBuilder=AvroParquetWriter.<GenericData.Record>builder(combineFilePath)
					.withSchema(this.recordSchema)
			        .withCompressionCodec(COMPRESSION_CODEC_NAME)
			        .withRowGroupSize(BLOCK_SIZE)
			        .withPageSize(PAGE_SIZE)
			        .withDictionaryEncoding(true)
			        .withConf(this.conf)
			        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE);
			
			this.currentCombineParquetWriter=writeBuilder.build();
			this.currentCombinePath= combineFilePath;
		} catch (Exception e) {
			LOGGER.error("roll next path:{},meet error:{}",combineFilePath!=null?combineFilePath.toUri().getPath():"NULL",e.getMessage());
			throw e;
		}
	}
	
	private void closeCombineWriter() {
		try {
			if(this.currentCombineParquetWriter!=null) {
				this.currentCombineParquetWriter.close();
				String tmpCombinePath=this.currentCombinePath.getName();
				String renameCombineName=this.currentCombinePath.getName().substring(0, tmpCombinePath.lastIndexOf(".parquet.tmp"))+"_"+this.currentAlreadyWriteRecordNum+"_"+this.currentAlreadyWriteSize+".parquet.presubmit";
				Path presubmitCombinePath=new  Path(this.currentCombinePath.getParent(),renameCombineName);
				this.fs.rename(this.currentCombinePath, presubmitCombinePath);
				this.presubmitCombinePaths.add(presubmitCombinePath);
				LOGGER.info("rename the tmpFile:{}  to presubmitFile:{} succ",this.currentCombinePath.toUri().getPath(),presubmitCombinePath.toUri().getPath());
			}
		} catch (IOException e) {
			LOGGER.error("close current path:{},meet error:{}",this.currentCombinePath!=null?currentCombinePath.toUri().getPath():"NULL",e.getMessage());
		}
	}
	
	public void submit() {
		for (Path presubmitCombinePath: presubmitCombinePaths) {
			this.doSubmitPath(presubmitCombinePath);
		}
	}
	
	private void doSubmitPath(Path presubmitCombinePath) {
		
		try {
			String tmpCombinePath=presubmitCombinePath.getName();
			String renameCombineName=presubmitCombinePath.getName().substring(1, tmpCombinePath.lastIndexOf(".presubmit"));
			Path submitCombinePath=new  Path(presubmitCombinePath.getParent(),renameCombineName);
			this.fs.rename(presubmitCombinePath, submitCombinePath);
			LOGGER.info("submit: rename presubmitFile:{}  to submitFile:{} succ",presubmitCombinePath.toUri().getPath(),submitCombinePath.toUri().getPath());
		} catch (IOException e) {
			LOGGER.error(e.getMessage(),e);
		}
	}

}
