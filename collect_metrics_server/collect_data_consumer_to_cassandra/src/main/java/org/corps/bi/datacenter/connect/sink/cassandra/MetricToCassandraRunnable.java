package org.corps.bi.datacenter.connect.sink.cassandra;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.corps.bi.datacenter.connect.core.MetricLoggerControl;
import org.corps.bi.metrics.converter.MetaConverter;
import org.corps.bi.metrics.converter.MetricEntityConverterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.GeneratedMessageV3;

public class MetricToCassandraRunnable implements Callable<Boolean>{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricToCassandraRunnable.class);
	
	private static final int PER_EXECUTE_BATCH_SIZE=20;
	
	private final String topic;
	
	private final List<ConsumerRecord<byte[], byte[]>> sinkRecords;
	
	private final Session session;
	
	private final PreparedStatement preparedStatement;
	
	private final AtomicLong perpistedRecordNum;
	
	private final MetricLoggerControl metricLoggerControl;
	
	public MetricToCassandraRunnable(String topic, Session session,PreparedStatement preparedStatement,AtomicLong perpistedRecordNum,List<ConsumerRecord<byte[], byte[]>> sinkRecords) {
		super();
		this.topic=topic;
		this.sinkRecords = sinkRecords;
		this.session = session;
		this.preparedStatement=preparedStatement;
		this.perpistedRecordNum=perpistedRecordNum;
		this.metricLoggerControl=MetricLoggerControl.parseFromName(topic);
	}

	@Override
	public Boolean call() throws Exception {
		int currentSize=0;
		try {
			BatchStatement batchStatement=new BatchStatement();
			for (ConsumerRecord<byte[], byte[]> sinkRecord : this.sinkRecords) {
				Statement statement=this.generateStatement(sinkRecord);	
				if(statement==null) {
					continue;
				}
				batchStatement.add(statement);
				currentSize=batchStatement.size();
				if(currentSize%PER_EXECUTE_BATCH_SIZE==0) {
					this.doExcute(batchStatement);
				}
			}
			
			this.doExcute(batchStatement);
			
			return true;
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage()+" topic:"+this.topic+" currentSize:"+currentSize,e);
			return false;
		}
		
	}
	
	private void doExcute(BatchStatement batchStatement) {
		if(batchStatement==null||batchStatement.size()<=0) {
			return ;
		}
		this.session.execute(batchStatement);
		long tmp=this.perpistedRecordNum.addAndGet(batchStatement.size());
		if(tmp%metricLoggerControl.getPerNum()==0) {
			LOGGER.info("topic:{} perpistedRecordNum:{} totalBatchSize:{} currentBatchSize:{} logCtlNum:{} to db...",this.topic,tmp,this.sinkRecords.size(),batchStatement.size(),this.metricLoggerControl.getPerNum());
		}
		batchStatement.clear();
	}
	
	
	
	private Statement generateStatement(ConsumerRecord<byte[], byte[]> sinkRecord) {
		
		String topic=sinkRecord.topic();
		
		MetricEntityConverterManager metricEntityConverterManager=MetricEntityConverterManager.parseFromName(topic);
		
		if(metricEntityConverterManager==null) {
			LOGGER.error("topic:{} is not match MetricEntityConverterManager",this.topic);
			return null;
		}
		
		MetaConverter metaConverter=new MetaConverter(sinkRecord.key());
		
		Object value=sinkRecord.value();
		
		GeneratedMessageV3 metricProto=metricEntityConverterManager.parseProtoFromBytes((byte[])value);
			
		
		BoundStatement boundStatement= this.preparedStatement.bind();
		
		MetricCassandraCqls topicCassandraCqls=MetricCassandraCqls.parseFromName(this.topic);
		
		if(topicCassandraCqls==null) {
			LOGGER.error("topic:{} is not match TopicCassandraCqls",this.topic);
			return null;
		}
		
		boundStatement.setString("snId", metaConverter.getEntity().getSnId());
		boundStatement.setString("gameId", metaConverter.getEntity().getGameId());
		boundStatement.setString("ds", metaConverter.getEntity().getDs());
		
		List<FieldDescriptor> feildDescriptors=topicCassandraCqls.getProtoFieldDescriptor();
		
		for (FieldDescriptor fieldDescriptor : feildDescriptors) {
			Object fieldValue=metricProto.getField(fieldDescriptor);
			this.bindValue(boundStatement, fieldDescriptor.getName(),fieldDescriptor.getType(), fieldValue);
		}
		
		
		return boundStatement;
	}
	
	private void bindValue(BoundStatement boundStatement,String name,Type valueType,Object value) {
		if(Type.BOOL.equals(valueType)) {
			boundStatement.setBool(name, (Boolean)value);
		}else if(Type.INT32.equals(valueType)) {
			boundStatement.setInt(name, (Integer)value);
		}else if(Type.INT64.equals(valueType)) {
			boundStatement.setLong(name, (Long)value);
		}else if(Type.FLOAT.equals(valueType)) {
			boundStatement.setFloat(name,(Float)value);
		}else if(Type.DOUBLE.equals(valueType)) {
			boundStatement.setDouble(name, (Double)value);
		}else if(Type.STRING.equals(valueType)) {
			boundStatement.setString(name, (String)value);
		}else if(Type.BYTES.equals(valueType)) {
			boundStatement.setBytes(name, ByteBuffer.wrap((byte[])value));
		}else {
			throw new RuntimeException("the instance fieldName:"+name+" valueType:"+valueType+" value:"+value +" unsupported!");
		}
	}

	
	
	
}