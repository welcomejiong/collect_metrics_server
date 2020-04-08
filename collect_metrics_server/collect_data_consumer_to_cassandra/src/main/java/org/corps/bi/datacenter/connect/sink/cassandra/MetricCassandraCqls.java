package org.corps.bi.datacenter.connect.sink.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.corps.bi.metrics.protobuf.AdTrackingProto;
import org.corps.bi.metrics.protobuf.CounterProto;
import org.corps.bi.metrics.protobuf.CustomBinaryBodyMetricProto;
import org.corps.bi.metrics.protobuf.DauProto;
import org.corps.bi.metrics.protobuf.EconomyProto;
import org.corps.bi.metrics.protobuf.GameInfoProto;
import org.corps.bi.metrics.protobuf.InstallProto;
import org.corps.bi.metrics.protobuf.MilestoneProto;
import org.corps.bi.metrics.protobuf.PaymentProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessageV3;

public enum MetricCassandraCqls {
	
	DAU("dau") {

		@Override
		public Class<? extends GeneratedMessageV3> getProtoClass() {
			return DauProto.class;
		}

		@Override
		public Descriptor getProtoDescriptor() {
			return DauProto.getDescriptor();
		}
		
	},
	
	INSTALL("install") {
		@Override
		public Class<? extends GeneratedMessageV3> getProtoClass() {
			return InstallProto.class;
		}
		
		@Override
		public Descriptor getProtoDescriptor() {
			return InstallProto.getDescriptor();
		}

	},
	
	COUNTER("counter") {
		@Override
		public Class<? extends GeneratedMessageV3> getProtoClass() {
			return CounterProto.class;
		}
		
		@Override
		public Descriptor getProtoDescriptor() {
			return CounterProto.getDescriptor();
		}
	},
	
	ECONOMY("economy") {
		@Override
		public Class<? extends GeneratedMessageV3> getProtoClass() {
			return EconomyProto.class;
		}
		
		@Override
		public Descriptor getProtoDescriptor() {
			return EconomyProto.getDescriptor();
		}
	},
	
	GAMEINFO("gameinfo") {
		@Override
		public Class<? extends GeneratedMessageV3> getProtoClass() {
			return GameInfoProto.class;
		}
		
		@Override
		public Descriptor getProtoDescriptor() {
			return GameInfoProto.getDescriptor();
		}
	},
	
	MILESTONE("milestone") {
		@Override
		public Class<? extends GeneratedMessageV3> getProtoClass() {
			return MilestoneProto.class;
		}
		
		@Override
		public Descriptor getProtoDescriptor() {
			return MilestoneProto.getDescriptor();
		}
	},
	
	PAYMENT("payment") {
		@Override
		public Class<? extends GeneratedMessageV3> getProtoClass() {
			return PaymentProto.class;
		}
		
		@Override
		public Descriptor getProtoDescriptor() {
			return PaymentProto.getDescriptor();
		}
	},
	
	ADTRACKING("adtracking") {
		@Override
		public Class<? extends GeneratedMessageV3> getProtoClass() {
			return AdTrackingProto.class;
		}
		
		@Override
		public Descriptor getProtoDescriptor() {
			return AdTrackingProto.getDescriptor();
		}
	},
	
	CUSTOMBINARYBODYMETRIC("custombinarybodymetric") {
		@Override
		public Class<? extends GeneratedMessageV3> getProtoClass() {
			return CustomBinaryBodyMetricProto.class;
		}
		
		@Override
		public Descriptor getProtoDescriptor() {
			return CustomBinaryBodyMetricProto.getDescriptor();
		}
	};
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricCassandraCqls.class);
	
	private static final Map<String,MetricCassandraCqls> TOPIC_METRIC_MAP=new HashMap<String,MetricCassandraCqls>();
	
	private static final Map<String,String> TOPIC_CQL_MAP=new HashMap<String,String>();
	
	private static final ConcurrentHashMap<String, List<Descriptors.FieldDescriptor>> METRIC_PROTO_FIELD_DESCRIPTORS=new ConcurrentHashMap<String, List<FieldDescriptor>>();
	
	private final String metric;
	
	public List<FieldDescriptor> getProtoFieldDescriptor(){
		String metric=this.getMetric();
		if(!METRIC_PROTO_FIELD_DESCRIPTORS.containsKey(metric)) {
			METRIC_PROTO_FIELD_DESCRIPTORS.put(metric, this.getProtoDescriptor().getFields());
		}
		return METRIC_PROTO_FIELD_DESCRIPTORS.get(metric);
	}
	
	public abstract Class<? extends GeneratedMessageV3> getProtoClass();
	
	public abstract Descriptor getProtoDescriptor();
	
	public  String getCassandraCql() {
		if(TOPIC_CQL_MAP.containsKey(this.getMetric())) {
			return TOPIC_CQL_MAP.get(this.getMetric());
		}
		final List<Descriptors.FieldDescriptor> fieldDescriptorList = this.getProtoFieldDescriptor();
		
		StringBuilder sb=new StringBuilder("insert into ");
		sb.append(this.metric).append("(");
		
		sb.append("snId,gameId,ds,");
		
		int fieldSize=fieldDescriptorList.size();
		
		for(int i=0;i<fieldSize;i++) {
			FieldDescriptor fieldDescriptor=fieldDescriptorList.get(i);
			sb.append(fieldDescriptor.getName());
			if(i!=fieldSize-1) {
				sb.append(",");
			}
		}
		sb.append(") values (");
		sb.append(":snId,:gameId,:ds,");
		for(int i=0;i<fieldSize;i++) {
			FieldDescriptor fieldDescriptor=fieldDescriptorList.get(i);
			sb.append(":");
			sb.append(fieldDescriptor.getName());
			if(i!=fieldSize-1) {
				sb.append(",");
			}
		}
		sb.append(")");
		String cql=sb.toString();
		TOPIC_CQL_MAP.put(this.getMetric(), cql);
		
		LOGGER.info(cql);
		
		return cql;
	}

	private MetricCassandraCqls(String metric) {
		this.metric = metric;
	}

	public String getMetric() {
		return metric;
	}
	
	static {
		for (MetricCassandraCqls dataCenterTopic : MetricCassandraCqls.values()) {
			TOPIC_METRIC_MAP.put(dataCenterTopic.metric, dataCenterTopic);
		}
	}
	
	public static MetricCassandraCqls parseFromName(String metric) {
		if(TOPIC_METRIC_MAP.containsKey(metric)) {
			return TOPIC_METRIC_MAP.get(metric);
		}
		return null;
	}
	
	public static void main(String[] args) {
		Descriptor descriptor=CounterProto.getDescriptor();
		List<FieldDescriptor> fields=descriptor.getFields();
		for (FieldDescriptor fieldDescriptor : fields) {
			System.out.println(fieldDescriptor.getFullName()+":"+fieldDescriptor.getName());
		}
		
		for (MetricCassandraCqls cql : MetricCassandraCqls.values()) {
			System.out.println(cql.getCassandraCql());
		}
	}

}
