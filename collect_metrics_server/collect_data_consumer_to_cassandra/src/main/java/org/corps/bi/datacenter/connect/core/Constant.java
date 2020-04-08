package org.corps.bi.datacenter.connect.core;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.corps.db.cassandra.core.CassandraConfig;
import org.yaml.snakeyaml.Yaml;

public class Constant {

	public static final String DEFAULT_FORMAT_DATE_DAY="yyyy-MM-dd";
	
	public static final String DEFAULT_FORMAT_DATE_MINUTES="yyyy-MM-dd HH:mm";
	
	public static final String DEFAULT_FORMAT_DATE_SECONDS="yyyy-MM-dd HH:mm:ss";
	
	public static final String DEFAULT_CHARSET="UTF-8";
	
	public final static long ONE_DAY_MILLS=24*60*60*1000;
	
	public final static long ONE_HOUR_MILLS=1*60*60*1000;
	
	// 30分钟毫秒数
	public final static long THIRTY_MINUTES_MILLS=30*60*1000;
	
	public static final int DEFAULT_INDEX_ZEAO = 0;
	
	public static final int DEFAULT_INDEX_FIRST= 1;
	
	// 默认long
	public static long DEFAULT_LONG_ZEAO = 0;
	
	// 默认long
	public static long DEFAULT_LONG_FIRST = 1;
	
	// 默认分隔符
	public static final String DEFAULT_SPLIT = ",";

	// 默认空格
	public static final String DEFAULT_BLANK = " ";

	// 默认下划线
	public static final String DEFAULT_UNDERLINE = "_";

	// 默认斜线
	public static final String DEFAULT_SPRIT = "/";
	
	public static final String CONSTANT_GLOBLE_FILE_PATH = "constant_globle.properties";
	
	private static Constant INSTANTCE;
	
	public static String CONNECT_METRICS_CONFIG_PATH;
	
	public static String KAFKA_CONSUMES_CONFIG_PATH;
	
	private static boolean isInited=false;
	
	private String kafkaConsumerConfigFilePath;
	
	private int metricConsumeThreadCoreSize;
	
	private int metricConsumeThreadMaxPoolSize;
	
	private int metricSinkTriggerThreadSize;
	
	private CassandraConfig cassandraConfig;
	
	private Constant() {
		super();
	}

	public static Constant getInstance(){
		if(INSTANTCE==null){
			throw new RuntimeException("the config is not inited!");
		}
		return INSTANTCE;
	}

	public static void init(){
		init(null);
	}
	
	public static  void init(String constantFilePathIn){
		if(isInited){
			System.out.println("constant instance is already inited!");
			return ;
		}
		try {
			InputStream globleIn = Constant.class.getClassLoader().getResourceAsStream(CONSTANT_GLOBLE_FILE_PATH);
			Properties globleProperties = new Properties();
			if(globleIn==null){
				throw new RuntimeException("constant_globle.properties is not exists!");
			}
			globleProperties.load(globleIn);
			
			// 项目本事的常量文件
			String constantFilePath=globleProperties.getProperty("constant_file_path");
			InputStream constantIn=Constant.class.getClassLoader().getResourceAsStream(constantFilePath);
			if(StringUtils.isNotEmpty(constantFilePathIn)){
				constantFilePath=constantFilePathIn;
				constantIn = new FileInputStream(constantFilePathIn);
			}
			
			processProjectConstant(constantIn);
			
			CONNECT_METRICS_CONFIG_PATH=globleProperties.getProperty("connect_metrics_config_path");
			
			KAFKA_CONSUMES_CONFIG_PATH=globleProperties.getProperty("kafka_consumes_config_path");
			
			isInited=true;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private  static void processProjectConstant(InputStream constantIn) throws Exception{
		Yaml yaml=new Yaml();
		INSTANTCE=(Constant)yaml.load(constantIn);
	}
	
	public String getKafkaConsumerConfigFilePath() {
		return kafkaConsumerConfigFilePath;
	}

	public void setKafkaConsumerConfigFilePath(String kafkaConsumerConfigFilePath) {
		this.kafkaConsumerConfigFilePath = kafkaConsumerConfigFilePath;
	}

	public int getMetricConsumeThreadCoreSize() {
		return metricConsumeThreadCoreSize;
	}

	public void setMetricConsumeThreadCoreSize(int metricConsumeThreadCoreSize) {
		this.metricConsumeThreadCoreSize = metricConsumeThreadCoreSize;
	}

	public int getMetricConsumeThreadMaxPoolSize() {
		return metricConsumeThreadMaxPoolSize;
	}

	public void setMetricConsumeThreadMaxPoolSize(int metricConsumeThreadMaxPoolSize) {
		this.metricConsumeThreadMaxPoolSize = metricConsumeThreadMaxPoolSize;
	}

	public int getMetricSinkTriggerThreadSize() {
		return metricSinkTriggerThreadSize;
	}

	public void setMetricSinkTriggerThreadSize(int metricSinkTriggerThreadSize) {
		this.metricSinkTriggerThreadSize = metricSinkTriggerThreadSize;
	}

	public CassandraConfig getCassandraConfig() {
		return cassandraConfig;
	}

	public void setCassandraConfig(CassandraConfig cassandraConfig) {
		this.cassandraConfig = cassandraConfig;
	}

	public static void main(String[] args) {
		Constant constant=new Constant();
		constant.init(null);
		System.out.println(Constant.getInstance().kafkaConsumerConfigFilePath);
	}
	
}
