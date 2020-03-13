package org.corps.bi.datacenter.connect.consume;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.datacenter.connect.core.Constant;
import org.corps.bi.datacenter.connect.utils.BISystemThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerServiceImpl implements ConsumerService{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(KafkaConsumerServiceImpl.class);
	
	private final ThreadPoolExecutor threadPoolExecutor;
	
	private final Map<String,MetricTopicConsumer> runnableMetricTopicConsumers=new HashMap<String,MetricTopicConsumer>();
	
	public KafkaConsumerServiceImpl() {
		super();
		this.threadPoolExecutor = new ThreadPoolExecutor(Constant.getInstance().getMetricConsumeThreadCoreSize(),		//指的是保留的线程池大小
				Constant.getInstance().getMetricConsumeThreadMaxPoolSize(), 	//最大线程池， 指的是线程池的最大大小
				100, 	//指的是空闲线程结束的超时时间
				TimeUnit.SECONDS, 	//表示 keepAliveTime 的单位
				new LinkedBlockingQueue<Runnable>(100),
				new BISystemThreadFactory("BI-datacener-metric-consume"),
				new ThreadPoolExecutor.CallerRunsPolicy() //直接放弃当前任务
		);
		
		this.init();
	}
	
	private void init() {
		try {
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
	}
	
	public void consume(List<ConnectMetricConfig> connectMetricConfigs) throws Exception {
		
		for (ConnectMetricConfig connectMetricConfig : connectMetricConfigs) {
			MetricTopicConsumer metricTopicConsumer=this.doConsume(connectMetricConfig);
			this.threadPoolExecutor.submit(metricTopicConsumer);
			runnableMetricTopicConsumers.put(connectMetricConfig.getName(), metricTopicConsumer);
		}
		
	}
	
	private MetricTopicConsumer doConsume(ConnectMetricConfig connectMetricConfig) throws Exception {
		String metricConfPath=Constant.KAFKA_CONSUMES_CONFIG_PATH+File.separator+"kafka_consumer-"+connectMetricConfig.getTopic()+".properties";
		InputStream globleIn = Constant.class.getClassLoader().getResourceAsStream(metricConfPath);
		Properties metricProperties = new Properties();
		if(globleIn==null){
			throw new RuntimeException("constant_globle.properties is not exists!");
		}
		metricProperties.load(globleIn);
		return new MetricTopicConsumer(connectMetricConfig,metricProperties);
	}

	@Override
	public void shutdown() {
		
		try {
			this.threadPoolExecutor.shutdown();
			
			for (Entry<String,MetricTopicConsumer> entry : this.runnableMetricTopicConsumers.entrySet()) {
				entry.getValue().shutdown();
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
		
	}
	
	
	
}
