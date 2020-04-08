package org.corps.bi.datacenter.connect;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.corps.bi.datacenter.connect.consume.ConsumerService;
import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.datacenter.connect.core.Constant;
import org.corps.bi.datacenter.core.DataCenterTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.yaml.snakeyaml.Yaml;

public class ConnectToCassandraMain {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(ConnectToCassandraMain.class);
	
	private ApplicationContext applicationContext;
	
	private List<ConnectMetricConfig> connectMetricConfigs;
	
	public ConnectToCassandraMain() {
		super();
		this.init();
		this.applicationContext=new ClassPathXmlApplicationContext("spring/appContext.xml");
	}
	
	private void init() {
		try {
			Constant.init();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
	}
	
	private void start(String metricDayPaths) throws Exception {
		
		this.connectMetricConfigs=this.parseConsumeMetrics();
		
		ConsumerService consumerService=this.applicationContext.getBean(ConsumerService.class);
		
		if(consumerService!=null) {
			consumerService.consume(Collections.unmodifiableList(this.connectMetricConfigs));
		}
		
	}
	
	private List<ConnectMetricConfig> parseConsumeMetrics() {
		try {
			List<ConnectMetricConfig> ret=new ArrayList<ConnectMetricConfig>();
			for (DataCenterTopics topic : DataCenterTopics.values()) {
				Yaml yaml=new Yaml();
				InputStream is=this.getClass().getClassLoader().getResourceAsStream(Constant.CONNECT_METRICS_CONFIG_PATH+File.separator+"connect_hdfs-"+topic.getMetric()+".yaml");
				if(is==null) {
					continue;
				}
				ConnectMetricConfig connectMetricConfig=(ConnectMetricConfig)yaml.load(is);
				// 修复由于重启造成的未命名为rawdata的文件
				connectMetricConfig.repairDataFile();
				ret.add(connectMetricConfig);
			}
			return ret;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
		return Collections.emptyList();
	}
	
	private static  Options generateOptions() {
		Options options=new Options();
		
		options.addOption("metricDayPaths",true, ",split for diff metricDayPaths");

		return options;
	}
	
	public void shutdownHookCallback() {
		
		ConsumerService consumerService=this.applicationContext.getBean(ConsumerService.class);
		
		if(consumerService!=null) {
			consumerService.shutdown();
		}
		
	}
	
	public static class ShutdownHookThread extends Thread{
		
		private static final Logger LOGGER=LoggerFactory.getLogger(ShutdownHookThread.class);
		
		private final ConnectToCassandraMain combineMain;

		public ShutdownHookThread(ConnectToCassandraMain combineMain) {
			super();
			this.combineMain = combineMain;
		}

		@Override
		public void run() {
			long begin=System.currentTimeMillis();
			LOGGER.info("shutdown hook callback begin....");
			super.run();
			this.combineMain.shutdownHookCallback();
			LOGGER.info("shutdown hook callback end. spendMills:{}",(System.currentTimeMillis()-begin));
		}
		
	}
	

	public static void main(String[] args) throws Exception {
		
		CommandLineParser parser = new DefaultParser();
		
		Options options=generateOptions();

		CommandLine commandLine=parser.parse(options, args);
		
		String metricDayPaths=commandLine.getOptionValue("metricDayPaths","autoScan");
			
		final ConnectToCassandraMain combineMain=new ConnectToCassandraMain();
		
		combineMain.start(metricDayPaths);
		
		Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(combineMain));
		
	}
	
}
