package org.corps.bi.datacenter.connect.sink.hdfs;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.datacenter.connect.hive.MetricMessDao;
import org.corps.bi.datacenter.connect.utils.BISystemThreadFactory;
import org.corps.bi.metrics.Meta;
import org.corps.bi.utils.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricRawdataToHdfsTrigger implements Runnable{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricRawdataToHdfsTrigger.class);
	
	private static final long MAX_COMBINE_TIMEOUT_MINUTES=30;
	
	private final AtomicBoolean shutdowned = new AtomicBoolean(false);
	
	private final AtomicBoolean combing = new AtomicBoolean(false);
	
	private long lastCombineMills=0;
	
	private final ConnectMetricConfig connectMetricConfig;
	
	private final MetricMessDao metricMessDao;
	
	private ThreadPoolExecutor threadPoolExecutor;
	
	public MetricRawdataToHdfsTrigger(ConnectMetricConfig connectMetricConfig,MetricMessDao metricMessDao) {
		super();
		this.connectMetricConfig = connectMetricConfig;
		this.metricMessDao=metricMessDao;
		this.threadPoolExecutor = new ThreadPoolExecutor(this.connectMetricConfig.getMetricTransferThreadCoreSize(),		//指的是保留的线程池大小
				this.connectMetricConfig.getMetricTransferThreadMaxSize(), 	//最大线程池， 指的是线程池的最大大小
				100, 	//指的是空闲线程结束的超时时间
				TimeUnit.SECONDS, 	//表示 keepAliveTime 的单位
				new LinkedBlockingQueue<Runnable>(100),
				new BISystemThreadFactory("BI-datacener-tohdfs-trigger"),
				new ThreadPoolExecutor.CallerRunsPolicy() //直接放弃当前任务
		);
	}

	@Override
	public void run() {
		
		try {
			
			if(this.combing.compareAndSet(false, true)) {
				this.lastCombineMills=System.currentTimeMillis();
				MetricRawdataScanner metricRawdataScanner=new MetricRawdataScanner(connectMetricConfig);
				
				Map<String,KV<Meta,List<File>>> metricPartitionMap=metricRawdataScanner.scan();
				
				this.doCombine(metricPartitionMap);
			}
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
	}
	
	private void doCombine(Map<String,KV<Meta,List<File>>> metricPartitionMap) {
		
		try {
			
			if(this.shutdowned.get()) {
				return ;
			}
			
			if(metricPartitionMap==null||metricPartitionMap.size()<=0) {
				return ;
			}
			
			CountDownLatch countDownLatch=new CountDownLatch(metricPartitionMap.size());
			
			for (Entry<String,KV<Meta,List<File>>> entry : metricPartitionMap.entrySet()) {
				try {
					this.checkHivePartition(entry.getValue().getK());
					MetricRawdataCombiner metricRawdataCombiner=new MetricRawdataCombiner(connectMetricConfig,countDownLatch, entry.getValue().getK(), entry.getValue().getV());
					this.threadPoolExecutor.submit(metricRawdataCombiner);
				} catch (Exception e) {
					LOGGER.error(e.getMessage(),e);
				}
			}
			
			countDownLatch.await(MAX_COMBINE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}finally{
			this.combing.compareAndSet(true, false);
		}
	}
	
	private void checkHivePartition(Meta partitionMeta) {
		if(!this.connectMetricConfig.isHiveIntegration()) {
			return ;
		}
		Map<String,String> partitionMessMap=new HashMap<String, String>();
		partitionMessMap.put("snid", partitionMeta.getSnId());
		partitionMessMap.put("gameid", partitionMeta.getGameId());
		partitionMessMap.put("ds", partitionMeta.getDs());
		this.metricMessDao.addPartition(partitionMeta.getMetric(), partitionMessMap);
	}
	
	
	public void shutdown() {
		
		try {
			if(this.shutdowned.compareAndSet(false, true)) {
				/**
				 * 此处有待权衡
				 * 1:由于此处是跟hdfs交互，所以会相对比较的慢
				 * 2:如果其中一个combiner需要一次处理的分区文件比较的多而且大，在线程内部，会有先提交成功一部分，后提交一部分的情况，因此如果刚好在期间，则有可能会造成数据重复，不是有可能，应该是一定
				 */
				this.threadPoolExecutor.shutdown();
				int waitTimes=0;
				int sleepMills=1000;
				while(this.combing.get()) {
					TimeUnit.MILLISECONDS.sleep(sleepMills);
					waitTimes++;
					LOGGER.info("waiting the metric:{} for times:{} alreadyWaitMills:{} ",this.connectMetricConfig.getTopic(),waitTimes,(waitTimes*sleepMills));
					// 最多等30秒
					if(waitTimes>30) {
						break;
					}
				}
				if(!this.threadPoolExecutor.isTerminated()) {
					this.threadPoolExecutor.shutdownNow();
				}
			}
			
			LOGGER.info("metric:{} dataDir:{} is combing:{} will be shutdown" ,this.connectMetricConfig.getTopic(),this.connectMetricConfig.getDataDir(),this.combing.get());
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
	
	}

}
