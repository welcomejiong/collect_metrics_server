package org.corps.bi.datacenter.connect.sink.hdfs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.datacenter.connect.core.Constant;
import org.corps.bi.datacenter.connect.hive.MetricMessDao;
import org.corps.bi.datacenter.connect.sink.SinkService;
import org.corps.bi.datacenter.connect.utils.BISystemThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HdfsSinkServiceImpl implements SinkService {
	
	@Autowired
	private MetricMessDao metricMessDao;
	
	private final ScheduledExecutorService triggerService;
	
	private final Map<String,MetricRawdataToHdfsTrigger> runnableTriggers=new HashMap<String,MetricRawdataToHdfsTrigger>();
	
	public HdfsSinkServiceImpl() {
		super();
		triggerService = Executors.newScheduledThreadPool(Constant.getInstance().getMetricSinkTriggerThreadSize(),
				new BISystemThreadFactory("bi-hdfs-sink-trigger-service"));
		
	}


	@Override
	public void sink(List<ConnectMetricConfig> connectMetricConfigs) {
		for (ConnectMetricConfig connectMetricConfig : connectMetricConfigs) {
			MetricRawdataToHdfsTrigger metricRawdataToHdfsTrigger=new MetricRawdataToHdfsTrigger(connectMetricConfig,this.metricMessDao);
			int rotateInterval=connectMetricConfig.getMetricSinkTriggeRrotateIntervalSeconds();
			this.triggerService.scheduleAtFixedRate(metricRawdataToHdfsTrigger, rotateInterval, rotateInterval, TimeUnit.SECONDS);
			runnableTriggers.put(connectMetricConfig.getName(), metricRawdataToHdfsTrigger);
		}
	}


	@Override
	public void shutdown() {
		
		this.triggerService.shutdown();
		
		for (Entry<String,MetricRawdataToHdfsTrigger> entry: this.runnableTriggers.entrySet()) {
			entry.getValue().shutdown();
		}
		
	}

}
