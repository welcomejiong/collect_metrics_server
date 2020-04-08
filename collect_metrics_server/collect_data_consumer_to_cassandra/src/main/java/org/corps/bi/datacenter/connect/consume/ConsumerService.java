package org.corps.bi.datacenter.connect.consume;

import java.util.List;

import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;

public interface ConsumerService {
	
	void consume(List<ConnectMetricConfig> connectMetricConfigs)throws Exception;
	
	void shutdown();

}
