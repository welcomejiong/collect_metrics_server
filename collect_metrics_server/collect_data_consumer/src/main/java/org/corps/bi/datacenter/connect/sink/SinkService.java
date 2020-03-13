package org.corps.bi.datacenter.connect.sink;

import java.util.List;

import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;

public interface SinkService {
	
	void sink(List<ConnectMetricConfig> connectMetricConfigs);
	
	void shutdown();

}
