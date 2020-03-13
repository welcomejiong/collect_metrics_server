package org.corps.bi.datacenter.connect.hive;

import java.util.Map;

public interface MetricMessDao {
	
	void addPartition(String table,Map<String,String> paritionsMap);

}
