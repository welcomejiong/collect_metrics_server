package org.corps.bi.datacenter.connect.consume.persister;

import java.util.List;

public interface Persister {
	
	boolean persist(String metric,List<byte[]> metricDatas);
	
	boolean persist(String metric,byte[] metricData);
	
	boolean isExpired();
	
	void flush();
	
	void close();
	

}
