package org.corps.bi.datacenter.connect.sink;

public interface SinkTransfer {
	
	boolean start();
	
	boolean stop();
	
	void flush();
	
	void close();
	
	boolean append(byte[] metricData);
	
	

}
