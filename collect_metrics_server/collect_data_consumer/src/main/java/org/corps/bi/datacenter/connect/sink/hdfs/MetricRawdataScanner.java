package org.corps.bi.datacenter.connect.sink.hdfs;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.corps.bi.datacenter.connect.core.ConnectMetricConfig;
import org.corps.bi.metrics.Meta;
import org.corps.bi.utils.KV;

public class MetricRawdataScanner{
	
	private final ConnectMetricConfig connectMetricConfig;
	
	public MetricRawdataScanner(ConnectMetricConfig connectMetricConfig) {
		super();
		this.connectMetricConfig = connectMetricConfig;
	}

	public Map<String,KV<Meta,List<File>>> scan() {
		File[] bufferFile=new File(this.connectMetricConfig.getDataDir()).listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if(name.contains(".rowdata")){
					return true;
				}
				return false;
			}
		});
		if(bufferFile==null){
			return Collections.emptyMap();
		}
		Map<String,KV<Meta,List<File>>> metricPartitionFileMap=new HashMap<String,KV<Meta,List<File>>>(); 
		for (File file : bufferFile) {
			String partitionFields[]=file.getName().split("_");
			String key="";
			for (int i = 0; i < 4; i++) {
				key+=partitionFields[i]+"_";
			}
			KV<Meta,List<File>> tmpKV=null;
			if(metricPartitionFileMap.containsKey(key)) {
				tmpKV=metricPartitionFileMap.get(key);
			}else {
				Meta meta=new Meta();
				meta.setMetric(partitionFields[0]);
				//meta.setSnId("-1");
				meta.setSnId(partitionFields[1]);
				meta.setGameId(partitionFields[2]);
				meta.setDs(partitionFields[3]);
				tmpKV=new KV<Meta,List<File>>(meta,new ArrayList<File>());
				metricPartitionFileMap.put(key,tmpKV );
			}
			tmpKV.getV().add(file);
		}
		return metricPartitionFileMap;
	}
}
