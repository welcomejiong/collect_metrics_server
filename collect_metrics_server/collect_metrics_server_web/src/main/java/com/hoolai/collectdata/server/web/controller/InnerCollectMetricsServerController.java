package com.hoolai.collectdata.server.web.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.corps.bi.core.MetricLoggerControl;
import org.corps.bi.dao.rocksdb.MetricRocksdbColumnFamilys;
import org.corps.bi.dao.rocksdb.RocksdbGlobalManager;
import org.corps.bi.protobuf.BytesList;
import org.corps.bi.protobuf.KVEntity;
import org.corps.bi.protobuf.LongEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import com.hoolai.bi.collectdata.server.core.Constant;
import com.hoolai.bi.collectdata.server.service.collect.MetricProcesser;
import com.hoolai.bi.collectdata.server.service.collect.MetricProcesserKafkaImpl;
import com.hoolai.bi.report.util.KV;
import com.jian.tools.util.JSONUtils;

@Controller
public class InnerCollectMetricsServerController extends AbstractCollectController{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(InnerCollectMetricsServerController.class);
	
	private  MetricProcesser metricProcesserKafka;
	
	public InnerCollectMetricsServerController() {
		super();
		if(Constant.IS_FINAL_DATACENTER) {
			this.metricProcesserKafka=new MetricProcesserKafkaImpl();
		}
	}

	@RequestMapping(value = {"/inner/transport/"}, method = {RequestMethod.POST })
	public void collect(MultipartHttpServletRequest request,HttpServletResponse response,Model model){
		long begin=System.currentTimeMillis();
		Map<String,Object> ret=new HashMap<String, Object>();
		List<KV<String,Boolean>> resultList=new ArrayList<KV<String,Boolean>>();
		try {
		
			
			String metric=request.getParameter("metric");
			String version=request.getParameter("version");
			String recordNumStr=request.getParameter("recordNum");
			int recordNum=0;
			if(StringUtils.isNotEmpty(recordNumStr)) {
				recordNum=Integer.parseInt(recordNumStr);
			}
			String dataSizeStr=request.getParameter("dataSize");
			int dataSize=0;
			if(StringUtils.isNotEmpty(dataSizeStr)) {
				dataSize=Integer.parseInt(dataSizeStr);
			}
			
			MutablePair<AtomicLong, AtomicLong> mutableProcessNumPair=super.metricProcessNumMap.get(metric);
			
			Long rts=mutableProcessNumPair.getLeft().incrementAndGet();
			
			String status="unProcess";
			List<MultipartFile> metricDatas=request.getFiles("metricDatas");
			if (!metricDatas.isEmpty()) {
				for(MultipartFile multipartFile:metricDatas){
					boolean issucc=false;
					if(version!=null&&version.equals("3.0")) {
						issucc=this.processMulipartV3(metric,recordNum,dataSize,multipartFile);
					}else {
						issucc=this.processMulipartV2(metric,multipartFile);
					}
					resultList.add(new KV<String, Boolean>(multipartFile.getName(), issucc));
					if(issucc) {
						status="succ";
					}else {
						status="fail";
						break;
					}
				}
			}else {
				LOGGER.error("metric:{} recordNum:{} dataSize:{} version:{} the metricDatas is null",metric,recordNumStr,dataSizeStr,version);
			}
			ret.put("resultList", resultList);
			ret.put("status", status);
			
			long metricProcessedNum = mutableProcessNumPair.getRight().addAndGet(recordNum);
			
			MetricLoggerControl metricLoggerControl=MetricLoggerControl.parseFromName(metric);
			
			if(rts!=null && rts%metricLoggerControl.getPerNum()==0) {
				long end=System.currentTimeMillis();
				LOGGER.info("metric:{} requestTimes:{}  metricProcessedNum:{} recordNum:{} dataSize:{} version:{} status:{} spendMills:{}",metric,rts,metricProcessedNum,recordNumStr,dataSizeStr,version,status,(end-begin));
			}
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
			ret.put("status", "err");
			ret.put("msg", e.getMessage());
		}
		
		try {
			String jsonResult=JSONUtils.toJSON(ret);
			response.getOutputStream().write(jsonResult.getBytes("UTF-8"));
			response.getOutputStream().flush();
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
	}
	
	/**
	 * 默认全部保存到本地，然后再往外发送
	 * @param metric
	 * @param multipartFile
	 * @return
	 */
	private boolean processMulipartV3(String metric,int recordNum,int dataSize,MultipartFile multipartFile) {
		try {
			
			byte[] datas=multipartFile.getBytes();
			
			BytesList dataList=new BytesList(datas);
			
			if(dataList.size()!=recordNum||dataSize!=datas.length) {
				LOGGER.error("processMulipartV3 metric:{} recordNum:{} receiveRecordNum:{} dataSize:{} receiveDataSize:{} is losed.",metric,recordNum,dataList.size(),dataSize,datas.length);
				return false;
			}
			
			boolean res=this.innerProcessMetricDatas(metric, dataList);
			
			return res;
		} catch (IOException e) {
			LOGGER.error(e.getMessage(),e);
			return false;
		}
	}
	
	/**
	 * 默认全部保存到本地，然后再往外发送
	 * @param metric
	 * @param multipartFile
	 * @return
	 */
	private boolean processMulipartV2(String metric,MultipartFile multipartFile) {
		try {
			byte[] datas=multipartFile.getBytes();
			
			BytesList dataList=new BytesList(datas);
			
			if(dataList.size()<=0) {
				LOGGER.error("processMulipartV2 metric:{}  receiveRecordNum:{} receiveDataSize:{} is losed.",metric,dataList.size(),datas.length);
				return false;
			}
			
			boolean res=this.innerProcessMetricDatas(metric, dataList);
			
			return res;
		} catch (IOException e) {
			LOGGER.error(e.getMessage(),e);
			return false;
		}
	}
	
	private boolean processMulipart(String metric,MultipartFile multipartFile) {
		try {
			byte[] datas=multipartFile.getBytes();
			
			BytesList dataList=new BytesList(datas);
			
			if(dataList.size()<=0) {
				return true;
			}
			
			boolean res=false;
			
			if(Constant.IS_FINAL_DATACENTER){
				for (byte[] metricData : dataList) {
					
					KVEntity kvEntity=new KVEntity(metricData);
					
//					DauConverter dauConverter=new DauConverter(kvEntity.getV());
//					
//					MetaConverter metaConverter=new MetaConverter(kvEntity.getK());
//					
//					System.out.println(JSONUtils.toJSON(metaConverter.getEntity())+":"+JSONUtils.toJSON(dauConverter.getEntity()));
					
					res=this.metricProcesserKafka.process(metric, kvEntity.getK(), kvEntity.getV());
				}
				// @TODO 有的成功，有的失败，失败记录日志，默认都成功（一般情况下在统一批处理当中都会成功）
				res=true;
			}else{
				res=this.innerProcessMetricDatas(metric, dataList);
			}
			return res;
		} catch (IOException e) {
			LOGGER.error(e.getMessage(),e);
			return false;
		}
	}
	
	private final boolean innerProcessMetricDatas(String metric,BytesList dataList) {
		
		if(dataList==null||dataList.isEmpty()) {
			return true;
		}
		
		Map<byte[],byte[]> batchDataMap=new HashMap<byte[],byte[]>();
		for (byte[] metricData : dataList) {
			
			byte[] rockV=metricData;
			
			long rockId=RocksdbGlobalManager.getInstance().getIdIncrAndGet(metric);
			
			byte[] rockK=new LongEntity(rockId).toByteArray();
			
			batchDataMap.put(rockK, rockV);
			
		}
		MetricRocksdbColumnFamilys metricRocksdbColumnFamily=MetricRocksdbColumnFamilys.parseFromName(metric);
		if(metricRocksdbColumnFamily==null) {
			return false;
		}
		metricRocksdbColumnFamily.getMetricDao().saveBatch(batchDataMap);
		return true;

	}
}
