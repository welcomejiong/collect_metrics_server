package com.hoolai.collectdata.server.web.controller;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.codehaus.jackson.type.TypeReference;
import org.corps.bi.core.MetricLoggerControl;
import org.corps.bi.core.MetricRequestParams;
import org.corps.bi.core.MetricResponse;
import org.corps.bi.metrics.AdTracking;
import org.corps.bi.metrics.Counter;
import org.corps.bi.metrics.CustomBinaryBodyMetric;
import org.corps.bi.metrics.Dau;
import org.corps.bi.metrics.Economy;
import org.corps.bi.metrics.GameInfo;
import org.corps.bi.metrics.IMetric;
import org.corps.bi.metrics.Install;
import org.corps.bi.metrics.Milestone;
import org.corps.bi.metrics.Payment;
import org.corps.bi.metrics.converter.MetricEntityConverterManager;
import org.corps.bi.services.RecordingServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hoolai.bi.collectdata.server.core.Constant;
import com.hoolai.bi.collectdata.server.service.collect.MetricProcesser;
import com.hoolai.bi.collectdata.server.service.collect.MetricProcesserKafkaImpl;
import com.jian.tools.util.JSONUtils;

@Controller
public class CollectMetricsServerController extends AbstractCollectController{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(CollectMetricsServerController.class.getSimpleName());
	
	private  MetricProcesser metricProcesserKafka;
	
	
	private RecordingServices recordingServices=RecordingServices.getInstance();
	
	public CollectMetricsServerController() {
		super();
		if(Constant.IS_FINAL_DATACENTER) {
			this.metricProcesserKafka=new MetricProcesserKafkaImpl();
		}
	}

	@RequestMapping(value = {"/collectMetrics/"}, method = {RequestMethod.POST })
	@ResponseBody
	public MetricResponse collectMetrics(@RequestParam String datas,HttpServletRequest request,HttpServletResponse response,Model model){
		MetricResponse ret=new MetricResponse();
		try {
			long begin=System.currentTimeMillis();
			
			boolean res=false;
			
			TypeReference<List<MetricRequestParams>> typeReference=new TypeReference<List<MetricRequestParams>>() {};
			List<MetricRequestParams> metricRequestParams=JSONUtils.fromJSON(datas, typeReference, false);
			if(metricRequestParams==null||metricRequestParams.isEmpty()) {
				res=true;
			}else {
				for (MetricRequestParams metricRequestParam : metricRequestParams) {
					try {
						
						MutablePair<AtomicLong, AtomicLong> mutableProcessNumPair=super.metricProcessNumMap.get(metricRequestParam.getMetric());
						Long rts=mutableProcessNumPair.getLeft().incrementAndGet();
						
						IMetric imetric=MetricEntityConverterManager.parseFromName(metricRequestParam.getMetric()).parseMetricEntityFromJson(metricRequestParam.getJsonData());
						this.recordingServices.add(metricRequestParam.getGameId(), metricRequestParam.getDs(), imetric);
						
						long metricProcessedNum = mutableProcessNumPair.getRight().incrementAndGet();
						MetricLoggerControl metricLoggerControl=MetricLoggerControl.parseFromName(metricRequestParam.getMetric());
						if(rts!=null && rts%metricLoggerControl.getPerNum()==0) {
							long end=System.currentTimeMillis();
							LOGGER.info("jsonMultiReq metric:{} requestTimes:{}  metricProcessedNum:{} recordNum:{} isSucc:{} spendMills:{}",metricRequestParam.getMetric(),rts,metricProcessedNum,1,true,(end-begin));
						}
						
					} catch (Exception e) {
						LOGGER.error(e.getMessage(),e);
					}
				}
				res=true;
			}
			if(res){
				ret.setStatus("succ");
			}else{
				ret.setStatus("fail");
			}
		} catch (Exception e) {
			LOGGER.error(datas);
			LOGGER.error(e.getMessage(),e);
			ret.setStatus("err");
			ret.setMsg(e.getMessage());
		}
		
		return ret;
		
	}
	
	@RequestMapping(value = {"/collectMetric/"}, method = {RequestMethod.POST })
	@ResponseBody
	public MetricResponse collectMetric(@RequestParam String datas,HttpServletRequest request,HttpServletResponse response,Model model){
		MetricResponse ret=new MetricResponse();
		try {
			long begin=System.currentTimeMillis();
			boolean res=false;
			MetricRequestParams metricRequestParams=JSONUtils.fromJSON(datas, MetricRequestParams.class);
			
			MutablePair<AtomicLong, AtomicLong> mutableProcessNumPair=super.metricProcessNumMap.get(metricRequestParams.getMetric());
			
			Long rts=mutableProcessNumPair.getLeft().incrementAndGet();
			
			IMetric imetric=MetricEntityConverterManager.parseFromName(metricRequestParams.getMetric()).parseMetricEntityFromJson(metricRequestParams.getJsonData());
			this.recordingServices.add(metricRequestParams.getGameId(), metricRequestParams.getDs(), imetric);
			res=true;
			if(res){
				ret.setStatus("succ");
			}else{
				ret.setStatus("fail");
			}
			
			long metricProcessedNum = mutableProcessNumPair.getRight().incrementAndGet();
			
			MetricLoggerControl metricLoggerControl=MetricLoggerControl.parseFromName(metricRequestParams.getMetric());
			
			if(rts!=null && rts%metricLoggerControl.getPerNum()==0) {
				long end=System.currentTimeMillis();
				LOGGER.info("jsonSingleReq metric:{} requestTimes:{}  metricProcessedNum:{} recordNum:{} isSucc:{} spendMills:{}",metricRequestParams.getMetric(),rts,metricProcessedNum,1,res,(end-begin));
			}
			
		} catch (Exception e) {
			LOGGER.error(datas);
			LOGGER.error(e.getMessage(),e);
			ret.setStatus("err");
			ret.setMsg(e.getMessage());
		}
		
		return ret;
		
		
	}
	
	@RequestMapping(value = {"/coll/"}, method = {RequestMethod.POST })
	@ResponseBody
	public MetricResponse collect(@RequestParam String gameId,@RequestParam String ds,@RequestParam String metric,
								HttpServletRequest request,HttpServletResponse response,Model model){
		MetricResponse ret=new MetricResponse();
		try {
			long begin=System.currentTimeMillis();
			
			IMetric  entityMetric=MetricRequestParser.parseFromName(metric).doParse(request);
			boolean res=false;

			MutablePair<AtomicLong, AtomicLong> mutableProcessNumPair=super.metricProcessNumMap.get(entityMetric.metric());
			Long rts=mutableProcessNumPair.getLeft().incrementAndGet();
			
			this.recordingServices.add(gameId, ds, entityMetric);
			res=true;
			if(res){
				ret.setStatus("succ");
			}else{
				ret.setStatus("fail");
			}
			
			long metricProcessedNum = mutableProcessNumPair.getRight().incrementAndGet();
			MetricLoggerControl metricLoggerControl=MetricLoggerControl.parseFromName(entityMetric.metric());
			if(rts!=null && rts%metricLoggerControl.getPerNum()==0) {
				long end=System.currentTimeMillis();
				LOGGER.info("normalSingleReq metric:{} requestTimes:{}  metricProcessedNum:{} recordNum:{} isSucc:{} spendMills:{}",entityMetric.metric(),rts,metricProcessedNum,1,res,(end-begin));
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
			ret.setStatus("err");
			ret.setMsg(e.getMessage());
		}
		
		return ret;
		
		
	}
	
	public enum MetricRequestParser{
		
		DAU("dau",Dau.class),
		
		INSTALL("install",Install.class),
		
		COUNTER("counter",Counter.class),
		
		ECONOMY("economy",Economy.class) ,
		
		GAMEINFO("gameinfo",GameInfo.class),
		
		MILESTONE("milestone",Milestone.class),
		
		PAYMENT("payment",Payment.class) ,
		
		ADTRACKING("adtracking",AdTracking.class) ,
		
		CUSTOMBINARYBODYMETRIC("custombinarybodymetric",CustomBinaryBodyMetric.class) ;
		
		private static final Logger LOGGER=LoggerFactory.getLogger(MetricRequestParser.class.getSimpleName());
		
		private static final Map<String,MetricRequestParser> TOPIC_METRIC_MAP=new HashMap<String,MetricRequestParser>();
		
		static {
			for (MetricRequestParser parser : MetricRequestParser.values()) {
				TOPIC_METRIC_MAP.put(parser.metric, parser);
			}
		}
		
		private String metric;
		
		private Class<?> metricClazz;
		
		private List<Field> fieldList;
		
		private Map<String,PropertyDescriptor> propertyDescriptorMap;
		
		private MetricRequestParser(String metric,Class<?> metricClazz) {
			this.metric = metric;
			this.metricClazz=metricClazz;
			this.fieldList=new ArrayList<Field>();
			this.propertyDescriptorMap=new HashMap<String, PropertyDescriptor>();
			this.init();
		}
		
		private void init() {
			try {
				for (Field field : FieldUtils.getAllFieldsList(this.metricClazz)) {
					
					if("extraCache".equals(field.getName())||"FIELD_SEPARATOR".equals(field.getName())) {
						continue;
					}
					PropertyDescriptor entityPropertyDescriptor=new PropertyDescriptor(field.getName(), this.metricClazz);
					propertyDescriptorMap.put(field.getName(), entityPropertyDescriptor);
					this.fieldList.add(field);
				}
				
			} catch (IntrospectionException e) {
				LOGGER.error(e.getMessage(),e);
			}
		}
		
		public IMetric doParse(HttpServletRequest request) throws Exception {
			IMetric ret=(IMetric)ConstructorUtils.invokeConstructor(this.metricClazz);
			for (Field field : this.fieldList) {
				String fieldName=field.getName();
				Object fieldValue=request.getParameter(fieldName);
				if(fieldValue==null) {
					continue;
				}
				if(!String.class.equals(field.getType())) {
					fieldValue=ConvertUtils.convert(fieldValue, field.getType());
				}
				PropertyDescriptor entityPropertyDescriptor=this.propertyDescriptorMap.get(fieldName);
				if(entityPropertyDescriptor==null) {
					continue;
				}
				entityPropertyDescriptor.getWriteMethod().invoke(ret, fieldValue);
			}
			return ret;
		}
		
		public static MetricRequestParser parseFromName(String metric) {
			if(TOPIC_METRIC_MAP.containsKey(metric)) {
				return TOPIC_METRIC_MAP.get(metric);
			}
			return null;
		}
		
		
		
		
	}
	
	
}

