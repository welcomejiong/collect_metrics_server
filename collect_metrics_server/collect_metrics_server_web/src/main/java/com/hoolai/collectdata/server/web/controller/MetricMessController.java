package com.hoolai.collectdata.server.web.controller;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.corps.bi.core.Constants;
import org.corps.bi.dao.rocksdb.CatMetricsInRocksdb;
import org.corps.bi.transport.MetricsTransporterConfig;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class MetricMessController extends AbstractPanelController{

	
	@RequestMapping(value = "/metrics/mess/", method = { RequestMethod.GET,RequestMethod.POST })
	@ResponseBody
	public Object metricsMess(@RequestParam(required=true) String userName,HttpServletRequest request,HttpServletResponse response,Model model){
		
		if(!"jianjiongguo".equals(userName)) {
			return "please input the valid userName.";
		}
		
		return CatMetricsInRocksdb.getInstance().catAll();
	}
	
	@RequestMapping(value = "/server/mess/", method = { RequestMethod.GET,RequestMethod.POST })
	@ResponseBody
	public Object collectMetrics(@RequestParam(required=true) String userName,HttpServletRequest request,HttpServletResponse response,Model model){
		
		if(!"jianjiongguo".equals(userName)) {
			return "please input the valid userName.";
		}
		
		Map<String,Object> ret=new HashMap<String,Object>();
		ret.put("metricsTransportrConfig", MetricsTransporterConfig.getInstance());
		
		try {
			Map<String,Object> globleConstantConfig=new HashMap<String,Object>();
			Field[] globleFields=FieldUtils.getAllFields(Constants.class);
			if(globleFields!=null) {
				for (Field field : globleFields) {
					field.setAccessible(true);
					globleConstantConfig.put(field.getName(), field.get(Constants.class));
				}
			}
			ret.put("globleConstantConfig", globleConstantConfig);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return ret;
	}
	
	
	@RequestMapping(value = "/transport/control/", method = { RequestMethod.GET,RequestMethod.POST })
	@ResponseBody
	public Object transportControl(@RequestParam(required=true) String userName,
			@RequestParam(required=true) boolean transportOn,
			HttpServletRequest request,HttpServletResponse response,Model model){
		
		if(!"jianjiongguo".equals(userName)) {
			return "please input the valid userName.";
		}
		
		MetricsTransporterConfig.getInstance().handControlTransportOn(transportOn);
		
		Map<String,Object> ret=new HashMap<String,Object>();
		ret.put("metricsTransportrConfig", MetricsTransporterConfig.getInstance());
		
		return ret;
	}
	
	
}

