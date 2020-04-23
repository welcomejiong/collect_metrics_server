package com.hoolai.collectdata.server.web.controller;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang3.tuple.MutablePair;
import org.corps.bi.datacenter.core.DataCenterTopics;

import com.jian.tools.util.HttpClientUtils;
import com.jian.tools.util.ServletUtils;


public abstract class AbstractPanelController {

	protected static final String PROCESS_STATUS_KEY="p_s";
	
	protected static final int PROCESS_STATUS_SUCCESS=0;
	
	protected static final int PROCESS_STATUS_FAIL=-1;
	/**
	 * panel_session_user_key
	 */
	public static final String SESSION_USER_KEY="p_s_u_k";
	
	public static final String SESSION_USER_LAST_URL_KEY="jianUserLastUrl";
	
	public static final int PAGE_SIZE = 10;
	
	public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	public static final String BEGIN_DATE = "beginTime";
	public static final String END_DATE = "endTime";
	
	public AbstractPanelController() {
		super();
	}

	public String forward(String url){
		return "forward:"+url;
	}
	
	public String redirect(String url){
		return "redirect:"+url;
	}
	
	public String redirect404(Map<String,String> params){
		if(params!=null&&!params.isEmpty()){
			try {
				String url=HttpClientUtils.concatUrlSerialParams("/404.ac", params, true);
				return redirect(url);
			} catch (UnsupportedEncodingException e) {
			}
		}
		return redirect("/404.ac");
	}
	
	public String redirect404(){
		return redirect404(null);
	}
	
	protected void setSessionDate(HttpServletRequest request,String beginDate,String endDate){
		HttpSession session=ServletUtils.getSession(request);
		session.setAttribute(BEGIN_DATE, beginDate);
		session.setAttribute(END_DATE, endDate);
	}
	
	protected void setSession(HttpServletRequest request,String key,Object obj) {
		if(obj==null){
			throw new RuntimeException("set session users is null!");
		}
		HttpSession session=ServletUtils.getSession(request);
		session.setAttribute(key, obj);
	}
	
	protected void removeSession(HttpServletRequest request,String key) {
		HttpSession session=ServletUtils.getSession(request);
		session.removeAttribute(key);
	}
	
	protected Object getSession(HttpServletRequest request,String key){
		HttpSession session=ServletUtils.getSession(request);
		return session.getAttribute(key);
	}
	
}
