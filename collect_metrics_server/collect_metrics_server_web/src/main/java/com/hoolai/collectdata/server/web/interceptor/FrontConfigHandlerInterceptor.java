package com.hoolai.collectdata.server.web.interceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.hoolai.bi.collectdata.server.core.Constant;

public class FrontConfigHandlerInterceptor extends HandlerInterceptorAdapter {
	
	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
		return super.preHandle(request, response, handler);
	}

	@Override
	public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
		
		super.postHandle(request, response, handler, modelAndView);
		
		request.setAttribute("serverUrl", Constant.SERVER_URL);
		request.setAttribute("resourceUrl", Constant.RESOURCE_URL);
		request.setAttribute("siteName",Constant.SITE_NAME);
		request.setAttribute("isPublished",Constant.IS_PUBLISHED);
		request.setAttribute("version",Constant.VERSION);
		
		request.setAttribute("contextPath",request.getContextPath());
		
		if(modelAndView==null){
			return ;
		}
	}
}
