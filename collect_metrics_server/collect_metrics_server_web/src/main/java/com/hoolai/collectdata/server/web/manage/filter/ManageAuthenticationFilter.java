package com.hoolai.collectdata.server.web.manage.filter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang3.StringUtils;

import com.hoolai.bi.collectdata.server.core.Constant;
import com.jian.tools.util.ServletUtils;

public class ManageAuthenticationFilter implements Filter {
	
	private static final Set<String> excludeUrls=new HashSet<String>();
	
	private FilterConfig filterConfig;
	
	private String loginUrl;

	@Override
	public void destroy() {

	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response,
			FilterChain chain) throws IOException, ServletException {
		HttpServletRequest httpRequest = (HttpServletRequest) request;
		HttpServletResponse httpResponse = (HttpServletResponse) response;
		
		HttpSession session = ServletUtils.getSession(httpRequest,true);
		
		String servletPath=httpRequest.getServletPath();
		String removeSuffix=servletPath;
		if(servletPath.indexOf(".")>-1){
			removeSuffix=servletPath.substring(0,servletPath.indexOf("."));
		}
//		boolean isExclude=this.isExclude(removeSuffix);
//		if (!isExclude&&(null == session.getAttribute(AbstractPanelController.SESSION_USER_KEY))) {
//			//session.setAttribute(AbstractPanelController.SESSION_USER_LAST_URL_KEY, httpRequest.getServletPath()+"?"+httpRequest.getQueryString());
//			httpResponse.sendRedirect(HttpUtil.getBasePath(httpRequest)+loginUrl);
//			return;
//		}
		
//		if(!isExclude&&session.getAttribute(AbstractPanelController.SESSION_USER_LAST_URL_KEY)!=null){
//			System.out.println("sesId:"+session.getId());
//			httpResponse.sendRedirect(HttpUtil.getBasePath(httpRequest)+(String)session.getAttribute(AbstractPanelController.SESSION_USER_LAST_URL_KEY));
//			session.removeAttribute(AbstractPanelController.SESSION_USER_LAST_URL_KEY);
//			return ; 
//		}

		chain.doFilter(request, response);

	}

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		this.filterConfig = filterConfig;
		this.loginUrl = filterConfig.getInitParameter("loginUrl");
		String excludeUrlsConf=Constant.MANAGE_AUTH_EXCLUDE_URL;
		if(StringUtils.isBlank(excludeUrlsConf)){
			return ;
		}
		String[] excludeUrlsArr=excludeUrlsConf.split(",");
		for(String exUrl:excludeUrlsArr){
			excludeUrls.add(exUrl);
		}
	}
	
	private boolean isExclude(String url){
		return excludeUrls.contains(url);
	}

}
