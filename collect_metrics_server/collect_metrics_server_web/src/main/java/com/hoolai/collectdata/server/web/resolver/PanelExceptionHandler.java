package com.hoolai.collectdata.server.web.resolver;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;

public class PanelExceptionHandler implements HandlerExceptionResolver {
	
	private static final Logger logger=LoggerFactory.getLogger("exception");

	@Override
	public ModelAndView resolveException(HttpServletRequest request,
			HttpServletResponse response, Object handler, Exception ex) {
		ModelAndView modelAndView=new ModelAndView("error/50x.jsp");
		logger.error("req "+request.getServletPath()+" meet error.", ex);
		return modelAndView;
	}

}
