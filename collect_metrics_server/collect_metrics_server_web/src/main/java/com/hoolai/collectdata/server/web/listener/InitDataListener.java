package com.hoolai.collectdata.server.web.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.corps.bi.services.RecordingServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hoolai.bi.collectdata.server.core.Constant;


public class InitDataListener implements ServletContextListener {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(InitDataListener.class);

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		LOGGER.info("contextInitialized serverUrl:{}",Constant.SERVER_URL);
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		try {
			boolean ret=RecordingServices.getInstance().shutdown();
			LOGGER.info("contextDestroyed shutdown RecordingServices ret:{}",ret);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
	}

}
