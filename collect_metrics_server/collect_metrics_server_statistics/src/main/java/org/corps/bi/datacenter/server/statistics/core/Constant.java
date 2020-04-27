package org.corps.bi.datacenter.server.statistics.core;

import java.util.concurrent.TimeUnit;

public class Constant {

	public static final String DEFAULT_FORMAT_DATE_DAY="yyyy-MM-dd";
	
	public static final String FORMAT_DATE_DAY_NUM="yyyyMMdd";
	
	public static final String DEFAULT_FORMAT_DATE_MINUTES="yyyy-MM-dd HH:mm";
	
	public static final String DEFAULT_FORMAT_DATE_SECONDS="yyyy-MM-dd HH:mm:ss";
	
	public static final String DEFAULT_CHARSET="UTF-8";
	
	public static final int ONE_HOUR_SECONDS=new Long(TimeUnit.HOURS.toSeconds(1)).intValue();
	
	public static final int ONE_DAY_SECONDS=new Long(TimeUnit.DAYS.toSeconds(1)).intValue();
	
	
	
	
}
