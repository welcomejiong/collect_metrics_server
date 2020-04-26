package org.corps.bi.datacenter.server.statistics.service.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.corps.bi.datacenter.core.DataCenterSystemThreadFactory;
import org.corps.bi.datacenter.server.statistics.core.GetSecondField;
import org.corps.bi.datacenter.server.statistics.core.MetricNumIncrement;
import org.corps.bi.datacenter.server.statistics.repo.MetricStatisticRepo;
import org.corps.bi.datacenter.server.statistics.service.MetricStatisticService;
import org.corps.bi.metrics.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricStatisticServiceImpl implements MetricStatisticService {
	
	private static final Logger LOGGER =LoggerFactory.getLogger(MetricStatisticServiceImpl.class);
	
	private static final int BATCH_TO_DB_SIZE=200;
	
	private final MetricStatisticRepo metricStatisticRepo;
	
	private final BlockingQueue<MetricNumIncrement> metricNumIncrementQueue;
	
	private final ScheduledExecutorService rollService;
	
	public MetricStatisticServiceImpl(MetricStatisticRepo metricStatisticRepo) {
		super();
		this.metricStatisticRepo = metricStatisticRepo;
		this.metricNumIncrementQueue = new LinkedBlockingDeque<MetricNumIncrement>(1000000);
		rollService = Executors.newScheduledThreadPool(1,
				new DataCenterSystemThreadFactory("datacenter-statistics-batch-to-redis"));
		rollService.scheduleAtFixedRate(new AsyncPersistToRedisThread(this.metricStatisticRepo,this.metricNumIncrementQueue), 500, 500, TimeUnit.MILLISECONDS);
	}

	@Override
	public void incrMetricNum(Meta metricDayMeta, String secondField, long incrNum) {
		this.metricNumIncrementQueue.add(new MetricNumIncrement(metricDayMeta, secondField, incrNum));
	}

	@Override
	public void incrMetricNum(TimeUnit timeUnit, int window, Meta metricDayMeta, long incrNum) {
		String secondField=this.getSecondField(timeUnit, window);
		this.metricNumIncrementQueue.add(new MetricNumIncrement(metricDayMeta, secondField, incrNum));
	}
	
	@Override
	public void incrMetricNum(TimeUnit timeUnit, int window, Date date, Meta metricDayMeta, long incrNum) {
		Calendar cal=Calendar.getInstance();
		cal.setTime(date);
		String secondField=this.getSecondField(timeUnit, window,cal);
		this.metricNumIncrementQueue.add(new MetricNumIncrement(metricDayMeta, secondField, incrNum));
	}
	
	@Override
	public Long getMetricNum(Meta metricDayMeta, String secondField) {
		return this.metricStatisticRepo.getMetricNum(metricDayMeta, secondField);
	}

	@Override
	public Map<String, Long> getMetricNum(Meta metricDayMeta) {
		return this.metricStatisticRepo.getMetricNum(metricDayMeta);
	}
	
	private String getSecondField(TimeUnit timeUnit, int window) {
		return this.getSecondField(timeUnit, window, Calendar.getInstance());
	}
	
	private String getSecondField(TimeUnit timeUnit, int window,Calendar calendar) {
		if(!TimeUnit.HOURS.equals(timeUnit)&&!TimeUnit.MINUTES.equals(timeUnit)&&!TimeUnit.SECONDS.equals(timeUnit)) {
			throw new RuntimeException("unsupported timeunit.now only support hours,ninutes,seconds");
		}
		if(TimeUnit.HOURS.equals(timeUnit)) {
			return GetSecondField.HOURS.parse(calendar, window);
		}else if(TimeUnit.MINUTES.equals(timeUnit)) {
			return GetSecondField.MINUTES.parse(calendar, window);
		}else{
			return GetSecondField.SECONDS.parse(calendar, window);
		}
	}
	
	private class AsyncPersistToRedisThread implements Runnable{
		
		private final MetricStatisticRepo metricStatisticRepo;
		
		private final BlockingQueue<MetricNumIncrement> metricNumIncrementQueue;

		public AsyncPersistToRedisThread(MetricStatisticRepo metricStatisticRepo,BlockingQueue<MetricNumIncrement> metricNumIncrementQueue) {
			super();
			this.metricStatisticRepo=metricStatisticRepo;
			this.metricNumIncrementQueue = metricNumIncrementQueue;
		}

		@Override
		public void run() {
			this.batchPerpist();
		}
		
		private void batchPerpist() {
			try {
				if(this.metricNumIncrementQueue.isEmpty()){
					return ;
				}
				long begin=System.currentTimeMillis();
				List<MetricNumIncrement> batchList=new ArrayList<MetricNumIncrement>();
				for (int i = 0; i < BATCH_TO_DB_SIZE; i++) {
					MetricNumIncrement metricNumIncrement=this.metricNumIncrementQueue.poll();
					if(metricNumIncrement==null){
						return ;
					}
					batchList.add(metricNumIncrement);
				}
				this.metricStatisticRepo.incrMetricNum(batchList);
				long end=System.currentTimeMillis();
				LOGGER.info("batch to redis the size:{} spendMills:{}",batchList.size(),(end-begin));
			} catch (Exception e) {
				LOGGER.error(e.getMessage(),e);
			}
		}
		
	}

	
	

	

}
