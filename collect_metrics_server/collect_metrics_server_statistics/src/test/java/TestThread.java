

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.corps.bi.datacenter.server.statistics.repo.impl.redis.MetricStatisticRepoRedisImpl;
import org.corps.bi.metrics.Meta;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

public class TestThread {
	

	@Test
	public void testInstall() throws Exception {
		
		JedisPool jedisPool=new JedisPool(new GenericObjectPoolConfig(),"127.0.0.1", 6379,3000,"jiong");
		
		MetricStatisticRepoRedisImpl repo=new MetricStatisticRepoRedisImpl(jedisPool);
		
		Meta metricDayMeta=new Meta();
		
		String secondField="10:20:25";
		
		Long ret=repo.incrMetricNum(metricDayMeta, secondField, 20);
		
		System.out.println("ret:"+ret);
		
		repo.incrMetricNum(metricDayMeta, "10:20:30", 20);
		
		Long incrRes=repo.getMetricNum(metricDayMeta, secondField);
		
		System.out.println("incrRes:"+incrRes);
		
		Map<String,Long> allNum=repo.getMetricNum(metricDayMeta);
		for (Entry<String,Long> entry : allNum.entrySet()) {
			System.out.println("key:"+entry.getKey()+" val:"+entry.getValue());
		}
	}
	
	@Test
	public void testTimeUnit() throws Exception {
		
		long now=System.currentTimeMillis();
		
		long hours=TimeUnit.MILLISECONDS.toHours(now);
		
		long minutes=TimeUnit.MILLISECONDS.toMinutes(now);
		
		long seconds=TimeUnit.MILLISECONDS.toSeconds(now);
		
		System.out.println("now:"+now+" hours:" +hours);
		
		System.out.println("now:"+now+" minutes:" +minutes);
		
		System.out.println("now:"+now+" seconds:" +seconds);
		
		long toMills=TimeUnit.SECONDS.toMillis(seconds);
		
		System.out.println("now:"+now+" seconds:" +seconds+" toMills:"+toMills);
		
		Date nowDate=new Date(now);
		
		System.out.println(DateFormatUtils.format(nowDate, "yyyy-MM-dd HH:mm:ss"));
		
		Date toDate=new Date(toMills);
		
		System.out.println(DateFormatUtils.format(toDate, "yyyy-MM-dd HH:mm:ss"));
	}
}
