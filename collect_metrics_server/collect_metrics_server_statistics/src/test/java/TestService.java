

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.corps.bi.datacenter.server.statistics.repo.impl.redis.MetricStatisticRepoRedisImpl;
import org.corps.bi.datacenter.server.statistics.service.MetricStatisticService;
import org.corps.bi.datacenter.server.statistics.service.impl.MetricStatisticServiceImpl;
import org.corps.bi.metrics.Meta;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

public class TestService {
	

	@Test
	public void testSeconds() throws Exception {
		
		JedisPool jedisPool=new JedisPool(new GenericObjectPoolConfig(),"127.0.0.1", 6379,3000,"jiong");
		
		MetricStatisticRepoRedisImpl metricStatisticRepo=new MetricStatisticRepoRedisImpl(jedisPool);
		
		MetricStatisticService metricStatisticService=new MetricStatisticServiceImpl(metricStatisticRepo);
		
		Meta metricDayMeta=new Meta();
		metricDayMeta.setMetric("counter");
		metricDayMeta.setSnId("1");
		metricDayMeta.setGameId("1");
		metricDayMeta.setDs("2020-05-01");
		
		Date now=new Date();
		
		for (int i = 0; i < 1000; i++) {
			metricStatisticService.incrMetricNum(TimeUnit.SECONDS, 5,DateUtils.addSeconds(now, i), metricDayMeta, i);
		}
		TimeUnit.SECONDS.sleep(30);
		
		Map<String,Long> allNum=metricStatisticService.getMetricNum(metricDayMeta);
		for (Entry<String,Long> entry : allNum.entrySet()) {
			System.out.println("key:"+entry.getKey()+" val:"+entry.getValue());
		}
		
		TimeUnit.MINUTES.sleep(10);
	}
	
	@Test
	public void testMinutes() throws Exception {
		
		JedisPool jedisPool=new JedisPool(new GenericObjectPoolConfig(),"127.0.0.1", 6379,3000,"jiong");
		
		MetricStatisticRepoRedisImpl metricStatisticRepo=new MetricStatisticRepoRedisImpl(jedisPool);
		
		MetricStatisticService metricStatisticService=new MetricStatisticServiceImpl(metricStatisticRepo);
		
		Meta metricDayMeta=new Meta();
		metricDayMeta.setMetric("counter");
		metricDayMeta.setSnId("1");
		metricDayMeta.setGameId("1");
		metricDayMeta.setDs("2020-05-02");
		
		Date now=new Date();
		
		for (int i = 0; i < 1000; i++) {
			metricStatisticService.incrMetricNum(TimeUnit.MINUTES, 5,DateUtils.addSeconds(now, i), metricDayMeta, i);
		}
		TimeUnit.SECONDS.sleep(30);
		
		Map<String,Long> allNum=metricStatisticService.getMetricNum(metricDayMeta);
		for (Entry<String,Long> entry : allNum.entrySet()) {
			System.out.println("key:"+entry.getKey()+" val:"+entry.getValue());
		}
		
		TimeUnit.MINUTES.sleep(10);
	}
	
	@Test
	public void testGetDatas() throws Exception {
		
		JedisPool jedisPool=new JedisPool(new GenericObjectPoolConfig(),"127.0.0.1", 6379,3000,"jiong");
		
		MetricStatisticRepoRedisImpl metricStatisticRepo=new MetricStatisticRepoRedisImpl(jedisPool);
		
		MetricStatisticService metricStatisticService=new MetricStatisticServiceImpl(metricStatisticRepo);
		
		Meta metricDayMeta=new Meta();
		metricDayMeta.setMetric("counter");
		metricDayMeta.setSnId("1");
		metricDayMeta.setGameId("1");
		metricDayMeta.setDs("2020-04-27");
		
		
		Map<String,Long> allNum=metricStatisticService.getMetricNum(metricDayMeta);
		for (Entry<String,Long> entry : allNum.entrySet()) {
			System.out.println("key:"+entry.getKey()+" val:"+entry.getValue());
		}
		
	}
	
	
}
