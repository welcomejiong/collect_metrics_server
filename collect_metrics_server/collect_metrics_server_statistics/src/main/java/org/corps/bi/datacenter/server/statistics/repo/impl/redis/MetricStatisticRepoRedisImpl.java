package org.corps.bi.datacenter.server.statistics.repo.impl.redis;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.corps.bi.datacenter.server.statistics.core.Constant;
import org.corps.bi.datacenter.server.statistics.core.MetricNumIncrement;
import org.corps.bi.datacenter.server.statistics.core.StorageStatisticsKeyManager;
import org.corps.bi.datacenter.server.statistics.repo.MetricStatisticRepo;
import org.corps.bi.metrics.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class MetricStatisticRepoRedisImpl implements MetricStatisticRepo {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricStatisticRepoRedisImpl.class);
	
	private JedisPool jedisPool;

	public MetricStatisticRepoRedisImpl(JedisPool jedisPool) {
		super();
		this.jedisPool = jedisPool;
	}

	@Override
	public Long incrMetricNum(Meta metricDayMeta, String secondField, long incrNum) {
		Jedis jedis=null;
		try {
			jedis = this.jedisPool.getResource();
			byte[] key=StorageStatisticsKeyManager.getMetricDayKey(metricDayMeta);
			byte[] field=ByteString.copyFrom(secondField, Constant.DEFAULT_CHARSET).toByteArray();
			return jedis.hincrBy(key, field, incrNum);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}finally {
			if(jedis!=null) {
				jedis.close();
			}
		}
		return null;
	}
	
	@Override
	public void incrMetricNum(List<MetricNumIncrement> metricNumIncrements) {
		if(metricNumIncrements==null||metricNumIncrements.isEmpty()) {
			return ;
		}
		Jedis jedis=null;
		try {
			jedis = this.jedisPool.getResource();
			Pipeline pipeline=jedis.pipelined();
			for (MetricNumIncrement metricNumIncrement : metricNumIncrements) {
				byte[] key=StorageStatisticsKeyManager.getMetricDayKey(metricNumIncrement.getMetricDayMeta());
				byte[] field=ByteString.copyFrom(metricNumIncrement.getSecondField(), Constant.DEFAULT_CHARSET).toByteArray();
				pipeline.hincrBy(key, field, metricNumIncrement.getIncrNum());
			}
			pipeline.sync();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}finally {
			if(jedis!=null) {
				jedis.close();
			}
		}
		
	}


	@Override
	public Long getMetricNum(Meta metricDayMeta, String secondField) {
		Jedis jedis=null;
		try {
			jedis = this.jedisPool.getResource();
			byte[] key=StorageStatisticsKeyManager.getMetricDayKey(metricDayMeta);
			byte[] field=ByteString.copyFrom(secondField, Constant.DEFAULT_CHARSET).toByteArray();
			byte[] val=jedis.hget(key, field);
			if(val==null) {
				return null;
			}
			return Long.parseLong(new String(val));
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage(),e);
		}finally {
			if(jedis!=null) {
				jedis.close();
			}
		}
		return null;
	}


	@Override
	public Map<String,Long> getMetricNum(Meta metricDayMeta) {
		Jedis jedis=null;
		try {
			jedis = this.jedisPool.getResource();
			byte[] key=StorageStatisticsKeyManager.getMetricDayKey(metricDayMeta);
			Map<byte[], byte[]> val=jedis.hgetAll(key);
			if(val==null) {
				return Collections.emptyMap();
			}
			Map<String,Long> ret=new HashMap<String,Long>();
			for (Entry<byte[],byte[]> entry: val.entrySet()) {
				String field=ByteString.copyFrom(entry.getKey()).toString(Constant.DEFAULT_CHARSET);
				Long fieldNum=Long.parseLong(new String(entry.getValue()));
				ret.put(field, fieldNum);
			}
			return ret;
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage(),e);
		}finally {
			if(jedis!=null) {
				jedis.close();
			}
		}
		return Collections.emptyMap();
	}

	
	
	
	



}