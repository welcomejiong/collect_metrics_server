package org.corps.bi.datacenter.server.statistics.core;

import org.corps.bi.metrics.Meta;
import org.corps.bi.metrics.converter.MetaConverter;
import org.corps.bi.protobuf.common.StorageKeyV2Proto;

import com.google.protobuf.ByteString;

public class StorageStatisticsKeyManager {

	private static final Long GLOBLE_USER_ID = 0L;

	private static final Long METRIC_DAY_BUSIFLAG = 10000L;

	public static byte[] getMetricDayKey(Meta metricDayMeta) {
		MetaConverter metaConverter = new MetaConverter(metricDayMeta);
		StorageKeyV2Proto.Builder builder = StorageKeyV2Proto.newBuilder();
		builder.setUserId(GLOBLE_USER_ID);
		builder.setBusiFlag(METRIC_DAY_BUSIFLAG);
		builder.setKeyFlag(0);
		builder.setExtra(ByteString.copyFrom(metaConverter.toByteArray()));
		return builder.build().toByteArray();
	}

	public static byte[] long2bytes(long input) {
		byte[] result = new byte[8];
		for (int i = 7; i >= 0; i--) {
			result[i] = (byte) (input & 0xff);
			input >>= 8;
		}
		return result;
	}

	public static long bytes2long(byte[] bytes) {
		long result = 0;
		result |= (bytes[0] & 0xff);
		for (int i = 0; i < 8; i++) {
			result <<= 8;
			result |= (bytes[i] & 0xff);
			
		}
		return result;
	}
	

}
