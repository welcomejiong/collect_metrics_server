package org.corps.bi.datacenter.server.statistics.core;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.MutablePair;

public enum GetSecondField {
	
	HOURS {
		@Override
		public MutablePair<Date, Date> doParse(Calendar calendar, int window) {
			Calendar roundCalendar=DateUtils.truncate(calendar, Calendar.HOUR_OF_DAY);
			int hourOfDay=roundCalendar.get(Calendar.HOUR_OF_DAY);
			int splitHour=(hourOfDay/window);
			int beginWindowHour=splitHour*window;
			roundCalendar.set(Calendar.HOUR_OF_DAY, beginWindowHour);
			Date endDate=DateUtils.addHours(roundCalendar.getTime(), window);
			return new MutablePair<Date, Date>(roundCalendar.getTime(),endDate);
		}
	},
	MINUTES {
		@Override
		public MutablePair<Date, Date> doParse(Calendar calendar, int window) {
			Calendar roundCalendar=DateUtils.truncate(calendar, Calendar.MINUTE);
			int minutes=roundCalendar.get(Calendar.MINUTE);
			int splitMinutes=(minutes/window);
			int beginMinutes=splitMinutes*window;
			roundCalendar.set(Calendar.MINUTE, beginMinutes);
			Date endDate=DateUtils.addMinutes(roundCalendar.getTime(), window);
			return new MutablePair<Date, Date>(roundCalendar.getTime(),endDate);
		}
	},
	SECONDS {
		@Override
		public MutablePair<Date, Date> doParse(Calendar calendar, int window) {
			Calendar roundCalendar=DateUtils.truncate(calendar, Calendar.SECOND);
			int seconds=roundCalendar.get(Calendar.SECOND);
			int splitSeconds=(seconds/window);
			int beginSeconds=splitSeconds*window;
			roundCalendar.set(Calendar.SECOND, beginSeconds);
			Date endDate=DateUtils.addSeconds(roundCalendar.getTime(), window);
			return new MutablePair<Date, Date>(roundCalendar.getTime(),endDate);
		}
	};
	
	public String parse(Calendar calendar,int window) {
		MutablePair<Date, Date> mutablePair=this.doParse(calendar, window);
		StringBuilder sb=new StringBuilder();
		sb.append(DateFormatUtils.format(mutablePair.left, Constant.DEFAULT_FORMAT_DATE_SECONDS));
		sb.append("_").append(DateFormatUtils.format(mutablePair.right, Constant.DEFAULT_FORMAT_DATE_SECONDS));
		return sb.toString();
	}
	
	public abstract MutablePair<Date, Date> doParse(Calendar calendar,int window);
}
