

import java.util.Calendar;

import org.corps.bi.datacenter.server.statistics.core.GetSecondField;
import org.junit.Test;

public class TestGetSecondField {
	

	@Test
	public void testHour() throws Exception {
		
		Calendar calendar=Calendar.getInstance();
		calendar.setTimeInMillis(System.currentTimeMillis());
		
		for (int i = 1; i <= 24; i++) {
			System.out.println("windowHour:"+i+"["+GetSecondField.HOURS.parse(calendar, i)+"]");
		}
		
	}
	
	
}
