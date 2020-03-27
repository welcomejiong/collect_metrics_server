package collect_metrics_server_web;

import java.util.Date;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.Test;

public class TestDate {
	
	@Test
	public void testInstall() throws Exception {
		Date d=new 	Date();
		d.setTime(1585278281023l);
		System.out.println(DateFormatUtils.format(d, "yyyy-MM-dd HH:mm:ss"));
	}
	
	
}
