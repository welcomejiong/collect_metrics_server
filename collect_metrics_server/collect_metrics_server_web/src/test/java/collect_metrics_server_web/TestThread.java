package collect_metrics_server_web;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.corps.bi.recording.clients.rollfile.RollFileClient.SystemThreadFactory;
import org.junit.Test;

public class TestThread {
	
	private ScheduledExecutorService transporterIntervalService = Executors.newScheduledThreadPool(1,new SystemThreadFactory("metric-test-scheduled"));
	

	@Test
	public void testInstall() throws Exception {
		
		class TestRunable implements Runnable{
			
			private boolean isGo=true;

			@Override
			public void run() {
				if(!isGo) {
					System.out.println("isGo is false return....");
					return ;
				}
				System.out.println("isGo is true normal going...");
			}

			public boolean isGo() {
				return isGo;
			}

			public void setGo(boolean isGo) {
				this.isGo = isGo;
			}
			
		}
		
		TestRunable testRunable=new TestRunable();
		
		this.transporterIntervalService.scheduleAtFixedRate(testRunable,1000, 1000, TimeUnit.MILLISECONDS);
		
		TimeUnit.SECONDS.sleep(5);
		
		testRunable.setGo(false);
		
		System.out.println("set isGo to false");
		
		TimeUnit.SECONDS.sleep(5);
		
		testRunable.setGo(true);
		
		System.out.println("set isGo to true");
		
		TimeUnit.HOURS.sleep(1);
		
	}
}
