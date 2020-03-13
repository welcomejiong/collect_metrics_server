package collect_metrics_server_web;

import java.util.HashMap;
import java.util.Map;

import org.corps.bi.tools.util.HttpClientUtils;
import org.junit.Test;

public class TestCollect {
	
	@Test
	public void testInstall() throws Exception {
		Map<String,String> params=new HashMap<String, String>();
		params.put("metric", "install");
		params.put("snId", "1");
		params.put("gameId", "1");
		params.put("clientId", "1");
		params.put("ds", "2019-08-12");
		params.put("userId", "12391");
		params.put("udid", "kakdfi2ekdja-023kdfms");
		params.put("roleid", "1007");
		params.put("source", "1");
		params.put("affiliate", "1");
		params.put("creative", "1");
		params.put("family", "1");
		params.put("genus", "1");
		params.put("fromUid", "1");
		params.put("installDate", "2019-08-12");
		params.put("installTime", "10:23:01");
		String url="http://127.0.0.1:8888/collect_metrics_server_web/coll/";
		String res=HttpClientUtils.executePostRequest(url, params, "UTF-8");
	}
	
	@Test
	public void testDau() throws Exception {
		Map<String,String> params=new HashMap<String, String>();
		params.put("metric", "dau");
		params.put("snId", "1");
		params.put("gameId", "1");
		params.put("clientId", "1");
		params.put("ds", "2019-08-16");
		params.put("userId", "12391");
		params.put("udid", "kakdfi2ekdja-023kdfms");
		params.put("roleid", "1007");
		params.put("source", "1");
		params.put("affiliate", "1");
		params.put("creative", "1");
		params.put("family", "1");
		params.put("genus", "1");
		params.put("ip", "192.168.2.30");
		params.put("dauDate", "2019-08-12");
		params.put("dauTime", "10:23:01");
		String url="http://127.0.0.1:8080/collect_metrics_server_web/coll/";
		String res=HttpClientUtils.executePostRequest(url, params, "UTF-8");
	}
	
	@Test
	public void testCounter() throws Exception {
		Map<String,String> params=new HashMap<String, String>();
		params.put("metric", "counter");
		params.put("snId", "1");
		params.put("gameId", "1");
		params.put("clientId", "1");
		params.put("ds", "2019-08-16");
		params.put("userId", "12391");
		params.put("udid", "kakdfi2ekdja-023kdfms");
		params.put("roleid", "1007");
		params.put("userLevel", "1");
		params.put("counter", "1");
		params.put("value", "1");
		params.put("kingdom", "1");
		params.put("phylum", "1");
		params.put("family", "1");
		params.put("classfield", "1");
		params.put("genus", "192.168.2.30");
		params.put("counterDate", "2019-08-12");
		params.put("counterTime", "10:23:01");
		String url="http://127.0.0.1:8080/collect_metrics_server_web/coll/";
		String res=HttpClientUtils.executePostRequest(url, params, "UTF-8");
	}
	
	@Test
	public void testPayment() throws Exception {
		Map<String,String> params=new HashMap<String, String>();
		params.put("metric", "payment");
		params.put("snId", "1");
		params.put("gameId", "1");
		params.put("clientId", "1");
		params.put("ds", "2019-08-16");
		params.put("userId", "12391");
		params.put("udid", "kakdfi2ekdja-023kdfms");
		params.put("roleid", "1007");
		params.put("amount", "1");
		params.put("currency", "RMB");
		params.put("provider", "TENCENT");
		params.put("ip", "192.168.2.30");
		params.put("transactionid", "1");
		params.put("status", "1");
		params.put("kingdom", "1");
		params.put("genus", "1");
		params.put("value2", "1");
		params.put("paymentDate", "2019-08-12");
		params.put("paymentTime", "10:23:01");
		String url="http://127.0.0.1:8080/collect_metrics_server_web/coll/";
		String res=HttpClientUtils.executePostRequest(url, params, "UTF-8");
	}

}
