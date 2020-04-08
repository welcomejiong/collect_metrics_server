import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.corps.bi.datacenter.connect.core.Constant;
import org.corps.bi.metrics.Dau;
import org.corps.bi.metrics.converter.DauConverter;
import org.corps.bi.tools.util.JSONUtils;
import org.junit.Test;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;

public class KafkaConsumerTest {
	
	public KafkaConsumerTest() {
		super();
	}

	@Test
	public void testConsumer(){
		Constant.init();
//		MetricTopicConsumer<String, byte[]> consumer=new MetricTopicConsumer<String, byte[]>(Constant.getInstance().getKafkaConsumerConfigProperties()); 
//		consumer.subscribe();
	}

	
	@Test
	public void testProtobuf() {
		int i=0;
		Date now=new Date();
		Dau dau=new Dau();
		dau.setClientId(i+"");
		dau.setAffiliate("affiliate_"+i);
		dau.setCreative("creative_"+i);
		dau.setDauDate(DateFormatUtils.format(DateUtils.addDays(now, i),"yyyy-MM-dd"));
		dau.setDauTime(DateFormatUtils.format(DateUtils.addDays(now, i),"hh:mm:ss"));
		dau.setFamily("family_"+i);
		dau.setFromUid("f_u_id-"+i);
		dau.setGenus("genus_"+i);
		dau.setIp("193.112.30.49");
		dau.setSource("source_"+i);
		dau.setUserId("userid_"+i);

		Map<String,String> extraMap=new HashMap<String,String>();
		extraMap.put("username", "username_"+i);
		extraMap.put("password", "password_"+i);
		dau.setExtra(JSONUtils.toJSON(extraMap));
		DauConverter dauConverter=new DauConverter(dau);
		GeneratedMessageV3 message=dauConverter.copyTo();
		final List<Descriptors.FieldDescriptor> fieldDescriptorList = message.getDescriptorForType().getFields();
	    for (Descriptors.FieldDescriptor descriptor : fieldDescriptorList) {
	     System.out.println(descriptor.getFullName()+":"+descriptor.getName());
	     Object obj=message.getField(descriptor);
	     System.out.println(obj.toString());
	    }
	    System.out.println(message.getClass().getName());
	    
	}
	
	
	
	@Test
	public void testFileSize(){
		File file=new File("/Users/guojianjiong/work/app_datas/data_center/kafka_consumer_to_hdfs_datas/counter/succed/2020-01-02/counter_1_1_2020-01-02_14_1577933371507_15.proto.rowdata");
		System.out.println(FileUtils.sizeOf(file));
	}
	
}
