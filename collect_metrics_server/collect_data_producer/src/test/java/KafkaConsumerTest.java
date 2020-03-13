import org.corps.bi.datacenter.producer.DataCenterTopicProducer;
import org.corps.bi.datacenter.producer.core.Constant;
import org.junit.Test;

public class KafkaConsumerTest {
	
	public KafkaConsumerTest() {
		super();
	}

	@Test
	public void testConsumer(){
		Constant.init();
		DataCenterTopicProducer<String, byte[]> consumer=new DataCenterTopicProducer<String, byte[]>("",Constant.getInstance().getKafkaProducerConfigProperties()); 
		consumer.send(null, null);
	}

}
