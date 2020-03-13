import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

public class KafkaProducerTest {
	
	public KafkaProducerTest() {
		super();
	}

	@Test
	public void testProducer(){
		KafkaProducer<String, byte[]> producer=this.configProducer("127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
	    String topic="back_main_data";
	    try {
	    	for(int i=0;i<10;i++){
	    		producer.send(this.produce(topic));
	    	}
			producer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			producer.close();
		}
	}
	
	private KafkaProducer<String,byte[]> configProducer(String servers) {
        Properties props = new Properties();
        props.put("bootstrap.servers",servers);
        props.put("client.id", "DemoProducer");
        props.put("batch.size", 16384);//16M
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);//32M
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        KafkaProducer<String,byte[]> producer = new KafkaProducer<String,byte[]>(props);
        return producer;
    }
	
	
	private ProducerRecord<String, byte[]> produce(String topic) throws IOException{
//		BackMainDataProto.Builder protoBuilder=BackMainDataProto.newBuilder();
//		protoBuilder.setSnId(0);
//		protoBuilder.setGameId(1);
//		protoBuilder.setServerId("1");
//		Random random=new Random();
//		protoBuilder.setUserId("10000"+random.nextInt());
//		protoBuilder.setDs("20181129"+random.nextInt());
//		protoBuilder.setArtifactLevel("1"+random.nextInt());
//		protoBuilder.setBack("back001"+random.nextInt());
//		protoBuilder.setBossEquips("bossEquips"+random.nextInt());
//		protoBuilder.setChariot("chariot"+random.nextInt());
//		protoBuilder.setDing("ding"+random.nextInt());
//		protoBuilder.setEquipGem("equipGem"+random.nextInt());
//		protoBuilder.setExtra("exter"+random.nextInt());
//		protoBuilder.setFlagOfficialData("flagOfficial"+random.nextInt());
//		BackMainDataProto proto=protoBuilder.build();
////		byte[] protoBody=new byte[proto.getSerializedSize()];
////		protoBuilder.build().writeTo(CodedOutputStream.newInstance(protoBody));
//		ByteArrayOutputStream byteArrayOutputStream=new ByteArrayOutputStream();
//		proto.writeTo(byteArrayOutputStream);
		//byte[] protoBody=proto.toByteArray();
		ProducerRecord<String, byte[]> ret=new ProducerRecord<String, byte[]>(topic,null);
		return ret;
		
	}
	
	


}
