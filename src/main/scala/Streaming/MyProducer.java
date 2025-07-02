//package Streaming;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.IntegerSerializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import java.util.Properties;
//
//import java.util.Properties;
//
//
//public class MyProducer {
//    public static void main(String[] args) {
//
//        Properties props = new Properties();
//        props.put(ProducerConfig.CLIENT_ID_CONFIG,ConstantsConfig.appId);
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,ConstantsConfig.bootstrapId);
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
//
//        producer.send(new ProducerRecord<Integer, String>(ConstantsConfig.topicname,4,"4,2013-07-25T00:00:00.000-04:00,8827,CLOSED")); 	 	//message.timestamp.type=1 		//Log Append Time
//
//        producer.close();
//
//    }
//}
//
////Input Data
//
////1,2013-07-25T00:00:00.000-04:00,11599,CLOSED
////2,2013-07-25T00:00:00.000-04:00,256,PENDING_PAYMENT
////3,2013-07-25T00:00:00.000-04:00,12111,COMPLETE
////4,2013-07-25T00:00:00.000-04:00,8827,CLOSED
////5,2013-07-25T00:00:00.000-04:00,11318,COMPLETE
////6,2013-07-25T00:00:00.000-04:00,7130,COMPLETE
////7,2013-07-25T00:00:00.000-04:00,4530,COMPLETE
////8,2013-07-25T00:00:00.000-04:00,2911,PROCESSING
////9,2013-07-25T00:00:00.000-04:00,5657,PENDING_PAYMENT