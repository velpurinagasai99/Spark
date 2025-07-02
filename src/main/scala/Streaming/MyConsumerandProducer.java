//package Streaming;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.IntegerSerializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.kafka.common.serialization.IntegerDeserializer;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import java.util.Properties;
//import java.util.Arrays;
//
//public class MyConsumerandProducer {
//    public static void main(String[] args) {
//        Properties consumerProps = new Properties();
//        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG,ConstantsConfig.appId);
//        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,ConstantsConfig.bootstrapId);
//        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
//        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-Group1");
//        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(consumerProps);
//        consumer.subscribe(Arrays.asList("all_orders"));
//
//        Properties producerProps = new Properties();
//        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG,ConstantsConfig.appId);
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,ConstantsConfig.bootstrapId);
//        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(producerProps);
//
//        while(true){
//            ConsumerRecords<Integer, String> records = consumer.poll(1000);
//            for (ConsumerRecord<Integer, String> record: records ){
//                if(record.value().split(",")[3].equals("CLOSED"))
//                    producer.send(new ProducerRecord<Integer, String>("topicClosedOrders",record.key(),record.value()));
//                else
//                    producer.send(new ProducerRecord<Integer, String>("topicCompletedOrders",record.key(),record.value()));
//            }
//        }
//    }
//}
