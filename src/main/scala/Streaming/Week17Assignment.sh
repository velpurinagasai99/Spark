#You all must have seen Artel’s zero complaint advertisement. Our assignment is the same. You need to create java producer and consumer. Producer will send number of complaints
#received, if complaint is solved then producer can send -1 message and consumer will consume and show the sum of total active complaints.
#
#
#
# File-1
# package Streaming;
#import org.apache.kafka.clients.consumer.ConsumerConfig;
#import org.apache.kafka.clients.consumer.ConsumerRecord;
#import org.apache.kafka.clients.consumer.ConsumerRecords;
#import org.apache.kafka.clients.consumer.KafkaConsumer;
#import org.apache.kafka.clients.producer.KafkaProducer;
#import org.apache.kafka.clients.producer.ProducerConfig;
#import org.apache.kafka.clients.producer.ProducerRecord;
#import org.apache.kafka.common.serialization.IntegerSerializer;
#import org.apache.kafka.common.serialization.StringSerializer;
#import org.apache.kafka.common.serialization.IntegerDeserializer;
#import org.apache.kafka.common.serialization.StringDeserializer;
#import java.util.Properties;
#import java.util.Arrays;
#
#public class MyConsumerandProducer2 {
#    public static void main(String[] args) {
#        Properties consumerProps = new Properties();
#        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG,ConstantsConfig.appId);
#        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,ConstantsConfig.bootstrapId);
#        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
#        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
#        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-Group1");
#        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
#
#        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(consumerProps);
#        consumer.subscribe(Arrays.asList("all_orders"));
#
#        Properties producerProps = new Properties();
#        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG,ConstantsConfig.appId);
#        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,ConstantsConfig.bootstrapId);
#        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
#        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
#
#        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(producerProps);
#
#        while(true){
#            ConsumerRecords<Integer, String> records = consumer.poll(1000);
#            for (ConsumerRecord<Integer, String> record: records ){
#                if(record.value().split(",")[3].equals("CLOSED"))
#                    producer.send(new ProducerRecord<Integer, String>("topicClosedOrders",record.key(),record.value()));
#                else
#                    producer.send(new ProducerRecord<Integer, String>("topicCompletedOrders",record.key(),record.value()));
#            }
#        }
#    }
#}







#File-2
#package Streaming;
#
#import org.apache.kafka.clients.producer.KafkaProducer;
#import org.apache.kafka.clients.producer.ProducerConfig;
#import org.apache.kafka.clients.producer.ProducerRecord;
#import org.apache.kafka.common.serialization.IntegerSerializer;
#import org.apache.kafka.common.serialization.StringSerializer;
#import java.util.Properties;
#
#import java.util.Properties;
#
#
#public class MyProducer {
#    public static void main(String[] args) {
#
#        Properties props = new Properties();
#        props.put(ProducerConfig.CLIENT_ID_CONFIG,ConstantsConfig.appId);
#        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,ConstantsConfig.bootstrapId);
#        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
#        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
#
#        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
#
#        producer.send(new ProducerRecord<Integer, String>(ConstantsConfig.topicname,4,"4,Andhra Pradesh,CLOSED")); 	 	//message.timestamp.type=1 		//Log Append Time
#
#        producer.close();
#
#    }
#}
#
#//Input Data
#
#//1,Andhra Pradesh,CLOSED
#//2,Uttar Pradesh,PENDING_PAYMENT
#//3,Bihar,COMPLETE
#//4,Andhra Pradesh,CLOSED
#//5,Bihar,COMPLETE
#//6,Andhra Pradesh,COMPLETE
#//7,Uttar Pradesh,COMPLETE
#//8,Andhra Pradesh,PROCESSING
#//9,Uttar Pradesh,PENDING_PAYMENT
#//10,Bihar,COMPLETE
#//11,Andhra Pradesh,CLOSED
#//12,Uttar Pradesh,PENDING_PAYMENT
#//13,Bihar,COMPLETE
#//14,Andhra Pradesh,CLOSED
#//15,Bihar,COMPLETE
#//16,Andhra Pradesh,COMPLETE
#//17,Uttar Pradesh,COMPLETE
#//18,Andhra Pradesh,PROCESSING
#//19,Uttar Pradesh,PENDING_PAYMENT
