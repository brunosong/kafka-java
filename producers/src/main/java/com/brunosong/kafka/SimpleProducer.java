package com.brunosong.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {

        String topicName = "simple-topic";

        /* 카푸카 프로듀서 컨피그 레이션 설정 */

        // 굳히 Properties 를 쓰지 않아도 가능하다 ( 맵으로 가능 )
        Properties properties = new Properties();

        // bootstrap.servers , key.serializer.class, value.serializer.class
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");  //properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // KafkaProducer 객체 생성 <키,벨류>
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        // ProducerRecord 객체 생성 ( 메시지 객체 )
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topicName,"hello world2");

        // KafkaProducer 메세지 send
        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();

    }

}
