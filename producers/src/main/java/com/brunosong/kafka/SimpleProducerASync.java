package com.brunosong.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class SimpleProducerASync {

    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerASync.class);

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
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topicName,"hello world10");

        // KafkaProducer 메세지 send (callBack 선언)
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if(exception == null) {
                    logger.info("######### record metadata received ########### \n" +
                            "partition : " + metadata.partition() + "\n" +
                            "offset : " + metadata.offset() + "\n" +
                            "timestamp : " + metadata.timestamp());
                } else {

                    logger.error("exception error from broker {}", exception.getMessage() );

                }

        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.flush();
        kafkaProducer.close();

    }

}
