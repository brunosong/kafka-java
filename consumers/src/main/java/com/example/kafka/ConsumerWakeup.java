package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeup {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerWakeup.class);


    static class HookThread extends Thread {
        @Override
        public void run() {
            System.out.println("Hook Run");
        }
    }

    public static void main(String[] args) {

        String topicName = "simple-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //그룹 아이디를 설정해 줘야 한다.
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");


        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(props);

        //토픽 명을 넣어주면 된다. 컬렉션도 들어가진다.
        kafkaConsumer.subscribe(List.of(topicName));

        //메인스레드를 가져온다.
        Thread mainThread = Thread.currentThread();

        //메인스레드가 죽을때 사용된다.
        Runtime.getRuntime().addShutdownHook( new Thread() {
            @Override
            public void run() {
                logger.info("main program shutdown");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) { e.printStackTrace(); }

            }

        });

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000)); //1초동안 기다려라 (폴 전용 스레드가 동작한다)
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record.key : {} , record.value : {} , record.partition : {} , recode.offset : {}" ,
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }

        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");

        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }

    }

}
