package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler {

    public static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);

    private KafkaProducer<String,String> kafkaProducer;
    private String topicName;
    private boolean sync;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.sync = sync;
        this.topicName = topicName;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topicName, messageEvent.key, messageEvent.value);
        if(sync) {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("######### record metadata received ########### \n" +
                    "partition : " + recordMetadata.partition() + "\n" +
                    "offset : " + recordMetadata.offset() + "\n" +
                    "timestamp : " + recordMetadata.timestamp());
        } else {
            kafkaProducer.send(producerRecord , (metadata, exception) -> {
                if(exception == null) {
                    logger.info("######### record metadata received ########### \n" +
                            "partition : " + metadata.partition() + "\n" +
                            "offset : " + metadata.offset() + "\n" +
                            "timestamp : " + metadata.timestamp());
                } else {
                    logger.error("exception error from broker {}", exception.getMessage() );
                }
            });
        }

    }

}
