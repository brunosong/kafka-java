package com.practice.kafka.producer;

import com.practice.kafka.event.FileEventHandler;
import com.practice.kafka.event.FileEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class FileAppendProducer {

    public static final Logger logger = LoggerFactory.getLogger(FileAppendProducer.class);

    public static void main(String[] args) {

        String topicName = "file-topic";

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String filePath = "C:\\DEV\\STUDY\\KAFKA\\KafkaProj-01\\practice\\src\\main\\resources\\pizza_append.txt";
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        FileEventSource fileEventSource = new FileEventSource(1000, new File(filePath),
                new FileEventHandler(kafkaProducer,topicName,true));

        Thread fileThread = new Thread(fileEventSource);
        fileThread.start();

        try {
            fileThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } finally {
            kafkaProducer.close();
        }

    }


}
