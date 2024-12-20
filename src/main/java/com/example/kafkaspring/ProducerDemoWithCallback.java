package com.tutorial.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        //Create and initialize Producer Properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer", StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

        ProducerRecord<String,String> record =  new ProducerRecord<>("first_topic","Hello World!");
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executed every time a record is successfully sent or an exception is thrown
                if (e==null){
                    logger.info("Received new metadata. \n" + recordMetadata.topic() + "\n" + recordMetadata.partition() + "\n" + recordMetadata.offset() + "\n" + recordMetadata.timestamp());
                }else {
                    logger.error("Error while producing", e);
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
