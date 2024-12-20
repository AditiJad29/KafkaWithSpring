package com.tutorial.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        System.out.println("Hello World!!");
        //Create and initialize Producer Properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer", StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

        ProducerRecord<String,String> record =  new ProducerRecord<>("first_topic","Hello World!");
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
