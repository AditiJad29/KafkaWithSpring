package com.example.kafkaspring;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-app");
        kafkaProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:29092");
        kafkaProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().toString());
        kafkaProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().toString());
        kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProperties.setProperty(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        //Build Kafka topology
        streamsBuilder.<String,String>stream("sentences")
                .flatMapValues((readOnlyKey, value)->
                    Arrays.asList(value.toLowerCase().split(" ")))
                .groupBy((key,value) -> value)
                .count(Materialized.with(Serdes.String(),Serdes.Long()))
                .toStream()
                .to("word-count", Produced.with(Serdes.String(),Serdes.Long()));

        //Configure Kafka Streams
        KafkaStreams kStream = new KafkaStreams(streamsBuilder.build(),kafkaProperties);
        kStream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kStream::close));
    }
}
