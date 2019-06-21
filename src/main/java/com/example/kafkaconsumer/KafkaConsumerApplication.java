package com.example.kafkaconsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

@SpringBootApplication
public class KafkaConsumerApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        pros.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pros.setProperty("group.id", "hello");
        pros.setProperty("enable.auto.commit", "false");
        pros.setProperty("max.poll.records", "1");

        Consumer<String, String> consumer = new KafkaConsumer(pros);
//        List<TopicPartition> topicPartitions = new LinkedList<>();
//        for (int i = 0; i < 3; i++) {
//            topicPartitions.add(new TopicPartition("hello", i));
//        }
//        consumer.seekToBeginning(topicPartitions);
        consumer.subscribe(Arrays.asList("hello"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records) {
                System.out.println("key = " + record.key() + ", value = " + record.value() + " at offset = " + record.offset() + " in partition = " + record.partition());
            }
            consumer.commitSync();
            Thread.sleep(1_000L);
        }

    }
}
