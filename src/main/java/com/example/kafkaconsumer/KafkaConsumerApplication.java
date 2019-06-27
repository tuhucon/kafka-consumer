package com.example.kafkaconsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
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
import java.time.LocalDateTime;
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
        pros.setProperty("group.id", "collection1");
        pros.setProperty("enable.auto.commit", "false");
        pros.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.StickyAssignor");
//        pros.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor");
        pros.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        pros.setProperty("max.poll.records", "1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(pros);
        consumer.subscribe(Arrays.asList("hello", "tuhucon"));

        String topic = "";
        String partition = "";
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            System.out.println("record count: " + records.count());
            int hello_count = 0;
            int tuhucon_count = 0;
            for (ConsumerRecord<String, String> record: records) {
//                System.out.println("key = " + record.key() + ", value = " + record.value() + " at offset = " + record.offset()
//                                    + " in partition = " + record.partition() + " of topic = " + record.topic());
                if (record.topic().equals(topic) == false) {
                    System.out.println(LocalDateTime.now());
                    topic = record.topic();
                    partition = record.partition() + "";
                } else if (partition.equals(record.partition() + "") == false) {
                    System.out.println(LocalDateTime.now());
                    partition = record.partition() + "";
                }


                if (record.topic().equals("hello")) {
                    hello_count++;
                } else {
                    tuhucon_count++;
                }
            }
            consumer.commitSync();
            System.out.println("topic = " + topic + " parition = " + partition);
            System.out.println("hello count: " + hello_count);
            System.out.println("tuhucon count: " + tuhucon_count);
            Thread.sleep(1_000L);
        }

    }
}
