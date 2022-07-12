package com.github.kafka.tutorial.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private static Logger logger = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {

        // create consumer configs
        Properties properties = new Properties();
        String BOOTSTRAP_SERVERS = "localhost:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe a consumer to our topic(s)
        consumer.subscribe(Collections.singleton(topic));

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            // reads partitions starting from 0
            for (ConsumerRecord<String, String> record : consumerRecords) {
                logger.info("Key:" + record.key() + ", Value:" + record.value());
                logger.info("Partition:" + record.partition() + ", Offset:" + record.offset());
            }
        }
    }
}
