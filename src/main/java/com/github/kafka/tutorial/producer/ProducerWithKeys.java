package com.github.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeys {

    private static Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String BOOTSTRAP_SERVERS = "localhost:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
            // create a producer record
            String topic = "first_topic";
            String key = "id_" + i;
            String value = "Hello Kafka! " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("Key:" + key);
            // send data - synchronous (!) just for testing the keys
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata. \n " +
                            "Topic: " + recordMetadata.topic() + "\n " +
                            "Partition: " + recordMetadata.partition() + "\n " +
                            "Offset: " + recordMetadata.offset() + "\n " +
                            "Timestamp: " + recordMetadata.timestamp()
                    );
                } else {
                    logger.error("Error while producing", e);
                }
            }).get(); // blocking (!) to check records with equal keys go to the same partition
            // partitions: 1 0 2 0 2 - every time the same
        }
        // flush data
        producer.flush();
        // flush data and close the producer
        producer.close();
    }
}
