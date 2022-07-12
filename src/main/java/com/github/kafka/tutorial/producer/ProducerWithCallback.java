package com.github.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    private static Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {
        String BOOTSTRAP_SERVERS = "localhost:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 100; i++) {
            // create a producer record
            String topic = "first_topic";
            String value = "Hello Kafka! " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);

            // send data - asynchronous
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
            });
        }
        // flush data
        producer.flush();
        // flush data and close the producer
        producer.close();
    }
}
