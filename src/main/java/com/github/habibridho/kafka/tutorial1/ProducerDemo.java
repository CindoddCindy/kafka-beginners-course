package com.github.habibridho.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {
            String topic = "first_topic";
            String key = "id_" + i;
            String value = "Message produced from java " + i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info(String.format("Topic: %s\nKey: %s\nPartition: %s\nOffset: %s\nTimestamp: %s",
                            metadata.topic(), key, metadata.partition(), metadata.offset(), metadata.timestamp()));
                } else {
                    logger.error("Error producing message", exception);
                }
            });
        }

        producer.flush();
        producer.close();
    }

}
