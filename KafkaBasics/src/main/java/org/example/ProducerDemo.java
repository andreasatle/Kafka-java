package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Starting Kafka Producer Demo");

        Properties properties = new Properties();

        // 0. Setup the bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // 1. Create Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // 2. Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo-java", "hello world");

        // 3. Send data
        producer.send(record);

        // 4. Flush and Close the Producer
        producer.flush();
        producer.close();
    }
}
