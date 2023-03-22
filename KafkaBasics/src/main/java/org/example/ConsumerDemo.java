package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Starting Kafka Consumer Demo");
        String topic = "java_demo2";
        String groupId = "my-java-application3";

        Properties properties = new Properties();

        // 0. Setup the bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // 1. Create Consumer Properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); // [none/earliest/latest]

        // 2. Create a consumer with the consumer properties
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 3. Subscribe to a topic (list not necessary here, used when subscribing to several topics.)
        consumer.subscribe(Arrays.asList(topic));

        // 4. Poll for data
        while (true) {
            log.info("Polling...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record: records) {
                log.info("1-Key: "+record.key()+",\t2-Value: "+record.value());
                log.info("3-Partition: "+record.partition()+",\t4-Offset: "+record.offset());
            }

        }
    }
}
