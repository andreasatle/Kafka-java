package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Starting Kafka Producer Demo With Callback");

        Properties properties = new Properties();

        // 0. Setup the bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // 1. Create Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        String topic = "java_demo2";
        // 2. Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 20; i++) {
                String key = "id-" + i;
                String value = "hello-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

                // 3. Send data with a callback function
                producer.send(record, new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception e) {
                                if (e == null) {
                                    log.info("Key: " + key + "\t" + "Partition: " + metadata.partition());
                                }
                            }
                        }
                );
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        // 4. Flush and Close the Producer
        producer.flush();
        producer.close();
    }
}
