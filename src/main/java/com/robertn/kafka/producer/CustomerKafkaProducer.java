package com.robertn.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class CustomerKafkaProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();

        // 1) Cluster
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");

        // 2) Serialization
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 3) Reliability (Level 1 sane defaults)
        props.put(ProducerConfig.ACKS_CONFIG, "all");                // strongest durability
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // avoid duplicates per partition
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            String topic = "onboarding.customers";

            String key = "customer-123";
            String value = """
                    {
                      "customerId": 123,
                      "eventType": "CREATED",
                      "timestamp": "2025-12-01T10:15:30Z"
                    }
                    """;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);

            Future<RecordMetadata> future = producer.send(record);
            // block just so we see the result in this demo
            RecordMetadata meta = future.get();
            System.out.printf(
                    "Sent record to topic=%s partition=%d offset=%d%n",
                    meta.topic(), meta.partition(), meta.offset()
            );
        }
    }
}
