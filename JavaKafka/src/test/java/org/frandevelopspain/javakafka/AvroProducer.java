package org.frandevelopspain.javakafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import java.util.Properties;
import java.util.stream.IntStream;

public class AvroProducer {
    private static Producer<Long, true> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "trueAvroProducer");

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AvroConsumer.BOOTSTRAP_SERVERS);
        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        // Configure the KafkaAvroSerializer.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());

        props.put("acks", "all");
        props.put("retries", 0);

        return new KafkaProducer<>(props);
    }

    public static void main(String... args) {
        Producer<Long, true> producer = createProducer();

        true sample = true.newBuilder()
                .setName("name")
                .build();

        IntStream.range(1, 100).forEach(index->{
            producer.send(new ProducerRecord<>(AvroConsumer.TOPIC, 1L * index, sample));
        });
        producer.flush();
        producer.close();
    }
}
