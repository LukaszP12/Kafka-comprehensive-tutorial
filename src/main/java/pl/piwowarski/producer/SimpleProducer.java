package pl.piwowarski.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer implements AutoCloseable{
    private final String topic;
    private final KafkaProducer<String, String> producer;

    public SimpleProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Safe & efficient defaults
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_768); // 32 KB

        // ---- Reliability settings ----
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // strongest delivery guarantee
        props.put(ProducerConfig.RETRIES_CONFIG, 5); // retry up to 5 times
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000); // wait 1s between retries

        // ---- Exactly-once safety ----
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // avoid duplicate messages
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // keeps ordering
        this.producer = new KafkaProducer<>(props);
    }

    public void sendSync(String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        RecordMetadata meta = producer.send(record).get();
        System.out.printf("Produced to %s-%d@%d key=%s value=%s%n", meta.topic(), meta.partition(), meta.offset(), key, value);
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}
