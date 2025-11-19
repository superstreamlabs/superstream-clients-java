package ai.superstream.examples.shadow;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseOverloadsExample {
    private static final Logger LOG = LoggerFactory.getLogger(CloseOverloadsExample.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "shadow-close-overloads");

        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        try {
            producer.close(Duration.ofMillis(1000));
            LOG.info("Invoked close(Duration)");
        } finally {
            try { producer.close(); } catch (Throwable ignored) { } // shadow should log that it already closed
        }
    }
}


