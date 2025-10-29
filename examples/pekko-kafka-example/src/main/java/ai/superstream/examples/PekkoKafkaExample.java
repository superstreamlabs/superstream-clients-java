package ai.superstream.examples;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.kafka.ProducerMessage;
import org.apache.pekko.kafka.ProducerSettings;
import org.apache.pekko.kafka.javadsl.Producer;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.NotUsed;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Example application that uses Apache Pekko Connectors Kafka to produce messages.
 *
 * Run with:
 *   java -Dlogback.configurationFile=logback.xml -jar pekko-kafka-example-1.0.0-jar-with-dependencies.jar
 *
 * Prerequisites:
 * 1. A Kafka broker. Topics:
 *    - example-topic (for test messages)
 *
 * Environment variables:
 * - KAFKA_BOOTSTRAP_SERVERS: The Kafka bootstrap servers (default: localhost:9092)
 */
public class PekkoKafkaExample {
    private static final Logger logger = LoggerFactory.getLogger(PekkoKafkaExample.class);

    public static void main(String[] args) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = "localhost:9092";
        }

        ActorSystem system = ActorSystem.create("pekko-kafka-example");

        try {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            org.apache.kafka.clients.producer.Producer<String, String> kafkaProducer = new KafkaProducer<>(configProps);

            ProducerSettings<String, String> producerSettings =
                    ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
                            .withProducer(kafkaProducer);

            String topic = "example-topic";
            logger.info("Sending messages to topic {}", topic);

            ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, "Hello from Pekko Kafka Example!");
            ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, "user-1", "Message for user 1");
            ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, "user-2", "Message for user 2");

            CompletableFuture<Void> completionFuture = Source.fromJavaStream(() -> java.util.stream.Stream.of(
                            ProducerMessage.single(record1),
                            ProducerMessage.single(record2),
                            ProducerMessage.single(record3)
                    ))
                    .via(Producer.flexiFlow(producerSettings))
                    .runForeach(result -> {
                        if (result instanceof ProducerMessage.Results) {
                            @SuppressWarnings("unchecked")
                            ProducerMessage.Results<String, String, NotUsed> res =
                                    (ProducerMessage.Results<String, String, NotUsed>) result;
                            logger.info("Message sent");
                        }
                    }, system)
                    .toCompletableFuture()
                    .thenApply(done -> null);

            completionFuture.get(10, TimeUnit.SECONDS);
            logger.info("Messages sent successfully!");
        } catch (Exception e) {
            logger.error("Error in Pekko Kafka example", e);
        } finally {
            system.terminate();
            try {
                system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Error terminating actor system", e);
            }
        }
    }
}

