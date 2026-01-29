package ai.superstream.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * Example application that uses the Kafka Clients API to consume messages.
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar
 * -Dlogback.configurationFile=logback.xml -jar
 * kafka-clients-example-1.0.0-jar-with-dependencies.jar
 *
 * Prerequisites:
 * 1. A Kafka server with the following topics:
 * - superstream.metadata_v1 - with a configuration message
 * - superstream.clients - for client reports
 * - example-topic - for test messages (created by KafkaProducerExample)
 *
 * Environment variables:
 * - KAFKA_BOOTSTRAP_SERVERS: The Kafka bootstrap servers (default:
 * localhost:9092)
 * - SUPERSTREAM_TOPICS_LIST: Comma-separated list of topics to optimize for
 * (default: example-topic)
 */
public class KafkaConsumerExample {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerExample.class);

    // === Configuration Constants ===
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CONSUMER_GROUP_ID = "superstream-example-consumer-group";
    private static final String CLIENT_ID = "superstream-example-consumer";
    private static final String TOPIC_NAME = "example-topic";

    public static void main(String[] args) {
        System.out.println("=== Superstream KafkaConsumer Example ===");
        System.out.println("This example demonstrates the Superstream agent intercepting KafkaConsumer operations");
        System.out.println();

        // Build consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");

        System.out.println("Consumer configuration:");
        System.out.println("  Bootstrap servers: " + DEFAULT_BOOTSTRAP_SERVERS);
        System.out.println("  Group ID: " + CONSUMER_GROUP_ID);
        System.out.println("  Client ID: " + CLIENT_ID);
        System.out.println("  Topic: " + TOPIC_NAME);
        System.out.println();

        // Create KafkaConsumer - this should trigger the interceptor
        System.out.println("Creating KafkaConsumer (should trigger Superstream interceptor)...");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        System.out.println("KafkaConsumer created successfully");
        System.out.println();

        try {
            // Subscribe to the topic
            System.out.println("Subscribing to topic: " + TOPIC_NAME);
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            System.out.println("Subscribed successfully");
            System.out.println();

            System.out.println("Starting to poll for messages...");
            System.out.println("Note: Each poll() call should trigger the message consumed interceptor");
            System.out.println();

            long startTime = System.currentTimeMillis();
            int totalMessagesConsumed = 0;
            int pollCount = 0;

            while (true) {
                pollCount++;
                System.out.println("--- Poll #" + pollCount + " ---");
                
                // Poll for messages - this should trigger the onPollExit interceptor
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                
                if (records.isEmpty()) {
                    System.out.println("No messages received in this poll");
                } else {
                    System.out.println("Received " + records.count() + " messages in this poll");
                    
                    for (ConsumerRecord<String, String> record : records) {
                        totalMessagesConsumed++;
                        System.out.printf("Application processed message %d: topic=%s, partition=%d, offset=%d, key=%s%n",
                                totalMessagesConsumed, record.topic(), record.partition(), record.offset(), record.key());
                        
                        // Log message value only in debug mode to avoid spam
                        logger.debug("Message value: {}", record.value());
                    }
                }

                System.out.println("Total messages consumed so far: " + totalMessagesConsumed);
                
                // Run for about 2 minutes then exit
                if (System.currentTimeMillis() - startTime > 120000) {
                    System.out.println("Demo completed after 2 minutes");
                    break;
                }
                
                System.out.println();
                Thread.sleep(2000); // Wait 2 seconds between polls
            }

        } catch (Exception e) {
            logger.error("Error in consumer", e);
            System.err.println("Error: " + e.getMessage());
        } finally {
            System.out.println("Closing KafkaConsumer...");
            consumer.close();
            System.out.println("Consumer closed successfully");
            System.out.println();
            System.out.println("=== Demo Complete ===");
        }
    }
} 