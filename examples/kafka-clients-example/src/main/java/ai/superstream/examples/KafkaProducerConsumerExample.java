package ai.superstream.examples;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Combined example that demonstrates both KafkaProducer and KafkaConsumer
 * interceptors working together.
 * 
 * This example will:
 * 1. Create a KafkaProducer (triggers producer interceptor)
 * 2. Create a KafkaConsumer (triggers consumer interceptor) 
 * 3. Send messages with the producer
 * 4. Consume messages with the consumer
 * 
 * Run with:
 * java -javaagent:path/to/superstream-clients-1.0.0.jar
 * -Dlogback.configurationFile=logback.xml -jar
 * kafka-clients-example-1.0.0-jar-with-dependencies.jar
 *
 * You should see both interceptor messages:
 * - "hello world - KafkaConsumer intercepted!"
 * - Producer optimization messages from the producer interceptor
 */
public class KafkaProducerConsumerExample {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerConsumerExample.class);

    // === Configuration Constants ===
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "combined-example-topic";
    
    // Producer settings
    private static final String PRODUCER_CLIENT_ID = "combined-example-producer";
    private static final String COMPRESSION_TYPE = "zstd";
    private static final Integer BATCH_SIZE = 16384; // 16KB batch size
    
    // Consumer settings
    private static final String CONSUMER_CLIENT_ID = "combined-example-consumer";
    private static final String GROUP_ID = "combined-example-consumer-group";
    
    private static final int MESSAGE_COUNT = 10;

    public static void main(String[] args) {
        logger.info("=== Kafka Producer-Consumer Combined Example ===");
        logger.info("This example demonstrates both producer and consumer interceptors");
        logger.info("Watch for interceptor messages:");
        logger.info("  - 'hello world - KafkaConsumer intercepted!'");
        logger.info("  - Producer optimization messages");
        logger.info("");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        try {
            // Start consumer first to ensure it's ready
            logger.info("Starting consumer thread...");
            Future<?> consumerFuture = executor.submit(KafkaProducerConsumerExample::runConsumer);
            
            // Give consumer time to start up
            Thread.sleep(2000);
            
            // Start producer
            logger.info("Starting producer thread...");
            Future<?> producerFuture = executor.submit(KafkaProducerConsumerExample::runProducer);
            
            // Wait for producer to finish
            producerFuture.get();
            logger.info("Producer finished, consumer will continue running...");
            
            // Let consumer run for a bit longer to process all messages
            Thread.sleep(5000);
            
            // Cancel consumer and shutdown
            consumerFuture.cancel(true);
            
        } catch (Exception e) {
            logger.error("Error in combined example", e);
        } finally {
            executor.shutdownNow();
            logger.info("Example completed");
        }
    }

    /**
     * Run the producer in a separate thread.
     */
    private static void runProducer() {
        logger.info("=== PRODUCER: Creating KafkaProducer ===");
        
        Properties producerProps = createProducerProperties();
        
        try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            logger.info("PRODUCER: KafkaProducer created successfully");
            
            for (int i = 1; i <= MESSAGE_COUNT; i++) {
                String key = "key-" + i;
                String value = String.format("Message %d: %s - timestamp: %d", 
                                            i, generateSampleMessage(), System.currentTimeMillis());
                
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("PRODUCER: Failed to send message: {}", exception.getMessage());
                    } else {
                        logger.info("PRODUCER: Sent message to partition {} at offset {}", 
                                   metadata.partition(), metadata.offset());
                    }
                });
                
                // Small delay between messages
                Thread.sleep(500);
            }
            
            producer.flush();
            logger.info("PRODUCER: All {} messages sent successfully", MESSAGE_COUNT);
            
        } catch (Exception e) {
            logger.error("PRODUCER: Error", e);
        }
    }

    /**
     * Run the consumer in a separate thread.
     */
    private static void runConsumer() {
        logger.info("=== CONSUMER: Creating KafkaConsumer ===");
        
        Properties consumerProps = createConsumerProperties();
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            logger.info("CONSUMER: KafkaConsumer created successfully");
            
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            logger.info("CONSUMER: Subscribed to topic: {}", TOPIC_NAME);
            
            int messagesConsumed = 0;
            long lastMessageTime = System.currentTimeMillis();
            
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    
                    if (!records.isEmpty()) {
                        lastMessageTime = System.currentTimeMillis();
                        
                        for (ConsumerRecord<String, String> record : records) {
                            messagesConsumed++;
                            logger.info("CONSUMER: Message {}: Key={}, Value preview={}", 
                                       messagesConsumed, 
                                       record.key(),
                                       record.value().length() > 50 
                                           ? record.value().substring(0, 50) + "..." 
                                           : record.value());
                        }
                        
                        consumer.commitSync();
                    } else {
                        // Stop if no messages for 10 seconds and we've consumed some messages
                        if (messagesConsumed > 0 && 
                            System.currentTimeMillis() - lastMessageTime > 10000) {
                            logger.info("CONSUMER: No messages for 10 seconds, stopping...");
                            break;
                        }
                    }
                    
                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) {
                        logger.info("CONSUMER: Consumer interrupted, stopping...");
                        break;
                    }
                    logger.error("CONSUMER: Error processing messages", e);
                }
            }
            
            logger.info("CONSUMER: Finished consuming {} messages", messagesConsumed);
            
        } catch (Exception e) {
            logger.error("CONSUMER: Error", e);
        }
    }

    /**
     * Create producer properties.
     */
    private static Properties createProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        
        logger.info("PRODUCER: Configuration created");
        return props;
    }

    /**
     * Create consumer properties.
     */
    private static Properties createConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        logger.info("CONSUMER: Configuration created");
        return props;
    }

    /**
     * Generate a sample message with some content.
     */
    private static String generateSampleMessage() {
        return String.format("Sample data with timestamp %d and random value %d", 
                           System.currentTimeMillis(), 
                           (int)(Math.random() * 1000));
    }
} 