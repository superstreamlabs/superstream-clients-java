package ai.superstream.core;

import ai.superstream.model.MetadataMessage;
import ai.superstream.util.SuperstreamLogger;
import ai.superstream.util.KafkaPropertiesUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import ai.superstream.agent.KafkaProducerInterceptor;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Consumes messages from the superstream.metadata_v1 topic.
 */
public class MetadataConsumer {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(MetadataConsumer.class);
    private static final String METADATA_TOPIC = "superstream.metadata_v1";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Get the metadata message from the Kafka cluster.
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @return The metadata message, or null if there was an error
     */
    public MetadataMessage getMetadataMessage(String bootstrapServers, Properties originalClientProperties) {
        Properties properties = new Properties();

        // Copy essential client configuration properties from the original client
        KafkaPropertiesUtils.copyClientConfigurationProperties(originalClientProperties, properties);

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, KafkaProducerInterceptor.SUPERSTREAM_LIBRARY_PREFIX + "metadata-consumer");

        // Log the configuration before creating consumer
        if (SuperstreamLogger.isDebugEnabled()) {
            StringBuilder configLog = new StringBuilder("Creating internal MetadataConsumer with configuration: ");
            properties.forEach((key, value) -> {
                // Mask sensitive values
                if (key.toString().toLowerCase().contains("password") || 
                    key.toString().toLowerCase().contains("sasl.jaas.config")) {
                    configLog.append(key).append("=[MASKED], ");
                } else {
                    configLog.append(key).append("=").append(value).append(", ");
                }
            });
            // Remove trailing comma and space
            if (configLog.length() > 2) {
                configLog.setLength(configLog.length() - 2);
            }
            logger.debug(configLog.toString());
        }

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            // Check if the metadata topic exists
            Set<String> topics = consumer.listTopics().keySet();
            if (!topics.contains(METADATA_TOPIC)) {
                logger.error("[ERR-034] Superstream internal topic is missing. This topic is required for Superstream to function properly. Please make sure the Kafka user has read/write/describe permissions on superstream.* topics.");
                return null;
            }

            // Assign the metadata topic
            TopicPartition partition = new TopicPartition(METADATA_TOPIC, 0);
            consumer.assign(Collections.singletonList(partition));

            // Seek to the end and get the current offset
            consumer.seekToEnd(Collections.singletonList(partition));
            long endOffset = consumer.position(partition);

            if (endOffset == 0) {
                logger.error("[ERR-035] Unable to retrieve optimizations data from Superstream. This is required for optimization. Please contact the Superstream team if the issue persists.");
                return null;
            }

            // Seek to the last message
            consumer.seek(partition, endOffset - 1);

            // Poll for the message
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                logger.error("[ERR-036] Unable to retrieve optimizations data from Superstream. This is required for optimization. Please contact the Superstream team if the issue persists.");
                return null;
            }
            logger.debug("Successfully retrieved a message from the {} topic", METADATA_TOPIC);

            // Parse the message
            String json = records.iterator().next().value();
            return objectMapper.readValue(json, MetadataMessage.class);
        } catch (IOException e) {
            logger.error("[ERR-027] Unable to retrieve optimizations data from Superstream. This is required for optimization. Please contact the Superstream team if the issue persists.", e);
            return null;
        } catch (Exception e) {
            logger.error("[ERR-028] Unable to retrieve optimizations data from Superstream. This is required for optimization. Please make sure the Kafka user has read/write/describe permissions on superstream.* topics.", e);
            return null;
        }
    }
}