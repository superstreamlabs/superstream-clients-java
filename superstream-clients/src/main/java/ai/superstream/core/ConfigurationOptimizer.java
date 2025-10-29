package ai.superstream.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import ai.superstream.model.MetadataMessage;
import ai.superstream.model.TopicConfiguration;
import ai.superstream.util.SuperstreamLogger;

/**
 * Optimizes Kafka producer configurations based on metadata.
 */
public class ConfigurationOptimizer {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(ConfigurationOptimizer.class);
    private static final String LATENCY_SENSITIVE_ENV_VAR = "SUPERSTREAM_LATENCY_SENSITIVE";

    // List of configuration parameters that should be preserved if larger than
    // recommended
    private static final Set<String> PRESERVE_IF_LARGER = new HashSet<>(Arrays.asList(
            "batch.size",
            "linger.ms"));

    /**
     * Get the optimal configuration for a set of topics.
     *
     * @param metadataMessage   The metadata message
     * @param applicationTopics The list of topics that the application might
     *                          produce to
     * @return The optimal configuration, or an empty map if no optimization is
     *         possible
     */
    /**
     * Select optimal parameters for the given topics while honoring latency sensitivity.
     * If SUPERSTREAM_LATENCY_SENSITIVE is set, we avoid setting linger.ms in the result.
     */
    public Map<String, Object> getOptimalConfiguration(MetadataMessage metadataMessage,
            List<String> applicationTopics) {
        boolean isLatencySensitive = isLatencySensitive();
        if (isLatencySensitive) {
            logger.debug().msg("Application is marked as latency-sensitive, linger.ms will not be modified");
        }

        // Get all matching topic configurations
        List<TopicConfiguration> topics = Optional.ofNullable(metadataMessage.getTopicsConfiguration())
                .orElse(Collections.emptyList());
        List<TopicConfiguration> matchingConfigurations = topics.stream()
                .filter(config -> applicationTopics.contains(config.getTopicName()))
                .collect(Collectors.toList());

        Map<String, Object> optimalConfiguration;

        if (matchingConfigurations.isEmpty()) {
            if (applicationTopics.isEmpty()) {
                logger.debug().msg(
                        "No application topics provided. Applying default optimizations.");
            } else {
                logger.debug().msg(
                        "No matching topic configurations found for the application topics. Applying default optimizations.");
                logger.warn().msg(
                        "The topics you're publishing to haven't been analyzed yet. For optimal results, either wait for the next analysis cycle or trigger one manually via the SuperClient Console");
            }

            // Apply default optimizations when no matching topics found
            optimalConfiguration = new HashMap<>();
            optimalConfiguration.put("compression.type", "zstd");
            optimalConfiguration.put("batch.size", 32768); // 32KB

            // Only add linger if not latency-sensitive
            if (!isLatencySensitive) {
                optimalConfiguration.put("linger.ms", 5000); // 5 seconds default
                logger.debug().msg(
                        "Default optimizations will be applied: compression.type=zstd, batch.size=32768, linger.ms=5000");
            } else {
                logger.debug().msg(
                        "Default optimizations will be applied: compression.type=zstd, batch.size=32768 (linger.ms unchanged)");
            }
            return optimalConfiguration;
        }

        // Find the most impactful topic
        TopicConfiguration mostImpactfulTopic = findMostImpactfulTopic(matchingConfigurations);

        optimalConfiguration = new HashMap<>(mostImpactfulTopic.getOptimizedConfiguration());

        // If latency sensitive, remove linger.ms setting
        if (isLatencySensitive && optimalConfiguration.containsKey("linger.ms")) {
            optimalConfiguration.remove("linger.ms");
            logger.info("Ignore linger.ms from optimizations due to latency-sensitive configuration");
        }

        return optimalConfiguration;
    }

    /**
     * Apply the optimal configuration to the producer properties.
     *
     * @param properties           The producer properties to modify
     * @param optimalConfiguration The optimal configuration to apply
     * @return The list of configuration keys that were modified
     */
    /**
     * Apply optimal parameters on top of the existing configuration with safeguards:
     * - Preserve-if-larger for batch.size/linger.ms so user choices are kept when higher.
     * - Respect SUPERSTREAM_LATENCY_SENSITIVE by skipping linger.ms.
     * - Validate values before applying.
     */
    public List<String> applyOptimalConfiguration(Properties properties, Map<String, Object> optimalConfiguration, String topic) {
        if (optimalConfiguration == null || optimalConfiguration.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> modifiedKeys = new ArrayList<>();
        boolean isLatencySensitive = isLatencySensitive();
        String clientIdForLog = null;
        try {
            Object cid = (properties != null) ? properties.get("client.id") : null;
            clientIdForLog = (cid != null) ? String.valueOf(cid) : "";
        } catch (Throwable ignored) { clientIdForLog = ""; }

        for (Map.Entry<String, Object> entry : optimalConfiguration.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value == null) {
                logger.warn("Skipping null value for configuration key: {}", key);
                continue;
            }

            // Skip linger.ms optimization if the application is latency-sensitive
            if ("linger.ms".equals(key) && isLatencySensitive) {
            logger.info().msg("Skipping linger.ms optimization due to latency-sensitive configuration");
                continue;
            }

            // Special handling for configurations that should be preserved if larger
            if (PRESERVE_IF_LARGER.contains(key)) {
                // Get the recommended value as a number
                int recommendedValue;
                try {
                    recommendedValue = value instanceof Number ? ((Number) value).intValue()
                            : Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    logger.warn().msg("Invalid recommended value for {}: {}. Skipping this parameter.", key, value);
                    continue;
                }

                // Check if there's an existing setting
                Object existingValue = properties.get(key);
                if (existingValue != null) {
                    int existingNumericValue;
                    try {
                        existingNumericValue = existingValue instanceof Number ? ((Number) existingValue).intValue()
                                : Integer.parseInt(existingValue.toString());
                    } catch (NumberFormatException e) {
                        logger.warn().msg("Invalid existing {} value: {}. Will use recommended value.", key, existingValue);
                        existingNumericValue = 0;
                    }

                    // Keep the existing value if it's larger than the recommended value
                    if (existingNumericValue > recommendedValue) {
                        logger.debug().msg("Keeping existing {} value {} as it's greater than recommended value {}",
                                key, existingNumericValue, recommendedValue);
                        continue; // Skip this key, keeping the existing value
                    }
                }
            }

            // Validate the configuration before applying
            if (!isValidConfiguration(key, value)) {
                logger.warn().msg("Invalid configuration value for {}: {}. Skipping this parameter.", key, value);
                continue;
            }

            // Store the original value for logging
            Object originalValue = properties.get(key);

            // Apply the optimization
            properties.put(key, value);
            modifiedKeys.add(key);

            if (originalValue == null) {
                logger.info().withClientId(clientIdForLog).forTopic(topic).msg("Setting configuration: {}={} (was not previously set)", key, value);
            } else {
                logger.info().withClientId(clientIdForLog).forTopic(topic).msg("Overriding configuration: {}={} (was: {})", key, value, originalValue);
            }
            
        }

        return modifiedKeys;
    }

    private boolean isValidConfiguration(String key, Object value) {
        if (value == null) {
            logger.warn("Invalid null value for configuration key: {}", key);
            return false;
        }
        try {
            if ("compression.type".equals(key)) {
                String compressionType = value.toString();
                // Valid compression types in Kafka
                return Arrays.asList("none", "gzip", "snappy", "lz4", "zstd").contains(compressionType);
            }
            // Add validation for other key types as needed
            return true;
        } catch (Exception e) {
            logger.warn("Error validating configuration {}: {}", key, value, e);
            return false;
        }
    }

    /**
     * Determine if the application is latency-sensitive based on environment
     * variable.
     *
     * @return true if the application is latency-sensitive, false otherwise
     */
    public boolean isLatencySensitive() {
        String latencySensitiveStr = System.getenv(LATENCY_SENSITIVE_ENV_VAR);
        if (latencySensitiveStr != null && !latencySensitiveStr.trim().isEmpty()) {
            return Boolean.parseBoolean(latencySensitiveStr.trim());
        }
        return false; // Default to not latency-sensitive
    }

    /**
     * Helper to find the most impactful topic from a list of matching
     * configurations.
     */
    private TopicConfiguration findMostImpactfulTopic(List<TopicConfiguration> matchingConfigurations) {
        return matchingConfigurations.stream()
                .max(Comparator.comparing(TopicConfiguration::calculateImpactScore))
                .orElse(null);
    }

    /**
     * Get the most impactful topic name for a set of topics.
     *
     * @param metadataMessage   The metadata message
     * @param applicationTopics The list of topics that the application might
     *                          produce to
     * @return The name of the most impactful topic, or null if none found
     */
    public String getMostImpactfulTopicName(MetadataMessage metadataMessage, List<String> applicationTopics) {
        List<TopicConfiguration> topics = Optional.ofNullable(metadataMessage.getTopicsConfiguration())
                .orElse(Collections.emptyList());
        List<TopicConfiguration> matchingConfigurations = topics.stream()
                .filter(config -> applicationTopics.contains(config.getTopicName()))
                .collect(Collectors.toList());
        if (matchingConfigurations.isEmpty()) {
            return null;
        }
        TopicConfiguration mostImpactfulTopic = findMostImpactfulTopic(matchingConfigurations);
        return mostImpactfulTopic != null ? mostImpactfulTopic.getTopicName() : null;
    }
}