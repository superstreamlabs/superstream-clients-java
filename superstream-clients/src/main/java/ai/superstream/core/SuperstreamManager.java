package ai.superstream.core;

import ai.superstream.model.MetadataMessage;
import ai.superstream.util.SuperstreamLogger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main manager class for the Superstream library.
 */
public class SuperstreamManager {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(SuperstreamManager.class);
    private static final String TOPICS_ENV_VAR = "SUPERSTREAM_TOPICS_LIST";
    private static final String DISABLED_ENV_VAR = "SUPERSTREAM_DISABLED";
    private static final ThreadLocal<Boolean> OPTIMIZATION_IN_PROGRESS = new ThreadLocal<>();
    private static volatile SuperstreamManager instance;

    private final MetadataConsumer metadataConsumer;
    private final ConfigurationOptimizer configurationOptimizer;
    private final Map<String, MetadataMessage> metadataCache;
    private final boolean disabled;

    private SuperstreamManager() {
        this.metadataConsumer = new MetadataConsumer();
        this.configurationOptimizer = new ConfigurationOptimizer();
        this.metadataCache = new ConcurrentHashMap<>();
        this.disabled = Boolean.parseBoolean(System.getenv(DISABLED_ENV_VAR));

        if (disabled) {
            logger.debug("Superstream optimization is disabled via environment variable");
        }
    }

    /**
     * Get the configuration optimizer instance.
     *
     * @return The configuration optimizer instance
     */
    public ConfigurationOptimizer getConfigurationOptimizer() {
        return configurationOptimizer;
    }

    /**
     * Check if optimization is already in progress for the current thread.
     *
     * @return true if optimization is in progress, false otherwise
     */
    public static boolean isOptimizationInProgress() {
        return Boolean.TRUE.equals(OPTIMIZATION_IN_PROGRESS.get());
    }

    /**
     * Set the optimization in progress flag for the current thread.
     *
     * @param inProgress true if optimization is in progress, false otherwise
     */
    public static void setOptimizationInProgress(boolean inProgress) {
        if (inProgress) {
            OPTIMIZATION_IN_PROGRESS.set(Boolean.TRUE);
        } else {
            OPTIMIZATION_IN_PROGRESS.remove();
        }
    }

    /**
     * Get the singleton instance of the SuperstreamManager.
     *
     * @return The SuperstreamManager instance
     */
    public static SuperstreamManager getInstance() {
        if (instance == null) {
            synchronized (SuperstreamManager.class) {
                if (instance == null) {
                    instance = new SuperstreamManager();
                }
            }
        }
        return instance;
    }

    public static Map<String, Object> convertPropertiesToMap(Properties properties) {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            map.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return map;
    }

    /**
     * Optimize the producer properties for a given Kafka cluster.
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @param clientId The client ID
     * @param properties The producer properties to optimize
     * @return True if the optimization was successful, false otherwise
     */
    public boolean optimizeProducer(String bootstrapServers, String clientId, Properties properties) {
        if (disabled) {
            return false;
        }

        // Skip if already optimizing (prevents infinite recursion)
        if (isOptimizationInProgress()) {
            logger.debug("Skipping optimization for producer {} as optimization is already in progress", clientId);
            return false;
        }

        try {
            // Mark optimization as in progress for this thread
            setOptimizationInProgress(true);

            // Get or fetch the metadata message
            java.util.AbstractMap.SimpleEntry<MetadataMessage, String> result = getOrFetchMetadataMessage(bootstrapServers, properties);
            MetadataMessage metadataMessage = result.getKey();
            String error = result.getValue();

            if (metadataMessage == null) {
                // Error is already logged in getOrFetchMetadataMessage
                // Push ConfigInfo with error and original config for stats reporting
                java.util.Deque<ai.superstream.agent.KafkaProducerInterceptor.ConfigInfo> cfgStack = ai.superstream.agent.KafkaProducerInterceptor.TL_CFG_STACK.get();
                cfgStack.push(new ai.superstream.agent.KafkaProducerInterceptor.ConfigInfo(convertPropertiesToMap(properties), new java.util.HashMap<>(), error));
                return false;
            }

            // Create a copy of the original configuration for reporting
            Properties originalProperties = new Properties();
            originalProperties.putAll(properties);

            // Check if optimization is active
            if (!metadataMessage.isActive()) {
                String errMsg = "[ERR-054] Superstream optimization is not active for this kafka cluster, please head to the Superstream console and activate it.";
                logger.error(errMsg);
                
                // Push ConfigInfo with error and original config for stats reporting
                java.util.Deque<ai.superstream.agent.KafkaProducerInterceptor.ConfigInfo> cfgStack = ai.superstream.agent.KafkaProducerInterceptor.TL_CFG_STACK.get();
                cfgStack.push(new ai.superstream.agent.KafkaProducerInterceptor.ConfigInfo(convertPropertiesToMap(properties), new java.util.HashMap<>(), errMsg));
                return false;
            }

            // Get the application topics
            List<String> applicationTopics = getApplicationTopics();

            // Get the optimal configuration
            Map<String, Object> optimalConfiguration = configurationOptimizer.getOptimalConfiguration(
                    metadataMessage, applicationTopics);

            // Capture the full original configuration map BEFORE applying optimizations
            Map<String,Object> originalFullMap = convertPropertiesToMap(properties);

            // Apply the optimal configuration
            List<String> modifiedKeys = configurationOptimizer.applyOptimalConfiguration(properties, optimalConfiguration);

            if (modifiedKeys.isEmpty()) {
                logger.debug("No configuration parameters were modified");
                return false;
            }

            // Build optimized configuration map to report: include every key that was considered for optimisation.
            Map<String, Object> optimizedProperties = new HashMap<>();
            for (String key : optimalConfiguration.keySet()) {
                // After applyOptimalConfiguration, 'properties' holds the final value (either overridden or original).
                Object finalVal = properties.get(key);
                if (finalVal == null) {
                    // If not present in current props, fall back to original value (may be null as well)
                    finalVal = originalProperties.get(key);
                }
                if (finalVal != null) {
                    // Convert numeric strings to actual numbers for reporting
                    if (finalVal instanceof String) {
                        String strVal = ((String) finalVal).trim();
                        try {
                            if (!strVal.isEmpty()) {
                                // Prefer Integer when within range, otherwise Long
                                long longVal = Long.parseLong(strVal);
                                if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
                                    finalVal = (int) longVal;
                                } else {
                                    finalVal = longVal;
                                }
                            }
                        } catch (NumberFormatException ignored) {
                            // leave as String if not purely numeric
                        }
                    }
                    optimizedProperties.put(key, finalVal);
                }
            }

            // If the application is latency-sensitive we leave linger.ms untouched. Ensure we still report its value
            // so that the clients topic contains the complete set actually in effect.
            final String LINGER_MS_KEY = "linger.ms";
            if (!optimizedProperties.containsKey(LINGER_MS_KEY)) {
                Object lingerVal = properties.get(LINGER_MS_KEY);
                if (lingerVal == null) {
                    lingerVal = originalProperties.get(LINGER_MS_KEY);
                }
                if (lingerVal != null) {
                    optimizedProperties.put(LINGER_MS_KEY, lingerVal);
                }
            }

            // Pass configuration info via ThreadLocal to interceptor's onExit (full map for original config)
            ai.superstream.agent.KafkaProducerInterceptor.TL_CFG_STACK.get()
                    .push(new ai.superstream.agent.KafkaProducerInterceptor.ConfigInfo(originalFullMap, optimizedProperties));

            // Log optimization success with appropriate message based on configuration and client ID
            boolean isLatencySensitive = configurationOptimizer.isLatencySensitive();
            boolean isUsingDefaults = applicationTopics != null && !applicationTopics.isEmpty() && 
                (metadataMessage.getTopicsConfiguration() == null || 
                metadataMessage.getTopicsConfiguration().stream().noneMatch(tc -> applicationTopics.contains(tc.getTopicName())));
            
            String baseMessage = isUsingDefaults ? 
                "Successfully optimized producer with default optimization parameters" :
                "Successfully optimized producer configuration";
                
            if (clientId != null && !clientId.trim().isEmpty()) {
                baseMessage += " for " + clientId;
            }
            
            if (isLatencySensitive) {
                baseMessage += " (linger.ms left unchanged due to latency sensitivity)";
            }
            
            logger.info(baseMessage);
            return true;
        } catch (Exception e) {
            logger.error("[ERR-030] Failed to optimize producer configuration: {}", e.getMessage(), e);
            return false;
        } finally {
            // Always clear the flag when done
            setOptimizationInProgress(false);
        }
    }

    /**
     * Get the metadata message for a given Kafka cluster.
     *
     * @param bootstrapServers The Kafka bootstrap servers
     * @return A pair containing the metadata message (or null if error) and the error message (or null if no error)
     */
    public java.util.AbstractMap.SimpleEntry<MetadataMessage, String> getOrFetchMetadataMessage(String bootstrapServers, Properties originalProperties) {
        // Normalise the bootstrap servers so that different orderings of the same
        // Kafka consumers and wasted network calls when the application creates
        // multiple producers with logically-identical bootstrap lists such as
        // "b1:9092,b2:9092" and "b2:9092,b1:9092".

        String cacheKey = normalizeBootstrapServers(bootstrapServers);

        // Check the cache first
        if (metadataCache.containsKey(cacheKey)) {
            return new java.util.AbstractMap.SimpleEntry<>(metadataCache.get(cacheKey), null);
        }

        // Fetch the metadata using the *original* string (ordering is irrelevant
        // for the Kafka client itself)
        java.util.AbstractMap.SimpleEntry<MetadataMessage, String> result = metadataConsumer.getMetadataMessage(bootstrapServers, originalProperties);
        MetadataMessage metadataMessage = result.getKey();

        if (metadataMessage != null) {
            metadataCache.put(cacheKey, metadataMessage);
        }

        return result;
    }

    /**
     * Produce a canonical representation of the bootstrap servers list.
     * <p>
     * The input may contain duplicates, whitespace or different ordering – we
     * split on commas, trim each entry, drop empties, sort the list
     * lexicographically and join it back with commas.  The resulting string can
     * safely be used as a map key that uniquely identifies a Kafka cluster.
     */
    private static String normalizeBootstrapServers(String servers) {
        if (servers == null) {
            return "";
        }

        String[] parts = servers.split(",");
        java.util.List<String> cleaned = new java.util.ArrayList<>();
        for (String p : parts) {
            if (p == null) continue;
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                cleaned.add(trimmed);
            }
        }
        java.util.Collections.sort(cleaned);
        return String.join(",", cleaned);
    }

    /**
     * Get the list of application topics from the environment variable.
     *
     * @return The list of application topics
     */
    private List<String> getApplicationTopics() {
        String topicsString = System.getenv(TOPICS_ENV_VAR);
        if (topicsString == null || topicsString.trim().isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(topicsString.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(java.util.stream.Collectors.toList());
    }
}