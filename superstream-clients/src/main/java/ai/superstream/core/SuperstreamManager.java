package ai.superstream.core;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import ai.superstream.model.MetadataMessage;
import ai.superstream.util.SuperstreamLogger;

/**
 * Main manager class for the Superstream library.
 * Manages metadata fetching, caching, and producer configuration optimization.
 */
public class SuperstreamManager {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(SuperstreamManager.class);
    private static final String DISABLED_ENV_VAR = "SUPERSTREAM_DISABLED";
    private static final String LATENCY_SENSITIVE_ENV_VAR = "SUPERSTREAM_LATENCY_SENSITIVE";
    private static final String GLOBAL_DEFAULTS_LABEL = "global-defaults";
    private static final long DEFAULT_METADATA_TTL_MS = 600_000L; // 10 minutes
    private static final ThreadLocal<Boolean> OPTIMIZATION_IN_PROGRESS = new ThreadLocal<>();
    private static volatile SuperstreamManager instance;

    private final MetadataConsumer metadataConsumer;
    private final ConfigurationOptimizer configurationOptimizer;
    private final Map<String, CacheEntry> metadataCache;
    private final boolean disabled;

    private SuperstreamManager() {
        this.metadataConsumer = new MetadataConsumer();
        this.configurationOptimizer = new ConfigurationOptimizer();
        this.metadataCache = new ConcurrentHashMap<>();
        this.disabled = Boolean.parseBoolean(System.getenv(DISABLED_ENV_VAR));

        if (disabled) {
            logger.debug().msg("Superstream optimization is disabled via environment variable");
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

    private long getMetadataTtlMs() {
        return DEFAULT_METADATA_TTL_MS;
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

    /**
     * Converts Properties to a {@code Map<String, Object>}.
     * 
     * @param properties The properties to convert, may be null
     * @return A new map containing the properties, or empty map if properties is null
     */
    public static Map<String, Object> convertPropertiesToMap(Properties properties) {
        if (properties == null) {
            return new HashMap<>();
        }
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            map.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return map;
    }

    /**
     * Get the default Superstream optimization configuration values.
     * This is the centralized location for default optimization parameters.
     *
     * @return A map containing the default optimization values:
     *         - compression.type: "zstd"
     *         - batch.size: 65536
     *         - linger.ms: 5000 (only if not latency-sensitive)
     */
    public static Map<String, Object> getDefaultOptimizationValues() {
        Map<String, Object> optimal = new HashMap<>();
        optimal.put("compression.type", "zstd");
        optimal.put("batch.size", 65536);
        
        // Respect latency sensitivity: do not modify linger when env flag is set
        boolean latencySensitive = isLatencySensitive();
        
        if (!latencySensitive) {
            optimal.put("linger.ms", 5000);
        }
        
        return optimal;
    }

    /**
     * Optimize the producer properties for a given Kafka cluster.
     * Applies global default optimizations (compression.type, batch.size, linger.ms).
     * Used in INIT_CONFIG mode when shadow routing is not available.
     *
     * @param bootstrapServers The Kafka bootstrap servers, must not be null
     * @param clientId The client ID, may be null
     * @param properties The producer properties to optimize, must not be null
     * @return True if the optimization was successful, false otherwise
     */
    public boolean optimizeProducer(String bootstrapServers, String clientId, Properties properties) {
        if (disabled) {
            return false;
        }

        if (bootstrapServers == null || properties == null) {
            logger.warn().msg("[OPTIMIZATION] Invalid parameters: bootstrapServers or properties is null");
            return false;
        }

        // Skip if already optimizing (prevents infinite recursion)
        if (isOptimizationInProgress()) {
            logger.debug().msg("Skipping optimization for producer {} as optimization is already in progress", clientId);
            return false;
        }

        try {
            setOptimizationInProgress(true);

            AbstractMap.SimpleEntry<MetadataMessage, String> result = getOrFetchMetadataMessage(bootstrapServers, properties);
            MetadataMessage metadataMessage = result.getKey();
            String error = result.getValue();

            if (metadataMessage == null) {
                pushConfigInfoWithError(properties, error);
                return false;
            }

            Properties originalProperties = new Properties();
            originalProperties.putAll(properties);

            if (!metadataMessage.isActive()) {
                String errMsg = "[ERR-054] Superstream optimization is not active for this kafka cluster, please head to the Superstream console and activate it.";
                logger.warn().msg(errMsg);
                pushConfigInfoWithError(properties, errMsg);
                return false;
            }

            Map<String, Object> optimal = getDefaultOptimizationValues();
            configurationOptimizer.applyOptimalConfiguration(properties, optimal, GLOBAL_DEFAULTS_LABEL);

            pushConfigInfo(originalProperties, properties);
            
            boolean latencySensitive = !optimal.containsKey("linger.ms");
            logger.info().withClientId(clientId).msg("[GLOBAL-OPTIMIZATION] Applied default Superstream optimizations (compression.type=zstd, batch.size=65536{} )",
                    latencySensitive ? ", linger.ms=unchanged (latency-sensitive)" : ", linger.ms=5000");
            return true;
        } catch (RuntimeException e) {
            logger.error().msg("[ERR-030] Failed to optimize producer configuration: {}", e.getMessage(), e);
            return false;
        } finally {
            setOptimizationInProgress(false);
        }
    }

    /**
     * Push ConfigInfo to ThreadLocal stack with error message.
     */
    private void pushConfigInfoWithError(Properties properties, String error) {
        Deque<ai.superstream.agent.KafkaProducerInterceptor.ConfigInfo> cfgStack = 
                ai.superstream.agent.KafkaProducerInterceptor.TL_CFG_STACK.get();
        cfgStack.push(new ai.superstream.agent.KafkaProducerInterceptor.ConfigInfo(
                convertPropertiesToMap(properties), new HashMap<>(), error));
    }

    /**
     * Push ConfigInfo to ThreadLocal stack with original and optimized configurations.
     */
    private void pushConfigInfo(Properties originalProperties, Properties optimizedProperties) {
        Map<String, Object> originalFullMap = convertPropertiesToMap(originalProperties);
        Map<String, Object> optimizedFullMap = convertPropertiesToMap(optimizedProperties);
        ai.superstream.agent.KafkaProducerInterceptor.TL_CFG_STACK.get()
                .push(new ai.superstream.agent.KafkaProducerInterceptor.ConfigInfo(originalFullMap, optimizedFullMap));
    }

    /**
     * Check if the application is latency-sensitive based on environment variable.
     * 
     * @return true if SUPERSTREAM_LATENCY_SENSITIVE is set to "true" or "1"
     */
    private static boolean isLatencySensitive() {
        try {
            String v = System.getenv(LATENCY_SENSITIVE_ENV_VAR);
            return v != null && ("true".equalsIgnoreCase(v) || "1".equals(v));
        } catch (Throwable ignored) {
            return false;
        }
    }

    /**
     * Get the metadata message for a given Kafka cluster.
     * Uses caching with TTL to avoid unnecessary network calls.
     * Cache key is normalized bootstrap servers to handle different orderings.
     *
     * @param bootstrapServers The Kafka bootstrap servers, must not be null
     * @param originalProperties The original producer properties, used for authentication
     * @return A pair containing the metadata message (or null if error) and the error message (or null if no error)
     */
    public AbstractMap.SimpleEntry<MetadataMessage, String> getOrFetchMetadataMessage(String bootstrapServers, Properties originalProperties) {
        // Normalise the bootstrap servers so that different orderings of the same
        // Kafka consumers and wasted network calls when the application creates
        // multiple producers with logically-identical bootstrap lists such as
        // "b1:9092,b2:9092" and "b2:9092,b1:9092".

        if (bootstrapServers == null) {
            return new AbstractMap.SimpleEntry<>(null, "bootstrapServers cannot be null");
        }
        String cacheKey = normalizeBootstrapServers(bootstrapServers);

        // Check the cache first with TTL
        CacheEntry cached = metadataCache.get(cacheKey);
        long now = System.currentTimeMillis();
        long ttl = getMetadataTtlMs();
        if (cached != null && (now - cached.storedAtMs) <= ttl) {
            return new AbstractMap.SimpleEntry<>(cached.value, null);
        }

        // Fetch the metadata using the original string (ordering is irrelevant for the Kafka client)
        AbstractMap.SimpleEntry<MetadataMessage, String> result = metadataConsumer.getMetadataMessage(bootstrapServers, originalProperties);
        MetadataMessage metadataMessage = result.getKey();

        if (metadataMessage != null) {
            metadataCache.put(cacheKey, new CacheEntry(metadataMessage, now));
        }

        return result;
    }

    /**
     * Cache entry for metadata messages with timestamp for TTL checking.
     */
    private static final class CacheEntry {
        final MetadataMessage value;
        final long storedAtMs;
        
        CacheEntry(MetadataMessage value, long storedAtMs) {
            this.value = value;
            this.storedAtMs = storedAtMs;
        }
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
        List<String> cleaned = new ArrayList<>();
        for (String p : parts) {
            if (p == null) continue;
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                cleaned.add(trimmed);
            }
        }
        Collections.sort(cleaned);
        return String.join(",", cleaned);
    }

    // Removed env-based application topics logic; per-producer topics are discovered from metrics.
}