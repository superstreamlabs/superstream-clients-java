package ai.superstream.agent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import ai.superstream.core.SuperstreamManager;
import ai.superstream.model.MetadataMessage;
import ai.superstream.model.TopicConfiguration;
import ai.superstream.util.SuperstreamLogger;

/**
 * Computes optimized swap configurations for shadow producer hot-swapping.
 * Builds topic-specific or default optimized configurations based on observed producer behavior.
 */
public final class SwapConfigManager {

    private SwapConfigManager() { }

    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(SwapConfigManager.class);

    /**
     * Result of building an optimized swap configuration.
     * Contains the final Properties to apply and its fingerprint for tracking.
     */
    public static final class SwapResult {
        public final Properties appliedProperties;
        public final String fingerprint;

        public SwapResult(Properties appliedProperties, String fingerprint) {
            this.appliedProperties = appliedProperties;
            this.fingerprint = fingerprint;
        }
    }

    /**
     * Builds swap configuration by selecting the most impactful observed topic from metadata,
     * extracting its optimized configuration, and overlaying it onto the base properties.
     * 
     * @param baseProps Base properties to overlay optimizations onto
     * @param metadata Metadata containing topic-specific optimizations
     * @param observedTopics Topics that have been observed via send() calls
     * @param latencySensitive If true, removes linger.ms from optimizations
     * @return SwapResult with optimized properties, or null if no configuration available
     */
    public static SwapResult buildSwapOptimizedConfig(Properties baseProps,
                                                      MetadataMessage metadata,
                                                      List<String> observedTopics,
                                                      boolean latencySensitive) {
        if (metadata == null || metadata.getTopicsConfiguration() == null || metadata.getTopicsConfiguration().isEmpty()) {
            return null;
        }
        if (observedTopics == null || observedTopics.isEmpty()) {
            return null;
        }

        Set<String> metadataTopics = new HashSet<>();
        for (TopicConfiguration tc : metadata.getTopicsConfiguration()) {
            if (tc != null && tc.getTopicName() != null) metadataTopics.add(tc.getTopicName());
        }
        List<String> candidates = new ArrayList<>();
        for (String t : observedTopics) {
            if (metadataTopics.contains(t)) candidates.add(t);
        }
        if (candidates.isEmpty()) {
            return null;
        }

        String impactfulTopic = SuperstreamManager.getInstance()
                .getConfigurationOptimizer()
                .getMostImpactfulTopicName(metadata, candidates);
        if (impactfulTopic == null || impactfulTopic.isEmpty()) {
            return null;
        }
        logger.info("[SWAP-CONFIG] Selected most impactful topic: {}", impactfulTopic);

        Map<String, Object> optimizedMap = null;
        for (TopicConfiguration tc : metadata.getTopicsConfiguration()) {
            if (tc != null && impactfulTopic.equals(tc.getTopicName())) {
                optimizedMap = tc.getOptimizedConfiguration();
                break;
            }
        }
        if (optimizedMap == null || optimizedMap.isEmpty()) {
            return null;
        }
        logger.info("[SWAP-CONFIG] Optimized configuration for topic '{}': {}", impactfulTopic, optimizedMap);
        
        // Create a copy of optimizedMap to avoid mutating the original (which may be shared/immutable)
        Map<String, Object> optimizedMapCopy = new HashMap<>(optimizedMap);
        if (latencySensitive) {
            optimizedMapCopy.remove("linger.ms");
        }

        Properties appliedProps = cloneProperties(baseProps);
        for (Map.Entry<String,Object> e : optimizedMapCopy.entrySet()) {
            try { appliedProps.put(e.getKey(), e.getValue()); } catch (Throwable ignored) { }
        }
        String fp = fingerprint(appliedProps);
        return new SwapResult(appliedProps, fp);
    }
    
    /**
     * Builds a default optimization swap configuration when no topic-specific configuration is available.
     * Applies global default optimizations on top of the base properties.
     * 
     * @param baseProps Base properties to overlay optimizations onto
     * @param latencySensitive If true, removes linger.ms from optimizations
     * @return SwapResult with optimized properties
     */
    public static SwapResult buildDefaultSwapOptimizedConfig(Properties baseProps, boolean latencySensitive) {
        Properties applied = cloneProperties(baseProps);
        Map<String, Object> defaultOptimal = SuperstreamManager.getDefaultOptimizationValues();
        
        // Create a copy to avoid mutating the original map (which may be shared/immutable)
        Map<String, Object> defaultOptimalCopy = new HashMap<>(defaultOptimal);
        if (latencySensitive && defaultOptimalCopy.containsKey("linger.ms")) {
            defaultOptimalCopy.remove("linger.ms");
        }
        
        for (Map.Entry<String, Object> entry : defaultOptimalCopy.entrySet()) {
            try {
                applied.put(entry.getKey(), entry.getValue());
            } catch (Throwable ignored) { }
        }
        String fp = fingerprint(applied);
        return new SwapResult(applied, fp);
    }

    private static Properties cloneProperties(Properties props) {
        Properties copy = new Properties();
        if (props != null) copy.putAll(props);
        return copy;
    }

    private static String fingerprint(Properties props) {
        if (props == null) return "{}";
        TreeMap<String, String> sorted = new TreeMap<>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            if (entry.getKey() == null) continue;
            sorted.put(String.valueOf(entry.getKey()), entry.getValue() == null ? "null" : String.valueOf(entry.getValue()));
        }
        return sorted.toString();
    }
}
