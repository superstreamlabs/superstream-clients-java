package ai.superstream.agent;

import ai.superstream.core.ClientStatsReporter;
import ai.superstream.core.SuperstreamManager;
import ai.superstream.model.MetadataMessage;
import ai.superstream.util.SuperstreamLogger;
import ai.superstream.util.ClientUtils;
import net.bytebuddy.asm.Advice;
import java.util.AbstractMap;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.lang.ThreadLocal;
import java.util.List;
import java.util.Arrays;

/**
 * Intercepts KafkaProducer constructor calls to optimize configurations and
 * collect metrics.
 */
public class KafkaProducerInterceptor {
    public static final SuperstreamLogger logger = SuperstreamLogger.getLogger(KafkaProducerInterceptor.class);

    // Constant for Superstream library prefix
    public static final String SUPERSTREAM_LIBRARY_PREFIX = "superstreamlib-";

    // Environment variable to check if Superstream is disabled
    private static final String DISABLED_ENV_VAR = "SUPERSTREAM_DISABLED";
    public static final boolean DISABLED = Boolean.parseBoolean(System.getenv(DISABLED_ENV_VAR));

    // Map to store client stats reporters for each producer
    public static final ConcurrentHashMap<String, ClientStatsReporter> clientStatsReporters = new ConcurrentHashMap<>();

    // Map to store producer metrics info by producer ID
    public static final ConcurrentHashMap<String, ProducerMetricsInfo> producerMetricsMap = new ConcurrentHashMap<>();

    // Single shared metrics collector for all producers
    public static final SharedMetricsCollector sharedCollector = new SharedMetricsCollector();

    // ThreadLocal stack to track nested KafkaProducer constructor calls per thread
    // This ensures that when the Superstream optimization logic creates its own producer
    // inside the application's producer construction, we still match the correct
    // properties object on exit (LIFO order).
    public static final ThreadLocal<java.util.Deque<Properties>> TL_PROPS_STACK =
            ThreadLocal.withInitial(java.util.ArrayDeque::new);

    // ThreadLocal stack to hold the producer UUIDs generated in onEnter so that the same
    // value can be reused later in onExit (for stats reporting) and by SuperstreamManager
    // when it reports the client information. The stack is aligned with the TL_PROPS_STACK
    // (push in onEnter, pop in onExit).
    public static final ThreadLocal<java.util.Deque<String>> TL_UUID_STACK =
            ThreadLocal.withInitial(java.util.ArrayDeque::new);

    // ThreadLocal stack to pass original/optimized configuration maps from optimization phase to reporter creation.
    public static final ThreadLocal<java.util.Deque<ConfigInfo>> TL_CFG_STACK =
            ThreadLocal.withInitial(java.util.ArrayDeque::new);

    // Static initializer to start the shared collector if enabled
    static {
        if (!DISABLED) {
            try {
                sharedCollector.start();
            } catch (Exception e) {
                logger.error("[ERR-001] Failed to start metrics collector: {}", e.getMessage(), e);
            }
        } else {
            logger.warn("Superstream is disabled via SUPERSTREAM_DISABLED environment variable");
        }
    }

    /**
     * Check if Superstream is disabled via environment variable.
     * 
     * @return true if Superstream is disabled, false otherwise
     */
    public static boolean isDisabled() {
        return DISABLED;
    }

    /**
     * Called before the KafkaProducer constructor.
     * Used to optimize producer configurations.
     *
     * @param args The producer properties
     */
    @Advice.OnMethodEnter
    public static void onEnter(@Advice.AllArguments Object[] args) {
        // Skip if Superstream is disabled via environment variable
        if (isDisabled()) {
            return;
        }

        // Check if this is a direct call from application code or an internal
        // delegation
        if (!isInitialProducerCreation()) {
            return;
        }

        // Extract Properties or Map from the arguments and push onto the stack
        Properties properties = extractProperties(args);
        if (properties != null) {
            // Replace the original Map argument with our Properties instance so that
            // the KafkaProducer constructor reads the optimised values.
            for (int i = 0; i < args.length; i++) {
                Object a = args[i];
                if (a instanceof java.util.Map && !(a instanceof java.util.Properties)) {
                    args[i] = properties;
                    break;
                }
            }

            TL_PROPS_STACK.get().push(properties);

            // Generate a UUID for this upcoming producer instance and push onto UUID stack
            String producerUuid = java.util.UUID.randomUUID().toString();
            TL_UUID_STACK.get().push(producerUuid);
        }  else {
          logger.error("[ERR-002] Could not extract properties from producer arguments");
          // here we can not report the error to the clients topic as we were not able to extract the properties
          return;
        }

        // Detect immutable Map argument (e.g., Collections.unmodifiableMap) so we can skip optimisation early
        boolean immutableConfigDetected = false;
        java.util.Map<String,Object> immutableOriginalMap = null;
        for (Object arg : args) {
            if (arg instanceof java.util.Map) {
                @SuppressWarnings("unchecked")
                java.util.Map<String,Object> testMap = (java.util.Map<String,Object>) arg;
                
                // Skip empty maps as they might be immutable but don't matter for optimization
                if (!testMap.isEmpty()) {
                    try {
                        String testKey = "__superstream_immutable_test_" + System.nanoTime();
                        testMap.put(testKey, "test");
                        testMap.remove(testKey);
                    } catch (UnsupportedOperationException | IllegalStateException e) {
                        // Map is immutable
                        immutableConfigDetected = true;
                        immutableOriginalMap = testMap;
                        break;
                    }
                }
            }
        }

        try {
            // Skip if we're already in the process of optimizing
            if (SuperstreamManager.isOptimizationInProgress()) {
                return;
            }

            if (properties == null || properties.isEmpty()) {
                logger.error("[ERR-003] Could not extract properties from properties");
                return;
            }

            // Skip producers created by the Superstream library
            String clientId = properties.getProperty("client.id", "");
            if (clientId.startsWith(SUPERSTREAM_LIBRARY_PREFIX)) {
                logger.debug("Skipping optimization for Superstream internal producer: {}", clientId);
                return;
            }

            // Extract bootstrap servers
            String bootstrapServers = properties.getProperty("bootstrap.servers");
            if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
                logger.error("[ERR-004] bootstrap.servers is not set, cannot optimize");
                return;
            }

            if (immutableConfigDetected && immutableOriginalMap != null) {
                String errMsg = String.format("[ERR-010] Cannot optimize KafkaProducer configuration: received an unmodifiable Map (%s). Please pass a mutable java.util.Properties or java.util.Map instead.",
                        immutableOriginalMap.getClass().getName());
                logger.error(errMsg);

                // Push ConfigInfo with error and original config for stats reporting
                java.util.Deque<ConfigInfo> cfgStack = TL_CFG_STACK.get();
                cfgStack.push(new ConfigInfo(propertiesToMap(properties), new java.util.HashMap<>(), errMsg));

                // Do NOT attempt optimisation
                return;
            }

            // Store original properties to restore in case of failure
            java.util.Map<String, Object> originalPropertiesMap = propertiesToMap(properties);
            Properties originalProperties = new Properties();
            originalProperties.putAll(properties);

            try {
                // Optimize the producer
                boolean optimized = SuperstreamManager.getInstance().optimizeProducer(bootstrapServers, clientId, properties);
                if (!optimized) {
                    // Restore original properties if optimization was not successful
                    properties.putAll(originalProperties);
                    // Push ConfigInfo with original config for stats reporting
                    java.util.Deque<ConfigInfo> cfgStack = TL_CFG_STACK.get();
                    // Check if there's an existing ConfigInfo with an error
                    ConfigInfo existingConfig = cfgStack.isEmpty() ? null : cfgStack.peek();
                    String error = existingConfig != null ? existingConfig.error : null;
                    cfgStack.push(new ConfigInfo(originalPropertiesMap, new java.util.HashMap<>(), error));
                }
            } catch (Exception e) {
                logger.error("[ERR-053] Error during producer optimization: {}", e.getMessage(), e);
                // Restore original properties in case of failure
                properties.putAll(originalProperties);
                // Push ConfigInfo with original config for stats reporting
                java.util.Deque<ConfigInfo> cfgStack = TL_CFG_STACK.get();
                cfgStack.push(new ConfigInfo(originalPropertiesMap, new java.util.HashMap<>()));
            }
        } catch (Exception e) {
            logger.error("[ERR-053] Error during producer optimization: {}", e.getMessage(), e);
        }
    }

    /**
     * Called after the KafkaProducer constructor.
     * Used to register the producer for metrics collection.
     * 
     * @param producer The KafkaProducer instance that was just created
     */
    @Advice.OnMethodExit
    public static void onExit(@Advice.This Object producer) {
        // Skip if Superstream is disabled via environment variable
        if (isDisabled()) {
            return;
        }

        try {
            // Process only for the outer-most constructor call
            if (!isInitialProducerCreation()) {
                return;
            }

            java.util.Deque<Properties> stack = TL_PROPS_STACK.get();
            if (stack.isEmpty()) {
                logger.error("[ERR-006] No captured properties for this producer constructor; skipping stats reporter setup");
                return;
            }

            Properties producerProps = stack.pop();

            // Retrieve matching UUID for this constructor instance
            java.util.Deque<String> uuidStack = TL_UUID_STACK.get();
            String producerUuid = "";
            if (!uuidStack.isEmpty()) {
                producerUuid = uuidStack.pop();
            } else {
                logger.error("[ERR-127] No producer UUID found for this constructor instance");
            }

            // Clean up ThreadLocal when outer-most constructor finishes
            if (stack.isEmpty()) {
                TL_PROPS_STACK.remove();
                TL_UUID_STACK.remove();
            }

            String bootstrapServers = producerProps.getProperty("bootstrap.servers");
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                logger.error("[ERR-007] bootstrap.servers missing in captured properties; skipping reporter setup");
                return;
            }

            String rawClientId = producerProps.getProperty("client.id"); // may be null or empty

            // Skip internal library producers (identified by client.id prefix)
            if (rawClientId != null && rawClientId.startsWith(SUPERSTREAM_LIBRARY_PREFIX)) {
                return;
            }

            // Use the JVM identity hash to create a unique key per producer instance
            String producerId = "producer-" + System.identityHashCode(producer);

            // The client ID to be reported is the raw value (may be null or empty, that's OK)
            String clientIdForStats = rawClientId != null ? rawClientId : "";

            // Only register if we don't already have this producer instance
            if (!producerMetricsMap.containsKey(producerId)) {
                logger.debug("Registering producer with metrics collector: {} (client.id='{}')", producerId, clientIdForStats);

                // Create a reporter for this producer instance – pass the original client.id
                ClientStatsReporter reporter = new ClientStatsReporter(bootstrapServers, producerProps, clientIdForStats, producerUuid);

                // Set the most impactful topic if possible
                try {
                    MetadataMessage metadataMessage = null;
                    List<String> topics = null;
                    // Try to get metadata and topics if available
                    if (producerProps != null) {
                        String bootstrapServersProp = producerProps.getProperty("bootstrap.servers");
                        if (bootstrapServersProp != null) {
                            AbstractMap.SimpleEntry<MetadataMessage, String> metadataResult = SuperstreamManager.getInstance().getOrFetchMetadataMessage(bootstrapServersProp, producerProps);
                            metadataMessage = metadataResult.getKey();
                        }
                        String topicsEnv = System.getenv("SUPERSTREAM_TOPICS_LIST");
                        if (topicsEnv != null && !topicsEnv.trim().isEmpty()) {
                            topics = Arrays.asList(topicsEnv.split(","));
                        }
                    }
                    if (metadataMessage != null && topics != null) {
                        String mostImpactfulTopic = SuperstreamManager.getInstance().getConfigurationOptimizer().getMostImpactfulTopicName(metadataMessage, topics);
                        if (mostImpactfulTopic == null) {
                            mostImpactfulTopic = "";
                        }
                        reporter.updateMostImpactfulTopic(mostImpactfulTopic);
                    }
                } catch (Exception e) {
                    logger.debug("Failed to get most impactful topic: {}", e.getMessage());
                }

                // Create metrics info for this producer
                ProducerMetricsInfo metricsInfo = new ProducerMetricsInfo(producer, reporter);

                // Register with the shared collector
                producerMetricsMap.put(producerId, metricsInfo);
                clientStatsReporters.put(producerId, reporter);

                // Pop configuration info from ThreadLocal stack (if any) and attach to reporter
                java.util.Deque<ConfigInfo> cfgStack = TL_CFG_STACK.get();
                ConfigInfo cfgInfo = cfgStack.isEmpty()? null : cfgStack.pop();
                if (cfgStack.isEmpty()) {
                    TL_CFG_STACK.remove();
                }
                if (cfgInfo != null) {
                    // Use the original configuration from ConfigInfo and get complete config with defaults
                    java.util.Map<String, Object> completeConfig = ClientUtils.getCompleteProducerConfig(cfgInfo.originalConfig);
                    java.util.Map<String, Object> optimizedConfig = cfgInfo.optimizedConfig != null ? cfgInfo.optimizedConfig : new java.util.HashMap<>();
                    reporter.setConfigurations(completeConfig, optimizedConfig);
                    // If optimizedConfig is empty and there is an error, set the error on the reporter
                    if (optimizedConfig.isEmpty() && cfgInfo.error != null && !cfgInfo.error.isEmpty()) {
                        reporter.updateError(cfgInfo.error);
                    }
                } else {
                    // No ConfigInfo available, so no optimization was performed
                    // Use the producer properties as both original and optimized (since no changes were made)
                    java.util.Map<String, Object> originalPropsMap = propertiesToMap(producerProps);
                    java.util.Map<String, Object> completeConfig = ClientUtils.getCompleteProducerConfig(originalPropsMap);
                    reporter.setConfigurations(completeConfig, new java.util.HashMap<>());
                }

                // Trigger immediate metrics collection for this producer
                try {
                    sharedCollector.collectMetricsForProducer(producerId, metricsInfo);
                } catch (Exception e) {
                    logger.error("[ERR-047] Failed to collect immediate metrics for new producer {}: {}", producerId, e.getMessage(), e);
                }

                logger.debug("Producer {} registered with shared metrics collector", producerId);
            }
        } catch (Exception e) {
            logger.error("[ERR-008] Error registering producer with metrics collector: {}", e.getMessage(), e);
        }
    }

    /**
     * Extract Properties object from constructor arguments.
     */
    public static Properties extractProperties(Object[] args) {
        // Look for Properties or Map in the arguments
        if (args == null) {
            logger.error("[ERR-009] extractProperties: args array is null");
            return null;
        }

        logger.debug("extractProperties: Processing {} arguments", args.length);
        for (Object arg : args) {
            if (arg == null) {
                logger.debug("extractProperties: Found null argument");
                continue;
            }

            String className = arg.getClass().getName();
            logger.debug("extractProperties: Processing argument of type: {}", className);

            if (arg instanceof Properties) {
                logger.debug("extractProperties: Found Properties object");
                Properties props = (Properties) arg;
                normalizeBootstrapServers(props);
                return props;
            }

            if (arg instanceof Map) {
                logger.debug("extractProperties: Found Map object of type: {}", arg.getClass().getName());

                // If the map is unmodifiable we cannot actually modify it later; we still let the caller decide
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) arg;
                    Properties props = new MapBackedProperties(map);
                    logger.debug("extractProperties: Successfully converted Map to Properties");
                    return props;
                } catch (ClassCastException e) {
                    // Not the map type we expected
                    logger.error("[ERR-011] extractProperties: Could not cast Map to Map<String, Object>: {}", e.getMessage(), e);
                    return null;
                }
            }

            // Handle ProducerConfig object which contains properties
            if (className.endsWith("ProducerConfig")) {
                logger.debug("extractProperties: Found ProducerConfig object");
                try {
                    // Try multiple possible field names
                    String[] fieldNames = { "originals", "values", "props", "properties", "configs" };

                    for (String fieldName : fieldNames) {
                        try {
                            Field field = arg.getClass().getDeclaredField(fieldName);
                            field.setAccessible(true);
                            Object fieldValue = field.get(arg);
                            logger.debug("extractProperties: Found field {} with value type: {}", fieldName,
                                    fieldValue != null ? fieldValue.getClass().getName() : "null");

                            if (fieldValue instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> map = (Map<String, Object>) fieldValue;

                                Properties props = new MapBackedProperties(map);
                                logger.debug(
                                        "extractProperties: Successfully converted ProducerConfig field {} to Properties",
                                        fieldName);
                                return props;
                            } else if (fieldValue instanceof Properties) {
                                logger.debug("extractProperties: Found Properties in ProducerConfig field {}",
                                        fieldName);
                                Properties props = (Properties) fieldValue;
                                normalizeBootstrapServers(props);
                                return props;
                            }
                        } catch (NoSuchFieldException e) {
                            // Field doesn't exist, try the next one
                            logger.error("[ERR-017] extractProperties: Field {} not found in ProducerConfig", fieldName);
                            continue;
                        }
                    }

                    // Try to call getters if field access failed
                    logger.debug("extractProperties: Trying getter methods for ProducerConfig");
                    for (Method method : arg.getClass().getMethods()) {
                        if ((method.getName().equals("originals") ||
                                method.getName().equals("values") ||
                                method.getName().equals("configs") ||
                                method.getName().equals("properties") ||
                                method.getName().equals("getOriginals") ||
                                method.getName().equals("getValues") ||
                                method.getName().equals("getConfigs") ||
                                method.getName().equals("getProperties")) &&
                                method.getParameterCount() == 0) {

                            Object result = method.invoke(arg);
                            logger.debug("extractProperties: Called method {} with result type: {}", method.getName(),
                                    result != null ? result.getClass().getName() : "null");
                            if (result instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> map = (Map<String, Object>) result;

                                Properties props = new MapBackedProperties(map);
                                logger.debug(
                                        "extractProperties: Successfully converted ProducerConfig method {} result to Properties",
                                        method.getName());
                                return props;
                            } else if (result instanceof Properties) {
                                logger.debug("extractProperties: Found Properties in ProducerConfig method {} result",
                                        method.getName());
                                Properties props = (Properties) result;
                                normalizeBootstrapServers(props);
                                return props;
                            }
                        }
                    }

                    // Last resort: Try to get the ProducerConfig's bootstrap.servers value
                    // and create a minimal Properties object
                    logger.debug("extractProperties: Trying last resort method to get bootstrap.servers");
                    for (Method method : arg.getClass().getMethods()) {
                        if (method.getName().equals("getString") && method.getParameterCount() == 1) {
                            try {
                                String bootstrapServers = (String) method.invoke(arg, "bootstrap.servers");
                                String clientId = (String) method.invoke(arg, "client.id");

                                if (bootstrapServers != null) {
                                    Properties minProps = new Properties();
                                    minProps.put("bootstrap.servers", bootstrapServers);
                                    if (clientId != null) {
                                        minProps.put("client.id", clientId);
                                    }
                                    logger.debug(
                                            "extractProperties: Created minimal Properties with bootstrap.servers and client.id");
                                    return minProps;
                                }
                            } catch (Exception e) {
                                logger.debug("extractProperties: Failed to get bootstrap.servers from ProducerConfig",
                                        e);
                            }
                        }
                    }

                } catch (Exception e) {
                    logger.error("[ERR-018] extractProperties: Failed to extract properties from ProducerConfig: {}",
                            e.getMessage(), e);
                            return null;
                }
            }
        }

        logger.error("[ERR-019] extractProperties: No valid configuration object found in arguments");
        return null;
    }

    /**
     * Ensure that the bootstrap.servers property is stored as a comma-separated String even when
     * the user supplied it as a Collection (or array) inside a java.util.Properties instance.
     * This keeps the rest of the optimisation pipeline – which relies on getProperty(String) – working.
     */
    private static void normalizeBootstrapServers(Properties props) {
        if (props == null) {
            return;
        }

        Object bsObj = props.get("bootstrap.servers");
        if (bsObj == null) {
            return;
        }

        String joined = null;
        if (bsObj instanceof java.util.Collection) {
            java.util.Collection<?> col = (java.util.Collection<?>) bsObj;
            StringBuilder sb = new StringBuilder();
            for (Object o : col) {
                if (o == null) continue;
                if (sb.length() > 0) sb.append(',');
                sb.append(o.toString());
            }
            joined = sb.toString();
        } else if (bsObj.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(bsObj);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < len; i++) {
                Object o = java.lang.reflect.Array.get(bsObj, i);
                if (o == null) continue;
                if (sb.length() > 0) sb.append(',');
                sb.append(o.toString());
            }
            joined = sb.toString();
        }

        if (joined != null && !joined.isEmpty()) {
            props.put("bootstrap.servers", joined);
        }
    }

    /**
     * Determines if this constructor call is the initial creation from application
     * code
     * rather than an internal delegation between constructors.
     */
    public static boolean isInitialProducerCreation() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

        // Start from index 1 to skip getStackTrace() itself
        boolean foundKafkaProducer = false;
        int kafkaProducerCount = 0;

        for (int i = 1; i < stackTrace.length; i++) {
            String className = stackTrace[i].getClassName();

            // Only treat the *actual* KafkaProducer class (or its anonymous / inner classes) as a match.
            // This avoids counting user subclasses such as TemplateKafkaProducer which also end with the
            // same suffix and would otherwise be mistaken for an internal constructor delegation.
            String simpleName;
            int lastDotIdx = className.lastIndexOf('.');
            simpleName = (lastDotIdx >= 0) ? className.substring(lastDotIdx + 1) : className;

            boolean isKafkaProducerClass = simpleName.equals("KafkaProducer") || simpleName.startsWith("KafkaProducer$");

            if (isKafkaProducerClass) {
                foundKafkaProducer = true;
                kafkaProducerCount++;

                // If we find more than one KafkaProducer in the stack, it's a delegation
                if (kafkaProducerCount > 1) {
                    return false;
                }
            }
            // Once we've seen KafkaProducer and then see a different class,
            // we've found the actual caller
            else if (foundKafkaProducer) {
                // Skip certain framework classes that might wrap the call
                if (className.startsWith("java.") ||
                        className.startsWith("javax.") ||
                        className.startsWith("sun.") ||
                        className.startsWith("com.sun.")) {
                    continue;
                }

                // We've found the application class that called KafkaProducer
                logger.debug("Detected initial producer creation from: " + className);
                return true;
            }
        }

        // If we make it here with exactly one KafkaProducer in the stack, it's likely
        // the initial creation (first constructor being called)
        return kafkaProducerCount == 1;
    }

    /**
     * Helper utility to extract a field value using reflection.
     */
    public static Object extractFieldValue(Object obj, String... fieldNames) {
        if (obj == null)
            return null;

        for (String fieldName : fieldNames) {
            try {
                Field field = findField(obj.getClass(), fieldName);
                if (field != null) {
                    field.setAccessible(true);
                    Object value = field.get(obj);
                    if (value != null) {
                        return value;
                    }
                }
            } catch (Exception e) {
                // Ignore and try next field
            }
        }

        return null;
    }

    /**
     * Find a field in a class or its superclasses.
     */
    public static Field findField(Class<?> clazz, String fieldName) {
        if (clazz == null || clazz == Object.class)
            return null;

        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            return findField(clazz.getSuperclass(), fieldName);
        }
    }

    /**
     * Find a method in a class or its superclasses.
     */
    public static Method findMethod(Class<?> clazz, String methodName) {
        if (clazz == null || clazz == Object.class)
            return null;

        try {
            return clazz.getDeclaredMethod(methodName);
        } catch (NoSuchMethodException e) {
            return findMethod(clazz.getSuperclass(), methodName);
        }
    }

    /**
     * Find a method in a class or its superclasses, trying multiple method names.
     */
    public static Method findMethod(Class<?> clazz, String... methodNames) {
        for (String methodName : methodNames) {
            Method method = findMethod(clazz, methodName);
            if (method != null) {
                return method;
            }
        }
        return null;
    }

    /**
     * Holds metrics information for a single producer.
     */
    public static class ProducerMetricsInfo {
        private final Object producer;
        private final ClientStatsReporter reporter;
        private final AtomicReference<CompressionStats> lastStats = new AtomicReference<>(new CompressionStats(0, 0));
        private final AtomicBoolean isActive = new AtomicBoolean(true);

        public ProducerMetricsInfo(Object producer, ClientStatsReporter reporter) {
            this.producer = producer;
            this.reporter = reporter;
        }

        public Object getProducer() {
            return producer;
        }

        public ClientStatsReporter getReporter() {
            return reporter;
        }

        public CompressionStats getLastStats() {
            return lastStats.get();
        }

        public void updateLastStats(CompressionStats stats) {
            lastStats.set(stats);
        }

        public boolean isActive() {
            return isActive.get();
        }
    }

    /**
     * A singleton class that collects Kafka metrics periodically for all registered
     * producers using a single shared thread. This is more efficient than having
     * one thread
     * per producer.
     */
    public static class SharedMetricsCollector {
        private final ScheduledExecutorService scheduler;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private static final long COLLECTION_INTERVAL_MS = 30000; // 30 seconds

        public SharedMetricsCollector() {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "superstream-kafka-metrics-collector");
                t.setDaemon(true);
                return t;
            });
        }

        /**
         * Start collecting metrics periodically for all registered producers.
         */
        public void start() {
            if (running.compareAndSet(false, true)) {
                logger.debug("Starting shared Kafka metrics collector with interval {} ms", COLLECTION_INTERVAL_MS);
                try {
                    scheduler.scheduleAtFixedRate(this::collectAllMetrics,
                            COLLECTION_INTERVAL_MS / 2, // Start sooner for first collection
                            COLLECTION_INTERVAL_MS,
                            TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    logger.error("[ERR-012] Failed to schedule metrics collection: {}", e.getMessage(), e);
                    running.set(false);
                }
            } else {
                logger.debug("Metrics collector already running");
            }
        }

        /**
         * Collect metrics from all registered producers.
         */
        public void collectAllMetrics() {
            try {
                // Skip if disabled
                if (isDisabled()) {
                    return;
                }

                int totalProducers = producerMetricsMap.size();
                if (totalProducers == 0) {
                    logger.debug("No producers registered for metrics collection");
                    return;
                }

                logger.debug("Starting metrics collection cycle for {} producers", totalProducers);
                int successCount = 0;
                int skippedCount = 0;

                // Iterate through all registered producers
                for (Map.Entry<String, ProducerMetricsInfo> entry : producerMetricsMap.entrySet()) {
                    String producerId = entry.getKey();
                    ProducerMetricsInfo info = entry.getValue();

                    // Skip inactive producers
                    if (!info.isActive()) {
                        skippedCount++;
                        continue;
                    }

                    try {
                        boolean success = collectMetricsForProducer(producerId, info);
                        if (success) {
                            successCount++;
                        } else {
                            skippedCount++;
                        }
                    } catch (Exception e) {
                        logger.error("[ERR-013] Error collecting metrics for producer {}: {}", producerId, e.getMessage(), e);
                    }
                }

                logger.debug(
                        "Completed metrics collection cycle: {} producers processed, {} reported stats, {} skipped",
                        totalProducers, successCount, skippedCount);

            } catch (Exception e) {
                logger.error("[ERR-014] Error in metrics collection cycle: {}", e.getMessage(), e);
            }
        }

        /**
         * Collect metrics for a single producer.
         * 
         * @return true if metrics were successfully collected and reported, false if
         *         skipped
         */
        public boolean collectMetricsForProducer(String producerId, ProducerMetricsInfo info) {
            try {
                Object producer = info.getProducer();
                ClientStatsReporter reporter = info.getReporter();

                // Get the metrics object from the producer
                Object metrics = extractFieldValue(producer, "metrics");
                if (metrics == null) {
                    logger.debug("No metrics object found in producer {}", producerId);
                    return false;
                }

                // Extract the metrics map once per invocation and reuse in subsequent calculations
                java.util.Map<?,?> metricsMap = extractMetricsMap(metrics);

                // Try to get the compression ratio metric; fall back to 1.0 (no compression)
                double compressionRatio = getCompressionRatio(metricsMap);
                if (compressionRatio <= 0) {
                    logger.debug("No compression ratio metric found; assuming ratio 1.0 for producer {}", producerId);
                    compressionRatio = 1.0;
                }

                // Get the outgoing-byte-total metrics - this is a per-node metric that
                // represents compressed bytes.  We need to sum it across all nodes and it is
                // cumulative over time.
                long totalOutgoingBytes = getOutgoingBytesTotal(metricsMap);

                // Calculate the delta since the last collection cycle (can be zero when idle)
                CompressionStats prevStats = info.getLastStats();
                long compressedBytes = Math.max(0, totalOutgoingBytes - prevStats.compressedBytes);

                // Use the compression ratio to calculate the uncompressed size
                // compression_ratio = compressed_size / uncompressed_size
                // Therefore: uncompressed_size = compressed_size / compression_ratio
                long uncompressedBytes = (compressionRatio > 0 && compressedBytes > 0)
                        ? (long) (compressedBytes / compressionRatio)
                        : 0;

                // Update the last stats with the new total (cumulative) bytes
                // For uncompressed, add the new delta to the previous total
                info.updateLastStats(
                        new CompressionStats(totalOutgoingBytes, prevStats.uncompressedBytes + uncompressedBytes));

                // Create snapshots for different metric types
                java.util.Map<String, Double> allMetricsSnapshot = new java.util.HashMap<>();
                java.util.Map<String, java.util.Map<String, Double>> topicMetricsSnapshot = new java.util.HashMap<>();
                java.util.Map<String, java.util.Map<String, Double>> nodeMetricsSnapshot = new java.util.HashMap<>();
                java.util.Map<String, String> appInfoMetricsSnapshot = new java.util.HashMap<>();

                try {
                    java.util.Map<?, ?> rawMetricsMap = metricsMap;
                    if (rawMetricsMap != null) {
                        for (java.util.Map.Entry<?, ?> mEntry : rawMetricsMap.entrySet()) {
                            Object mKey = mEntry.getKey();
                            String group = null;
                            String namePart;
                            String keyString = null;
                            String topicName = null;
                            String nodeId = null;
                            if (mKey == null) continue;

                            if (mKey.getClass().getName().endsWith("MetricName")) {
                                try {
                                    java.lang.reflect.Method nameMethod = findMethod(mKey.getClass(), "name");
                                    java.lang.reflect.Method groupMethod = findMethod(mKey.getClass(), "group");
                                    java.lang.reflect.Method tagsMethod = findMethod(mKey.getClass(), "tags");
                                    namePart = (nameMethod != null) ? nameMethod.invoke(mKey).toString() : mKey.toString();
                                    group = (groupMethod != null) ? groupMethod.invoke(mKey).toString() : "";
                                    if ("producer-metrics".equals(group)) {
                                        keyString = namePart;
                                    } else if ("producer-topic-metrics".equals(group)) {
                                        if (tagsMethod != null) {
                                            tagsMethod.setAccessible(true);
                                            Object tagObj = tagsMethod.invoke(mKey);
                                            if (tagObj instanceof java.util.Map) {
                                                Object topicObj = ((java.util.Map<?,?>)tagObj).get("topic");
                                                if (topicObj != null) {
                                                    topicName = topicObj.toString();
                                                    keyString = namePart;
                                                }
                                            }
                                        }
                                    } else if ("producer-node-metrics".equals(group)) {
                                        if (tagsMethod != null) {
                                            tagsMethod.setAccessible(true);
                                            Object tagObj = tagsMethod.invoke(mKey);
                                            if (tagObj instanceof java.util.Map) {
                                                Object nodeIdObj = ((java.util.Map<?,?>)tagObj).get("node-id");
                                                if (nodeIdObj != null) {
                                                    String rawNodeId = nodeIdObj.toString();
                                                    String[] parts = rawNodeId.split("-");
                                                    if (parts.length > 1) {
                                                        try {
                                                            int id = Integer.parseInt(parts[1]);
                                                            if (id >= 0) {
                                                                nodeId = String.valueOf(id);
                                                                keyString = namePart;
                                                            }
                                                        } catch (NumberFormatException e) {
                                                            // ignore making us ignore the negative node IDs (represents metrics from the controller that we don't care about)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else if ("app-info".equals(group)) {
                                        keyString = namePart;
                                    } else {
                                        continue; // skip non-producer groups
                                    }
                                } catch (Exception e) {
                                    logger.debug("Failed to process metric name: {}", e.getMessage());
                                }
                            } else if (mKey instanceof String) {
                                keyString = mKey.toString();

                                // producer-metrics group (per producer)
                                if (keyString.startsWith("producer-metrics.")) {
                                    keyString = keyString.substring("producer-metrics.".length());

                                // producer-topic-metrics group (per topic)
                                } else if (keyString.startsWith("producer-topic-metrics.")) {
                                    String[] parts = keyString.split("\\.", 3);
                                    if (parts.length == 3) {
                                        topicName = parts[1];
                                        keyString = parts[2];
                                    }

                                // producer-node-metrics group (per broker node)
                                } else if (keyString.startsWith("producer-node-metrics.")) {
                                    String[] parts = keyString.split("\\.", 3);
                                    if (parts.length == 3) {
                                        String rawNodeId = parts[1];
                                        String[] idParts = rawNodeId.split("-");
                                        if (idParts.length > 1) {
                                            try {
                                                int id = Integer.parseInt(idParts[1]);
                                                if (id >= 0) {
                                                    nodeId = String.valueOf(id);
                                                    keyString = parts[2];
                                                }
                                            } catch (NumberFormatException e) {
                                                // ignore making us ignore the negative node IDs (represents metrics from the controller that we don't care about)
                                            }
                                        }
                                    }

                                // app-info group (string values)
                                } else if (keyString.startsWith("app-info.")) {
                                    keyString = keyString.substring("app-info.".length());

                                // skip metrics groups that are not producer-metrics, producer-topic-metrics, producer-node-metrics, or app-info
                                } else if (!keyString.startsWith("producer-metrics") &&
                                           !keyString.startsWith("producer-topic-metrics") &&
                                           !keyString.startsWith("producer-node-metrics") &&
                                           !keyString.startsWith("app-info")) {
                                    continue;
                                }
                            }
                            if (keyString == null) continue;

                            // Handle app-info metrics differently - store as strings
                            if ("app-info".equals(group) || 
                                (mKey instanceof String && ((String)mKey).startsWith("app-info"))) {
                                Object value = mEntry.getValue();
                                String stringValue = null;
                                if (value != null) {
                                    // Try to extract the value from KafkaMetric if possible
                                    try {
                                        java.lang.reflect.Method metricValueMethod = value.getClass().getMethod("metricValue");
                                        Object actualValue = metricValueMethod.invoke(value);
                                        stringValue = (actualValue != null) ? actualValue.toString() : null;
                                    } catch (Exception e) {
                                        stringValue = value.toString();
                                    }
                                }
                                if (stringValue != null) {
                                    appInfoMetricsSnapshot.put(keyString, stringValue);
                                }
                                continue;
                            }

                            // Handle numeric metrics
                            double mVal = extractMetricValue(mEntry.getValue());
                            if (!Double.isNaN(mVal)) {
                                if (topicName != null) {
                                    topicMetricsSnapshot.computeIfAbsent(topicName, k -> new java.util.HashMap<>())
                                            .put(keyString, mVal);
                                } else if (nodeId != null) {
                                    nodeMetricsSnapshot.computeIfAbsent(nodeId, k -> new java.util.HashMap<>())
                                            .put(keyString, mVal);
                                } else {
                                    allMetricsSnapshot.put(keyString, mVal);
                                }
                            }
                        }
                    }
                } catch (Exception snapshotEx) {
                    logger.error("[ERR-015] Error extracting metrics snapshot for producer {}: {}", producerId, snapshotEx.getMessage(), snapshotEx);
                }

                // Update reporter with latest metrics snapshots
                reporter.updateProducerMetrics(allMetricsSnapshot);
                reporter.updateTopicMetrics(topicMetricsSnapshot);
                reporter.updateNodeMetrics(nodeMetricsSnapshot);
                reporter.updateAppInfoMetrics(appInfoMetricsSnapshot);

                // Aggregate topics written by this producer from producer-topic-metrics
                java.util.Set<String> newTopics = new java.util.HashSet<>();
                try {
                    java.util.Map<?,?> rawMapForTopics = metricsMap;
                    if (rawMapForTopics != null) {
                        for (java.util.Map.Entry<?,?> me : rawMapForTopics.entrySet()) {
                            Object k = me.getKey();
                            if (k == null) continue;
                            if (k.getClass().getName().endsWith("MetricName")) {
                                try {
                                    java.lang.reflect.Method groupMethod = findMethod(k.getClass(), "group");
                                    java.lang.reflect.Method tagsMethod = findMethod(k.getClass(), "tags");
                                    if (groupMethod != null && tagsMethod != null) {
                                        groupMethod.setAccessible(true);
                                        String g = groupMethod.invoke(k).toString();
                                        if ("producer-topic-metrics".equals(g)) {
                                            tagsMethod.setAccessible(true);
                                            Object tagObj = tagsMethod.invoke(k);
                                            if (tagObj instanceof java.util.Map) {
                                                Object topicObj = ((java.util.Map<?,?>)tagObj).get("topic");
                                                if (topicObj != null) newTopics.add(topicObj.toString());
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    logger.debug("Failed to extract topic from metric tags: {}", e.getMessage());
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.debug("Failed to aggregate topics from metrics: {}", e.getMessage());
                }

                if (!newTopics.isEmpty()) {
                    reporter.addTopics(newTopics);
                }

                // Report the compression statistics for this interval (delta)
                reporter.recordBatch(uncompressedBytes, compressedBytes);

                logger.debug("Producer {} compression collected: before={} bytes, after={} bytes, ratio={}",
                        producerId, uncompressedBytes, compressedBytes, String.format("%.4f", compressionRatio));

                return true;
            } catch (Exception e) {
                logger.error("[ERR-016] Error collecting Kafka metrics for producer {}: {}", producerId, e.getMessage(), e);
                return false;
            }
        }

        /**
         * Get the compression ratio from the metrics object.
         */
        public double getCompressionRatio(java.util.Map<?,?> metricsMap) {
            try {
                if (metricsMap != null) {
                    logger.debug("Metrics map size: {}", metricsMap.size());

                    double compressionRatio = findDirectCompressionMetric(metricsMap);
                    if (compressionRatio > 0) {
                        return compressionRatio;
                    }
                }
            } catch (Exception e) {
                logger.debug("Error getting compression ratio: " + e.getMessage(), e);
            }
            return 0;
        }

        /**
         * Find direct compression metrics in the metrics map.
         */
        private double findDirectCompressionMetric(java.util.Map<?, ?> metricsMap) {
            // Look for compression metrics in the *producer-metrics* group only
            for (java.util.Map.Entry<?, ?> entry : metricsMap.entrySet()) {
                Object key = entry.getKey();

                // Handle MetricName keys
                if (key.getClass().getName().endsWith("MetricName")) {
                    try {
                        Method nameMethod = findMethod(key.getClass(), "name");
                        Method groupMethod = findMethod(key.getClass(), "group");

                        if (nameMethod != null && groupMethod != null) {
                            nameMethod.setAccessible(true);
                            groupMethod.setAccessible(true);

                            Object nameObj = nameMethod.invoke(key);
                            Object groupObj = groupMethod.invoke(key);
                            
                            if (nameObj == null || groupObj == null) {
                                continue;
                            }
                            
                            String name = nameObj.toString();
                            String group = groupObj.toString();

                            // Only accept metrics from producer-metrics group
                            if ("producer-metrics".equals(group) &&
                                    ("compression-rate-avg".equals(name) || "compression-ratio".equals(name))) {

                                double value = extractMetricValue(entry.getValue());
                                if (value >= 0) {
                                    logger.debug("Found producer-metrics compression metric: {} -> {}", name, value);
                                    return value;
                                }
                            }
                        }
                    } catch (Exception ignored) {
                    }
                }
                // Handle String keys
                else if (key instanceof String) {
                    String keyStr = (String) key;
                    if (keyStr.startsWith("producer-metrics") &&
                            (keyStr.contains("compression-rate-avg") || keyStr.contains("compression-ratio"))) {
                        double value = extractMetricValue(entry.getValue());
                        if (value >= 0) {
                            logger.debug("Found producer-metrics compression metric (string key): {} -> {}", keyStr, value);
                            return value;
                        }
                    }
                }
            }
            return 0;
        }

        /**
         * Get the total outgoing bytes for the *producer* (after compression).
         * Uses producer-metrics group only to keep numbers per-producer rather than per-node.
         */
        public long getOutgoingBytesTotal(java.util.Map<?,?> metricsMap) {
            try {
                if (metricsMap != null) {
                    String targetGroup = "producer-metrics";
                    String[] candidateNames = {"outgoing-byte-total", "byte-total"};

                    for (java.util.Map.Entry<?, ?> entry : metricsMap.entrySet()) {
                        Object key = entry.getKey();

                        // MetricName keys
                        if (key.getClass().getName().endsWith("MetricName")) {
                            try {
                                Method nameMethod = findMethod(key.getClass(), "name");
                                Method groupMethod = findMethod(key.getClass(), "group");
                                if (nameMethod != null && groupMethod != null) {
                                    nameMethod.setAccessible(true);
                                    groupMethod.setAccessible(true);
                                    Object nameObj = nameMethod.invoke(key);
                                    Object groupObj = groupMethod.invoke(key);

                                    if (nameObj == null || groupObj == null) {
                                        continue;
                                    }
                                    
                                    String name = nameObj.toString();
                                    String group = groupObj.toString();

                                    if (targetGroup.equals(group)) {
                                        for (String n : candidateNames) {
                                            if (n.equals(name)) {
                                                double val = extractMetricValue(entry.getValue());
                                                if (val >= 0) {
                                                    logger.debug("Found producer-metrics {} = {}", name, val);
                                                    return (long) val;
                                                }
                                            }
                                        }
                                    }
                                }
                            } catch (Exception ignored) {}
                        } else if (key instanceof String) {
                            String keyStr = (String) key;
                            if (keyStr.startsWith(targetGroup) && (keyStr.contains("outgoing-byte-total") || keyStr.contains("byte-total"))) {
                                double val = extractMetricValue(entry.getValue());
                                if (val >= 0) {
                                    logger.debug("Found producer-metrics byte counter (string key) {} = {}", keyStr, val);
                                    return (long) val;
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.debug("Error getting outgoing bytes total from producer-metrics: {}", e.getMessage());
            }

            return 0;
        }

        /**
         * Extract the metrics map from a Metrics object.
         * Handles both cases where metrics is a Map directly or a Metrics object
         * with an internal 'metrics' field.
         */
        private java.util.Map<?, ?> extractMetricsMap(Object metrics) {
            if (metrics == null) {
                return null;
            }

            try {
                // If it's already a Map, just cast it
                if (metrics instanceof java.util.Map) {
                    return (java.util.Map<?, ?>) metrics;
                }

                // Try to extract the internal metrics map field
                Field metricsField = findField(metrics.getClass(), "metrics");
                if (metricsField != null) {
                    metricsField.setAccessible(true);
                    Object metricsValue = metricsField.get(metrics);
                    if (metricsValue instanceof java.util.Map) {
                        logger.debug("Successfully extracted metrics map from Metrics object");
                        return (java.util.Map<?, ?>) metricsValue;
                    }
                }

                // Try to get metrics through a method
                Method getMetricsMethod = findMethod(metrics.getClass(), "metrics", "getMetrics");
                if (getMetricsMethod != null) {
                    getMetricsMethod.setAccessible(true);
                    Object metricsValue = getMetricsMethod.invoke(metrics);
                    if (metricsValue instanceof java.util.Map) {
                        logger.debug("Successfully extracted metrics map via method");
                        return (java.util.Map<?, ?>) metricsValue;
                    }
                }

                logger.debug("Object is neither a Map nor has a metrics field/method: {}",
                        metrics.getClass().getName());
            } catch (Exception e) {
                logger.debug("Error extracting metrics map: {}", e.getMessage());
            }

            return null;
        }

        /**
         * Extract a numeric value from a metric object.
         */
        public double extractMetricValue(Object metric) {
            if (metric == null) {
                return 0;
            }

            try {
                // Try value() method (common in metrics libraries)
                Method valueMethod = findMethod(metric.getClass(), "metricValue");
                if (valueMethod != null) {
                    valueMethod.setAccessible(true);
                    Object value = valueMethod.invoke(metric);
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                }
            } catch (Exception e) {
                logger.debug("Error extracting metric value: " + e.getMessage());
            }

            return 0;
        }
    }

    /**
     * Simple class to track compression statistics over time.
     */
    public static class CompressionStats {
        public final long compressedBytes;
        public final long uncompressedBytes;

        public CompressionStats(long compressedBytes, long uncompressedBytes) {
            this.compressedBytes = compressedBytes;
            this.uncompressedBytes = uncompressedBytes;
        }
    }

    /**
     * Holder for original and optimized configuration maps passed between optimization
     * phase and stats reporter creation using ThreadLocal.
     */
    public static class ConfigInfo {
        public final java.util.Map<String, Object> originalConfig;
        public final java.util.Map<String, Object> optimizedConfig;
        public final String error;

        public ConfigInfo(java.util.Map<String, Object> orig, java.util.Map<String, Object> opt) {
            this.originalConfig = orig;
            this.optimizedConfig = opt;
            this.error = null;
        }

        public ConfigInfo(java.util.Map<String, Object> orig, java.util.Map<String, Object> opt, String error) {
            this.originalConfig = orig;
            this.optimizedConfig = opt;
            this.error = error;
        }
    }

    /**
     * A Properties view that writes through to a backing Map, ensuring updates are visible
     * to code that continues to use the original Map instance.
     */
    public static class MapBackedProperties extends java.util.Properties {
        private static final long serialVersionUID = 1L;
        private final java.util.Map<String,Object> backing;

        public MapBackedProperties(java.util.Map<String,Object> backing) {
            this.backing = backing;
            super.putAll(backing);
        }

        @Override
        public synchronized Object put(Object key, Object value) {
            try { backing.put(String.valueOf(key), value); } catch (UnsupportedOperationException ignored) {}
            return super.put(key, value);
        }

        @Override
        public synchronized Object remove(Object key) {
            try { backing.remove(String.valueOf(key)); } catch (UnsupportedOperationException ignored) {}
            return super.remove(key);
        }

        @Override
        public synchronized void putAll(java.util.Map<?,?> m) {
            for (java.util.Map.Entry<?,?> e : m.entrySet()) {
                put(e.getKey(), e.getValue());
            }
        }

        @Override
        public String getProperty(String key) {
            Object value = backing.get(key);
            if (value == null) {
                return super.getProperty(key);
            }
            
            // Handle special case for bootstrap.servers which can be any Collection<String>
            if ("bootstrap.servers".equals(key) && value instanceof java.util.Collection) {
                try {
                    @SuppressWarnings("unchecked")
                    java.util.Collection<String> serverCollection = (java.util.Collection<String>) value;
                    return String.join(",", serverCollection);
                } catch (ClassCastException e) {
                    // If the collection doesn't contain strings, fall back to toString()
                    logger.debug("bootstrap.servers collection contains non-String elements, falling back to toString()");
                }
            }
            
            // For all other cases, return the original value
            return value.toString();
        }

        @Override
        public String getProperty(String key, String defaultValue) {
            String result = getProperty(key);
            return result != null ? result : defaultValue;
        }

        @Override
        public Object get(Object key) {
            return backing.get(key);
        }
    }

    // Utility method to convert Properties to Map<String, Object>
    public static java.util.Map<String, Object> propertiesToMap(Properties props) {
        java.util.Map<String, Object> map = new java.util.HashMap<>();
        if (props != null) {
            for (java.util.Map.Entry<Object,Object> entry : props.entrySet()) {
                if (entry.getKey() == null) continue;
                map.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        return map;
    }

    /**
     * Mark a producer as closed.
     *
     * @param producer the producer instance
     * @return {@code true} if this is the first time we saw close() for this instance
     */
    public static boolean markProducerClosed(Object producer) {
        if (producer == null || isDisabled()) {
            return false;
        }
        try {
            String producerId = "producer-" + System.identityHashCode(producer);
            ProducerMetricsInfo info = producerMetricsMap.get(producerId);
            if (info != null) {
                if (info.isActive.getAndSet(false)) {
                    logger.debug("Producer {} marked as closed; metrics collection will stop", producerId);
                    try {
                        ClientStatsReporter reporter = info.getReporter();
                        if (reporter != null) {
                            // Stop the reporter and deregister it from coordinators
                            reporter.deactivate();
                            ai.superstream.core.ClientStatsReporter.deregisterReporter(reporter);
                        }
                    } catch (Exception e) {
                        logger.debug("Error deactivating reporter for {}: {}", producerId, e.getMessage());
                    }

                    // Remove from lookup maps to free memory
                    clientStatsReporters.remove(producerId);
                    producerMetricsMap.remove(producerId);
                    return true;
                } else {
                    return false; // already closed previously
                }
            }
            // no info found
            return false;
        } catch (Exception e) {
            logger.error("[ERR-200] Failed to mark producer as closed: {}", e.getMessage(), e);
            return false;
        }
    }
}
