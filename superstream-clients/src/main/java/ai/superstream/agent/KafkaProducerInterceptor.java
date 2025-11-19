package ai.superstream.agent;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import ai.superstream.agent.KafkaProducerShadowAdvice.RunMode;
import ai.superstream.core.ClientStatsReporter;
import ai.superstream.core.SuperstreamManager;
import ai.superstream.util.ClientUtils;
import ai.superstream.util.SuperstreamLogger;
import net.bytebuddy.asm.Advice;

/**
 * Intercepts KafkaProducer to register metrics and drive per-producer optimization.
 *
 * Behavior
 * - Constructor: capture original Properties, compute run-mode (INIT_CONFIG or SHADOW), register a metrics reporter
 * - SHADOW mode: route selected methods to a single shadow producer and schedule a one-shot optimized swap after a learning period
 * - INIT_CONFIG mode: apply init-time optimization only; no shadow or swaps
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

    // Regex to detect Kafka namespace anywhere in a fully-qualified class name (case-insensitive)
    private static final Pattern KAFKA_NAMESPACE_PATTERN =
            Pattern.compile("(?i)(^|\\.)org\\.apache\\.kafka(\\.|$)");

    // ThreadLocal stack to track nested KafkaProducer constructor calls per thread
    // This ensures that when the Superstream optimization logic creates its own producer
    // inside the application's producer construction, we still match the correct
    // properties object on exit (LIFO order).
    public static final ThreadLocal<Deque<Properties>> TL_PROPS_STACK =
            ThreadLocal.withInitial(ArrayDeque::new);

    // ThreadLocal stack to hold the producer UUIDs generated in onEnter so that the same
    // value can be reused later in onExit (for stats reporting) and by SuperstreamManager
    // when it reports the client information. The stack is aligned with the TL_PROPS_STACK
    // (push in onEnter, pop in onExit).
    public static final ThreadLocal<Deque<String>> TL_UUID_STACK =
            ThreadLocal.withInitial(ArrayDeque::new);

    // ThreadLocal stack to pass original/optimized configuration maps from optimization phase to reporter creation.
    public static final ThreadLocal<Deque<ConfigInfo>> TL_CFG_STACK =
            ThreadLocal.withInitial(ArrayDeque::new);

    // ThreadLocal stack to hold computed run-mode for this constructor invocation (aligned with TL_PROPS_STACK)
    public static final ThreadLocal<Deque<RunMode>> TL_RUN_MODE_STACK =
            ThreadLocal.withInitial(ArrayDeque::new);

    // Static initializer to start the shared collector if enabled
    static {
        if (!DISABLED) {
            try {
                sharedCollector.start();
            } catch (Exception e) {
                logger.error().msg("[ERR-001] Failed to start metrics collector: {}", e.getMessage(), e);
            }
        } else {
            logger.warn().msg("Superstream is disabled via SUPERSTREAM_DISABLED environment variable");
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
                if (a instanceof Map && !(a instanceof Properties)) {
                    args[i] = properties;
                    break;
                }
            }

            TL_PROPS_STACK.get().push(properties);

            // Generate a UUID for this upcoming producer instance and push onto UUID stack
            String producerUuid = UUID.randomUUID().toString();
            TL_UUID_STACK.get().push(producerUuid);

            // Compute run mode
            String client = properties.getProperty("client.id", "");
            logger.info().withClientId(client).msg("[COMPAT-CHECK] Starting compatibility check for KafkaProducer API");
            boolean methodSignatureCompatible = checkMethodSignatureCompatibility();
            if (methodSignatureCompatible) {
                logger.info().withClientId(client).msg("[COMPAT-CHECK] KafkaProducer API compatible (signatures verified)");
            } else {
                logger.info().withClientId(client).msg("[COMPAT-CHECK] Incompatible KafkaProducer API; defaulting to INIT-CONFIG MODE");
            }

            List<String> customClasses = findCustomKafkaClasses(properties);
            if (!customClasses.isEmpty()) {
                logger.info().withClientId(client).msg("[COMPAT-CHECK] Custom classes detected: {}", customClasses);
            } else {
                logger.info().withClientId(client).msg("[COMPAT-CHECK] No custom classes detected");
            }

            // Direct calculation: ON_INIT_CONFIG if signatures are bad OR custom classes present, otherwise SHADOW
            RunMode runMode = (!methodSignatureCompatible || !customClasses.isEmpty())
                    ? RunMode.INIT_CONFIG
                    : RunMode.SHADOW;
            logger.info().withClientId(client).msg("[COMPAT-CHECK] Selected run mode: {}", runMode);
            TL_RUN_MODE_STACK.get().push(runMode);

            // If ON_INIT_CONFIG, apply init-time optimization immediately so app producer reads it
            try {
                if (runMode == RunMode.INIT_CONFIG) {
                    String bootstrap = properties.getProperty("bootstrap.servers", "");
                    if (bootstrap != null && !bootstrap.isEmpty()) {
                        SuperstreamManager.getInstance().optimizeProducer(bootstrap, client, properties);
                        // Mark that init-time optimization was already applied
                        try { properties.setProperty("superstream.optimizedOnInit", "true"); } catch (Throwable ignored) { }
                        logger.info().withClientId(client).msg("[ON-INIT-CONFIG] Applied init-time optimization before constructor");
                    } else {
                        logger.warn().msg("[ON-INIT-CONFIG] bootstrap.servers missing; cannot apply init-time optimization");
                    }
                }
            } catch (Throwable t) {
                logger.error().msg("[ERR-020] [ON-INIT-CONFIG] Failed to apply init-time optimization: {}", t.getMessage(), t);
            }
        }  else {
          logger.error().msg("[ERR-002] Could not extract properties from producer arguments");
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

            Deque<Properties> stack = TL_PROPS_STACK.get();
            if (stack.isEmpty()) {
                logger.error().msg("[ERR-006] No captured properties for this producer constructor; skipping stats reporter setup");
                return;
            }

            Properties producerProps = stack.pop();
            // Pop run-mode and set on instance
            Deque<RunMode> rmStack = TL_RUN_MODE_STACK.get();
            RunMode usedRunMode = rmStack.isEmpty() ? null : rmStack.pop();
            try {
                if (usedRunMode != null) {
                    // Set the run mode for this producer instance
                    KafkaProducerShadowAdvice.setRunModeForInstance(producer, usedRunMode);
                    KafkaProducerShadowAdvice.runModeInit(producer, producerProps);
                }
            } catch (Throwable t) {
                logger.error().msg("[ERR-023] Failed to set run mode on instance: {}", t.getMessage(), t);
            }

            // Retrieve matching UUID for this constructor instance
            Deque<String> uuidStack = TL_UUID_STACK.get();
            String producerUuid = "";
            if (!uuidStack.isEmpty()) {
                producerUuid = uuidStack.pop();
            } else {
                logger.error().msg("[ERR-127] No producer UUID found for this constructor instance");
            }

            // Clean up ThreadLocal when outer-most constructor finishes
            if (stack.isEmpty()) {
                TL_PROPS_STACK.remove();
                TL_UUID_STACK.remove();
                TL_RUN_MODE_STACK.remove();
            }

            String bootstrapServers = producerProps.getProperty("bootstrap.servers");
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                logger.error().msg("[ERR-007] bootstrap.servers missing in captured properties; skipping reporter setup");
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
                logger.debug().withClientId(clientIdForStats).msg("Registering producer with metrics collector: {}", producerId);

                // Create a reporter for this producer instance – pass the original client.id
                ClientStatsReporter reporter = new ClientStatsReporter(bootstrapServers, producerProps, clientIdForStats, producerUuid);

                // Create metrics info for this producer (preserve original properties for future swaps)
                Properties originalPropsCopy = new Properties();
                originalPropsCopy.putAll(producerProps);
                ProducerMetricsInfo metricsInfo = new ProducerMetricsInfo(producer, reporter, originalPropsCopy);

                // Register with the shared collector
                producerMetricsMap.put(producerId, metricsInfo);
                clientStatsReporters.put(producerId, reporter);

                // Pop configuration info from ThreadLocal stack (if any) and attach to reporter
                Deque<ConfigInfo> cfgStack = TL_CFG_STACK.get();
                ConfigInfo cfgInfo = cfgStack.isEmpty()? null : cfgStack.pop();
                if (cfgStack.isEmpty()) {
                    TL_CFG_STACK.remove();
                }
                if (cfgInfo != null) {
                    // Use the original configuration from ConfigInfo and get complete config with defaults
                    Map<String, Object> completeConfig = ClientUtils.getCompleteProducerConfig(cfgInfo.originalConfig);
                    Map<String, Object> optimizedConfig = cfgInfo.optimizedConfig != null ? cfgInfo.optimizedConfig : new HashMap<>();
                    reporter.setConfigurations(completeConfig, optimizedConfig);
                    // If optimizedConfig is empty and there is an error, set the error on the reporter
                    if (optimizedConfig.isEmpty() && cfgInfo.error != null && !cfgInfo.error.isEmpty()) {
                        reporter.updateError(cfgInfo.error);
                    }
                } else {
                    // populate shadow optimized config on hot swap
                    Map<String, Object> originalPropsMap = propertiesToMap(producerProps);
                    Map<String, Object> originalComplete = ClientUtils.getCompleteProducerConfig(originalPropsMap);
                    Map<String, Object> emptyOptimizedConfig = new HashMap<>();
                    reporter.setConfigurations(originalComplete, emptyOptimizedConfig);
                }

                // Trigger immediate metrics collection for this producer
                try {
                    sharedCollector.collectMetricsForProducer(producerId, metricsInfo);
                } catch (Exception e) {
                    logger.error().msg("[ERR-047] Failed to collect immediate metrics for new producer {}: {}", producerId, e.getMessage(), e);
                }

                logger.debug().withClientId(clientIdForStats).msg("Producer {} registered with shared metrics collector", producerId);
            }
        } catch (Exception e) {
            logger.error().msg("[ERR-008] Error registering producer with metrics collector: {}", e.getMessage(), e);
        }
    }

    /**
     * Extract Properties object from constructor arguments.
     */
    public static Properties extractProperties(Object[] args) {
        // Look for Properties or Map in the arguments
        if (args == null) {
            logger.error().msg("[ERR-009] extractProperties: args array is null");
            return null;
        }

        logger.debug().msg("extractProperties: Processing {} arguments", args.length);
        for (Object arg : args) {
            if (arg == null) {
                logger.debug().msg("extractProperties: Found null argument");
                continue;
            }

            String className = arg.getClass().getName();
            logger.debug().msg("extractProperties: Processing argument of type: {}", className);

            if (arg instanceof Properties) {
                logger.debug().msg("extractProperties: Found Properties object");
                Properties props = (Properties) arg;
                normalizeBootstrapServers(props);
                return props;
            }

            if (arg instanceof Map) {
                logger.debug().msg("extractProperties: Found Map object of type: {}", arg.getClass().getName());

                // If the map is unmodifiable we cannot actually modify it later; we still let the caller decide
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) arg;
                    Properties props = new MapBackedProperties(map);
                    logger.debug().msg("extractProperties: Successfully converted Map to Properties");
                    return props;
                } catch (ClassCastException e) {
                    // Not the map type we expected
                    logger.error().msg("[ERR-011] extractProperties: Could not cast Map to Map<String, Object>: {}", e.getMessage(), e);
                    return null;
                }
            }

            // Handle ProducerConfig object which contains properties
            if (className.endsWith("ProducerConfig")) {
                logger.debug().msg("extractProperties: Found ProducerConfig object");
                try {
                    // Try multiple possible field names
                    String[] fieldNames = { "originals", "values", "props", "properties", "configs" };

                    for (String fieldName : fieldNames) {
                        try {
                            Field field = arg.getClass().getDeclaredField(fieldName);
                            field.setAccessible(true);
                            Object fieldValue = field.get(arg);
                            logger.debug().msg("extractProperties: Found field {} with value type: {}", fieldName,
                                    fieldValue != null ? fieldValue.getClass().getName() : "null");

                            if (fieldValue instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> map = (Map<String, Object>) fieldValue;

                                Properties props = new MapBackedProperties(map);
                                logger.debug().msg(
                                        "extractProperties: Successfully converted ProducerConfig field {} to Properties",
                                        fieldName);
                                return props;
                            } else if (fieldValue instanceof Properties) {
                                logger.debug().msg("extractProperties: Found Properties in ProducerConfig field {}",
                                        fieldName);
                                Properties props = (Properties) fieldValue;
                                normalizeBootstrapServers(props);
                                return props;
                            }
                        } catch (NoSuchFieldException e) {
                            // Field doesn't exist, try the next one
                            logger.error().msg("[ERR-017] extractProperties: Field {} not found in ProducerConfig", fieldName);
                            continue;
                        }
                    }

                    // Try to call getters if field access failed
                    logger.debug().msg("extractProperties: Trying getter methods for ProducerConfig");
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
                            logger.debug().msg("extractProperties: Called method {} with result type: {}", method.getName(),
                                    result != null ? result.getClass().getName() : "null");
                            if (result instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> map = (Map<String, Object>) result;

                                Properties props = new MapBackedProperties(map);
                                logger.debug().msg(
                                        "extractProperties: Successfully converted ProducerConfig method {} result to Properties",
                                        method.getName());
                                return props;
                            } else if (result instanceof Properties) {
                                logger.debug().msg("extractProperties: Found Properties in ProducerConfig method {} result",
                                        method.getName());
                                Properties props = (Properties) result;
                                normalizeBootstrapServers(props);
                                return props;
                            }
                        }
                    }

                    // Last resort: Try to get the ProducerConfig's bootstrap.servers value
                    // and create a minimal Properties object
                    logger.debug().msg("extractProperties: Trying last resort method to get bootstrap.servers");
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
                                    logger.debug().msg(
                                            "extractProperties: Created minimal Properties with bootstrap.servers and client.id");
                                    return minProps;
                                }
                            } catch (Exception e) {
                                logger.debug().msg("extractProperties: Failed to get bootstrap.servers from ProducerConfig",
                                        e);
                            }
                        }
                    }

                } catch (Exception e) {
                    logger.error().msg("[ERR-018] extractProperties: Failed to extract properties from ProducerConfig: {}",
                            e.getMessage(), e);
                            return null;
                }
            }
        }

        logger.error().msg("[ERR-019] extractProperties: No valid configuration object found in arguments");
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
        if (bsObj instanceof Collection) {
            Collection<?> col = (Collection<?>) bsObj;
            StringBuilder sb = new StringBuilder();
            for (Object o : col) {
                if (o == null) continue;
                if (sb.length() > 0) sb.append(',');
                sb.append(o.toString());
            }
            joined = sb.toString();
        } else if (bsObj.getClass().isArray()) {
            int len = Array.getLength(bsObj);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < len; i++) {
                Object o = Array.get(bsObj, i);
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
                logger.debug().msg("Detected initial producer creation from: " + className);
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
        private final Properties originalProperties;
        /**
         * Logical cumulative counters across the lifetime of this application producer.
         * Keys are composite identifiers:
         *  - "producer::<metricName>"
         *  - "topic::<topicName>::<metricName>"
         *  - "node::<nodeId>::<metricName>"
         */
        private final Map<String, LogicalCounter> logicalCounters = new ConcurrentHashMap<>();

        public ProducerMetricsInfo(Object producer, ClientStatsReporter reporter, Properties originalProperties) {
            this.producer = producer;
            this.reporter = reporter;
            this.originalProperties = originalProperties;
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

        public void setActive(boolean active) {
            isActive.set(active);
        }

        public Properties getOriginalPropertiesCopy() {
            Properties p = new Properties();
            if (originalProperties != null) {
                p.putAll(originalProperties);
            }
            return p;
        }

        public Map<String, LogicalCounter> getLogicalCounters() {
            return logicalCounters;
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
        // Metric names that should be treated as logical cumulative counters across shadow swaps
        private static final java.util.Set<String> PRODUCER_CUMULATIVE_METRICS = new java.util.HashSet<>();
        private static final java.util.Set<String> TOPIC_CUMULATIVE_METRICS = new java.util.HashSet<>();
        private static final java.util.Set<String> NODE_CUMULATIVE_METRICS = new java.util.HashSet<>();

        static {
            // Producer-level cumulative metrics
            PRODUCER_CUMULATIVE_METRICS.add("outgoing-byte-total");
            PRODUCER_CUMULATIVE_METRICS.add("byte-total");
            PRODUCER_CUMULATIVE_METRICS.add("record-send-total");
            PRODUCER_CUMULATIVE_METRICS.add("record-error-total");
            PRODUCER_CUMULATIVE_METRICS.add("record-retry-total");

            // Topic-level cumulative metrics
            TOPIC_CUMULATIVE_METRICS.add("record-send-total");
            TOPIC_CUMULATIVE_METRICS.add("byte-total");
            TOPIC_CUMULATIVE_METRICS.add("record-error-total");
            TOPIC_CUMULATIVE_METRICS.add("record-retry-total");

            // Node-level cumulative metrics
            NODE_CUMULATIVE_METRICS.add("outgoing-byte-total");
            NODE_CUMULATIVE_METRICS.add("byte-total");
        }

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
                logger.debug().msg("Starting shared Kafka metrics collector with interval {} ms", COLLECTION_INTERVAL_MS);
                try {
                    scheduler.scheduleAtFixedRate(this::collectAllMetrics,
                            COLLECTION_INTERVAL_MS / 2, // Start sooner for first collection
                            COLLECTION_INTERVAL_MS,
                            TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    logger.error().msg("[ERR-012] Failed to schedule metrics collection: {}", e.getMessage(), e);
                    running.set(false);
                }
            } else {
                logger.debug().msg("Metrics collector already running");
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
                    logger.debug().msg("No producers registered for metrics collection");
                    return;
                }

                logger.debug().msg("Starting metrics collection cycle for {} producers", totalProducers);
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
                        logger.error().msg("[ERR-013] Error collecting metrics for producer {}: {}", producerId, e.getMessage(), e);
                    }
                }

                logger.debug().msg(
                        "Completed metrics collection cycle: {} producers processed, {} reported stats, {} skipped",
                        totalProducers, successCount, skippedCount);

            } catch (Exception e) {
                logger.error().msg("[ERR-014] Error in metrics collection cycle: {}", e.getMessage(), e);
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
                String cid = KafkaProducerShadowAdvice.getOriginalClientId(producer);
                // Safety: Prefer collecting from the active shadow producer if present so
                // metrics reflect the actual sender post-swap and we avoid double-counting.
                boolean usingShadow = false;
                String activeShadowUuid = null;
                try {
                    if (KafkaProducerShadowAdvice.isShadowReady(producer)) {
                        KafkaProducerShadowAdvice.ShadowProducer active = KafkaProducerShadowAdvice.getGlobalShadowProducer(info.getProducer());
                        if (active != null) {
                            activeShadowUuid = active.getInternalUuid();
                        }
                        Object shadow = KafkaProducerShadowAdvice.getShadow(producer);
                        if (shadow != null) {
                            producer = shadow;
                            usingShadow = true;
                        }
                    }
                } catch (Throwable ignored) { }
                ClientStatsReporter reporter = info.getReporter();

                // Get the metrics object from the producer
                Object metrics = extractFieldValue(producer, "metrics");
                if (metrics == null) {
                    logger.debug().withClientId(cid).msg("No metrics object found in producer {}", producerId);
                    return false;
                }

                // Extract the metrics map once per invocation and reuse in subsequent calculations
                Map<?,?> metricsMap = extractMetricsMap(metrics);

                if (activeShadowUuid != null) {
                    logger.debug().withClientId(cid).msg("Collecting metrics from shadow UUID {}", activeShadowUuid);
                }

                // Try to get the compression ratio metric; fall back to 1.0 (no compression)
                double compressionRatio = getCompressionRatio(metricsMap);
                if (compressionRatio <= 0) {
                    logger.debug().withClientId(cid).msg("No compression ratio metric found; assuming ratio 1.0 for producer {}", producerId);
                    compressionRatio = 1.0;
                }

                // Get the outgoing-byte-total metric (compressed bytes). This is cumulative over time.
                long totalOutgoingBytes = getOutgoingBytesTotal(metricsMap);

                // Create snapshots for different metric types
                Map<String, Double> allMetricsSnapshot = new HashMap<>();
                Map<String, Map<String, Double>> topicMetricsSnapshot = new HashMap<>();
                Map<String, Map<String, Double>> nodeMetricsSnapshot = new HashMap<>();
                Map<String, String> appInfoMetricsSnapshot = new HashMap<>();

                try {
                    Map<?, ?> rawMetricsMap = metricsMap;
                    if (rawMetricsMap != null) {
                        for (Map.Entry<?, ?> mEntry : rawMetricsMap.entrySet()) {
                            Object mKey = mEntry.getKey();
                            String group = null;
                            String namePart;
                            String keyString = null;
                            String topicName = null;
                            String nodeId = null;
                            if (mKey == null) continue;

                            if (mKey.getClass().getName().endsWith("MetricName")) {
                                try {
                                    Method nameMethod = findMethod(mKey.getClass(), "name");
                                    Method groupMethod = findMethod(mKey.getClass(), "group");
                                    Method tagsMethod = findMethod(mKey.getClass(), "tags");
                                    namePart = (nameMethod != null) ? nameMethod.invoke(mKey).toString() : mKey.toString();
                                    group = (groupMethod != null) ? groupMethod.invoke(mKey).toString() : "";
                                    if ("producer-metrics".equals(group)) {
                                        keyString = namePart;
                                    } else if ("producer-topic-metrics".equals(group)) {
                                        if (tagsMethod != null) {
                                            tagsMethod.setAccessible(true);
                                            Object tagObj = tagsMethod.invoke(mKey);
                                            if (tagObj instanceof Map) {
                                                Object topicObj = ((Map<?,?>)tagObj).get("topic");
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
                                            if (tagObj instanceof Map) {
                                                Object nodeIdObj = ((Map<?,?>)tagObj).get("node-id");
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
                                        Method metricValueMethod = value.getClass().getMethod("metricValue");
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
                                    double valueToStore = mVal;
                                    if (TOPIC_CUMULATIVE_METRICS.contains(keyString)) {
                                        valueToStore = computeLogicalCounter(info, "topic", topicName, keyString, mVal, activeShadowUuid);
                                    }
                                    topicMetricsSnapshot.computeIfAbsent(topicName, k -> new HashMap<>())
                                            .put(keyString, valueToStore);
                                } else if (nodeId != null) {
                                    double valueToStore = mVal;
                                    if (NODE_CUMULATIVE_METRICS.contains(keyString)) {
                                        valueToStore = computeLogicalCounter(info, "node", nodeId, keyString, mVal, activeShadowUuid);
                                    }
                                    nodeMetricsSnapshot.computeIfAbsent(nodeId, k -> new HashMap<>())
                                            .put(keyString, valueToStore);
                                } else {
                                    double valueToStore = mVal;
                                    if (PRODUCER_CUMULATIVE_METRICS.contains(keyString)) {
                                        valueToStore = computeLogicalCounter(info, "producer", null, keyString, mVal, activeShadowUuid);
                                    }
                                    allMetricsSnapshot.put(keyString, valueToStore);
                                }
                            }
                        }
                    }
                } catch (Exception snapshotEx) {
                logger.error().withClientId(cid).msg("[ERR-015] Error extracting metrics snapshot for producer {}: {}", producerId, snapshotEx.getMessage(), snapshotEx);
                }

                // Update reporter with latest metrics snapshots
                reporter.updateProducerMetrics(allMetricsSnapshot);
                reporter.updateTopicMetrics(topicMetricsSnapshot);
                reporter.updateNodeMetrics(nodeMetricsSnapshot);
                // If using a shadow, prefer original producer app-info values to keep them consistent
                if (usingShadow) {
                    try {
                        Map<String,String> originalAppInfo = collectAppInfoMetricsFrom(info.getProducer());
                        if (originalAppInfo != null && !originalAppInfo.isEmpty()) {
                            appInfoMetricsSnapshot.putAll(originalAppInfo);
                        }
                    } catch (Throwable ignored) { }
                }
                reporter.updateAppInfoMetrics(appInfoMetricsSnapshot);

                // Update reporter with observed topics discovered via .send() routing
                try {
                    List<String> observedTopics = KafkaProducerShadowAdvice.getObservedTopics(info.getProducer());
                    if (observedTopics != null && !observedTopics.isEmpty()) {
                        reporter.addTopics(observedTopics);
                    }
                } catch (Throwable ignored) { }

                // Compute compression statistics for this interval (delta) based on logical outgoing bytes.
                // Prefer the logical cumulative value from producer_metrics; fall back to the raw Kafka counter if missing.
                double logicalOutgoingDouble = 0;
                Double logicalOutgoingMetric = allMetricsSnapshot.get("outgoing-byte-total");
                if (logicalOutgoingMetric != null) {
                    logicalOutgoingDouble = logicalOutgoingMetric;
                } else {
                    logicalOutgoingDouble = totalOutgoingBytes;
                }
                long logicalOutgoingTotal = (long) logicalOutgoingDouble;

                CompressionStats prevStats = info.getLastStats();
                long compressedBytes = Math.max(0, logicalOutgoingTotal - prevStats.compressedBytes);

                // Use the compression ratio to calculate the uncompressed size delta
                // compression_ratio = compressed_size / uncompressed_size
                // Therefore: uncompressed_size = compressed_size / compression_ratio
                long uncompressedBytes = (compressionRatio > 0 && compressedBytes > 0)
                        ? (long) (compressedBytes / compressionRatio)
                        : 0;

                // Update the last stats with the new logical total and cumulative uncompressed bytes
                info.updateLastStats(
                        new CompressionStats(logicalOutgoingTotal, prevStats.uncompressedBytes + uncompressedBytes));

                // Report the compression statistics for this interval (delta)
                reporter.recordBatch(uncompressedBytes, compressedBytes);

                logger.debug().withClientId(cid).msg("Producer {} compression collected: before={} bytes, after={} bytes, ratio={}",
                        producerId, uncompressedBytes, compressedBytes, String.format("%.4f", compressionRatio));
                        
                return true;
            } catch (Exception e) {
                logger.warn().withClientId(KafkaProducerShadowAdvice.getOriginalClientId(info.getProducer())).msg("[ERR-016] Error collecting Kafka metrics for producer {}: {}", producerId, e.getMessage(), e);
                return false;
            }
        }

        /**
         * Compute a logical cumulative counter value for the given metric, scoped to the
         * application producer and (optionally) topic or node. This allows counters such
         * as outgoing-byte-total to continue increasing across shadow swaps.
         */
        private double computeLogicalCounter(ProducerMetricsInfo info,
                                             String scope,
                                             String qualifier,
                                             String metricName,
                                             double kafkaValue,
                                             String activeShadowUuid) {
            if (Double.isNaN(kafkaValue)) {
                return 0.0;
            }

            StringBuilder keyBuilder = new StringBuilder(scope).append("::");
            if (qualifier != null) {
                keyBuilder.append(qualifier).append("::");
            }
            keyBuilder.append(metricName);
            String key = keyBuilder.toString();

            Map<String, LogicalCounter> counters = info.getLogicalCounters();
            LogicalCounter counter = counters.get(key);
            if (counter == null) {
                counter = new LogicalCounter();
                counter.lastShadowUuid = activeShadowUuid;
                counter.lastKafkaValue = kafkaValue;
                counter.logicalTotal = kafkaValue;
                counters.put(key, counter);
                return counter.logicalTotal;
            }

            boolean shadowChanged = activeShadowUuid != null && !activeShadowUuid.equals(counter.lastShadowUuid);
            double delta;
            if (shadowChanged) {
                // New shadow producer instance: its Kafka counter starts from 0, but logically
                // we want to keep accumulating total bytes/records across the swap.
                delta = kafkaValue;
                counter.lastShadowUuid = activeShadowUuid;
            } else {
                delta = kafkaValue - counter.lastKafkaValue;
                if (delta < 0) {
                    // Counter reset (producer restart or metric reset); treat current value as fresh delta.
                    delta = kafkaValue;
                }
            }

            if (delta > 0) {
                counter.logicalTotal += delta;
            }
            counter.lastKafkaValue = kafkaValue;
            return counter.logicalTotal;
        }

        // Collect only app-info metrics from a given producer instance
        private Map<String,String> collectAppInfoMetricsFrom(Object producer) {
            Map<String,String> out = new HashMap<>();
            try {
                if (producer == null) return out;
                Object metrics = extractFieldValue(producer, "metrics");
                if (metrics == null) return out;
                Map<?,?> metricsMap = extractMetricsMap(metrics);
                if (metricsMap == null) return out;
                for (Map.Entry<?, ?> mEntry : metricsMap.entrySet()) {
                    Object mKey = mEntry.getKey();
                    String group = null;
                    String keyString = null;
                    if (mKey == null) continue;
                    if (mKey.getClass().getName().endsWith("MetricName")) {
                        try {
                            Method nameMethod = findMethod(mKey.getClass(), "name");
                            Method groupMethod = findMethod(mKey.getClass(), "group");
                            if (nameMethod != null) keyString = (nameMethod.invoke(mKey)).toString();
                            if (groupMethod != null) group = (groupMethod.invoke(mKey)).toString();
                        } catch (Exception ignored) { }
                    } else if (mKey instanceof String) {
                        keyString = mKey.toString();
                        if (keyString.startsWith("app-info.")) {
                            group = "app-info";
                            keyString = keyString.substring("app-info.".length());
                        }
                    }
                    if (keyString == null) continue;
                    if ("app-info".equals(group)) {
                        Object value = mEntry.getValue();
                        String stringValue = null;
                        if (value != null) {
                            try {
                                java.lang.reflect.Method metricValueMethod = value.getClass().getMethod("metricValue");
                                Object actualValue = metricValueMethod.invoke(value);
                                stringValue = (actualValue != null) ? actualValue.toString() : null;
                            } catch (Exception e) {
                                stringValue = value.toString();
                            }
                        }
                        if (stringValue != null) {
                            out.put(keyString, stringValue);
                        }
                    }
                }
            } catch (Throwable ignored) { }
            return out;
        }

        // Topic-specific swap execution is delegated to KafkaProducerShadowAdvice.swapTopicShadow,
        // which flushes and closes the prior shadow before installing the new optimized one.
        /**
         * Get the compression ratio from the metrics object.
         */
        public double getCompressionRatio(Map<?,?> metricsMap) {
            try {
                if (metricsMap != null) {
                    logger.debug().msg("Metrics map size: {}", metricsMap.size());

                    double compressionRatio = findDirectCompressionMetric(metricsMap);
                    if (compressionRatio > 0) {
                        return compressionRatio;
                    }
                }
            } catch (Exception e) {
                logger.debug().msg("Error getting compression ratio: " + e.getMessage(), e);
            }
            return 0;
        }

        /**
         * Find direct compression metrics in the metrics map.
         */
        private double findDirectCompressionMetric(Map<?, ?> metricsMap) {
            // Look for compression metrics in the *producer-metrics* group only
            for (Map.Entry<?, ?> entry : metricsMap.entrySet()) {
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
                                    logger.debug().msg("Found producer-metrics compression metric: {} -> {}", name, value);
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
                            logger.debug().msg("Found producer-metrics compression metric (string key): {} -> {}", keyStr, value);
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
        public long getOutgoingBytesTotal(Map<?,?> metricsMap) {
            try {
                if (metricsMap != null) {
                    String targetGroup = "producer-metrics";
                    String[] candidateNames = {"outgoing-byte-total", "byte-total"};

                    for (Map.Entry<?, ?> entry : metricsMap.entrySet()) {
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
                                                    logger.debug().msg("Found producer-metrics {} = {}", name, val);
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
                                    logger.debug().msg("Found producer-metrics byte counter (string key) {} = {}", keyStr, val);
                                    return (long) val;
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.debug().msg("Error getting outgoing bytes total from producer-metrics: {}", e.getMessage());
            }

            return 0;
        }

        /**
         * Extract the metrics map from a Metrics object.
         * Handles both cases where metrics is a Map directly or a Metrics object
         * with an internal 'metrics' field.
         */
        private Map<?, ?> extractMetricsMap(Object metrics) {
            if (metrics == null) {
                return null;
            }

            try {
                // If it's already a Map, just cast it
                if (metrics instanceof Map) {
                    return (Map<?, ?>) metrics;
                }

                // Try to extract the internal metrics map field
                Field metricsField = findField(metrics.getClass(), "metrics");
                if (metricsField != null) {
                    metricsField.setAccessible(true);
                    Object metricsValue = metricsField.get(metrics);
                    if (metricsValue instanceof Map) {
                        logger.debug().msg("Successfully extracted metrics map from Metrics object");
                        return (Map<?, ?>) metricsValue;
                    }
                }

                // Try to get metrics through a method
                Method getMetricsMethod = findMethod(metrics.getClass(), "metrics", "getMetrics");
                if (getMetricsMethod != null) {
                    getMetricsMethod.setAccessible(true);
                    Object metricsValue = getMetricsMethod.invoke(metrics);
                    if (metricsValue instanceof Map) {
                        logger.debug().msg("Successfully extracted metrics map via method");
                        return (Map<?, ?>) metricsValue;
                    }
                }

                logger.debug().msg("Object is neither a Map nor has a metrics field/method: {}",
                        metrics.getClass().getName());
            } catch (Exception e) {
                logger.debug().msg("Error extracting metrics map: {}", e.getMessage());
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
                logger.debug().msg("Error extracting metric value: " + e.getMessage());
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
     * Tracks a logical cumulative counter for a specific metric key across
     * producer lifecycles (including shadow swaps).
     */
    public static class LogicalCounter {
        String lastShadowUuid;
        double lastKafkaValue;
        double logicalTotal;
    }

    /**
     * Holder for original and optimized configuration maps passed between optimization
     * phase and stats reporter creation using ThreadLocal.
     */
    public static class ConfigInfo {
        public final Map<String, Object> originalConfig;
        public final Map<String, Object> optimizedConfig;
        public final String error;

        public ConfigInfo(Map<String, Object> orig, Map<String, Object> opt) {
            this.originalConfig = orig;
            this.optimizedConfig = opt;
            this.error = null;
        }

        public ConfigInfo(Map<String, Object> orig, Map<String, Object> opt, String error) {
            this.originalConfig = orig;
            this.optimizedConfig = opt;
            this.error = error;
        }
    }

    /**
     * A Properties view that writes through to a backing Map, ensuring updates are visible
     * to code that continues to use the original Map instance.
     */
    public static class MapBackedProperties extends Properties {
        private static final long serialVersionUID = 1L;
        private final Map<String,Object> backing;

        public MapBackedProperties(Map<String,Object> backing) {
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
        public synchronized void putAll(Map<?,?> m) {
            for (Map.Entry<?,?> e : m.entrySet()) {
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
            if ("bootstrap.servers".equals(key) && value instanceof Collection) {
                try {
                    @SuppressWarnings("unchecked")
                    Collection<String> serverCollection = (Collection<String>) value;
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
    public static Map<String, Object> propertiesToMap(Properties props) {
        Map<String, Object> map = new HashMap<>();
        if (props != null) {
            for (Map.Entry<Object,Object> entry : props.entrySet()) {
                if (entry.getKey() == null) continue;
                map.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        return map;
    }

    public static List<String> findCustomKafkaClasses(Properties props) {
        List<String> reasons = new ArrayList<>();
        if (props == null) return reasons;
        BiConsumer<String,Object> check = (k, v) -> {
            List<String> names = toClassNames(v);
            for (String cn : names) {
                if (cn == null) continue;
                String c = cn.trim();
                if (c.isEmpty()) continue;
                if (c.indexOf('.') < 0) continue; // only FQCNs considered
                boolean isKafka = KAFKA_NAMESPACE_PATTERN.matcher(c).find();
                if (isKafka) continue; // Kafka class, not custom
                reasons.add(k + "=" + c);
            }
        };
        check.accept("key.serializer", props.get("key.serializer"));
        check.accept("value.serializer", props.get("value.serializer"));
        check.accept("partitioner.class", props.get("partitioner.class"));
        check.accept("interceptor.classes", props.get("interceptor.classes"));
        check.accept("metric.reporters", props.get("metric.reporters"));
        check.accept("sasl.login.callback.handler.class", props.get("sasl.login.callback.handler.class"));
        check.accept("sasl.client.callback.handler.class", props.get("sasl.client.callback.handler.class"));
        return reasons;
    }

    public static boolean checkMethodSignatureCompatibility() {
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Class<?> kp = Class.forName("org.apache.kafka.clients.producer.KafkaProducer", false, cl);
            Class<?> pr = Class.forName("org.apache.kafka.clients.producer.ProducerRecord", false, cl);
            Class<?> cb = Class.forName("org.apache.kafka.clients.producer.Callback", false, cl);
            Class<?> cgm = Class.forName("org.apache.kafka.clients.consumer.ConsumerGroupMetadata", false, cl);
            Class<?> future = Future.class;
            Class<?> map = Map.class;
            Class<?> list = List.class;

            Method m;
            m = kp.getMethod("send", pr);
            if (!future.isAssignableFrom(m.getReturnType())) return false;
            m = kp.getMethod("send", pr, cb);
            if (!future.isAssignableFrom(m.getReturnType())) return false;
            m = kp.getMethod("flush");
            if (m.getReturnType() != void.class) return false;
            m = kp.getMethod("close");
            if (m.getReturnType() != void.class) return false;
            String[] txn = {"initTransactions","beginTransaction","commitTransaction","abortTransaction"};
            for (String t : txn) {
                m = kp.getMethod(t);
                if (m.getReturnType() != void.class) return false;
            }
            try {
                m = kp.getMethod("sendOffsetsToTransaction", map, String.class);
                if (m.getReturnType() != void.class) return false;
            } catch (NoSuchMethodException ignore) {
                m = kp.getMethod("sendOffsetsToTransaction", map, cgm);
                if (m.getReturnType() != void.class) return false;
            }
            m = kp.getMethod("metrics");
            if (!map.isAssignableFrom(m.getReturnType())) return false;
            m = kp.getMethod("partitionsFor", String.class);
            if (!list.isAssignableFrom(m.getReturnType())) return false;
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    public static List<String> toClassNames(Object val) {
        List<String> out = new ArrayList<>();
        if (val == null) return out;
        if (val instanceof Class) {
            out.add(((Class<?>) val).getName());
            return out;
        }
        if (val instanceof Collection) {
            for (Object o : (Collection<?>) val) {
                out.addAll(toClassNames(o));
            }
            return out;
        }
        String s = String.valueOf(val);
        if (s == null) return out;
        s = s
            .replace('\u00A0', ' ')
            .replace("\u200B", "")
            .replace("\uFEFF", "")
            .replace("\"", "")
            .replace("'", "")
            .trim();
        if (s.isEmpty()) return out;
        if (s.startsWith("class ")) s = s.substring(6).trim();
        if (s.endsWith(".class")) s = s.substring(0, s.length() - 6).trim();
        for (String token : s.split(",")) {
            String t = token.trim();
            if (t.isEmpty()) continue;
            out.add(t);
        }
        return out;
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
                    String cid = KafkaProducerShadowAdvice.getOriginalClientId(producer);
                    logger.debug().withClientId(cid).msg("Producer {} marked as closed; metrics collection will stop", producerId);
                    try {
                        ClientStatsReporter reporter = info.getReporter();
                        if (reporter != null) {
                            // Stop the reporter and deregister it from coordinators
                            reporter.deactivate();
                            ClientStatsReporter.deregisterReporter(reporter);
                        }
                    } catch (Exception e) {
                        logger.debug("Error deactivating reporter for {}: {}", producerId, e.getMessage());
                    }

                        try {
                            KafkaProducerShadowAdvice.closeShadowProducerForInstance(producer);
                        } catch (Exception e) {
                            logger.debug("Error closing topic shadows for {}: {}", producerId, e.getMessage());
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

    // Expose optimized configuration update to advice after one-shot swap
    public static void updateReporterOptimizedConfigFor(Object appProducer, Properties originalProps, Properties optimizedProps) {
        if (appProducer == null || isDisabled()) return;
        try {
            String producerId = "producer-" + System.identityHashCode(appProducer);
            ProducerMetricsInfo info = producerMetricsMap.get(producerId);
            if (info == null) return;
            ClientStatsReporter reporter = info.getReporter();
            if (reporter == null) return;
            Map<String, Object> originalMap = propertiesToMap(originalProps);
            Map<String, Object> optimizedMap = propertiesToMap(optimizedProps);
            Map<String, Object> originalComplete = ClientUtils.getCompleteProducerConfig(originalMap);
            Map<String, Object> optimizedComplete = ClientUtils.getCompleteProducerConfig(optimizedMap);
            reporter.setConfigurations(originalComplete, optimizedComplete);
        } catch (Throwable ignored) { }
    }
}
