package ai.superstream.shadow;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import ai.superstream.util.SuperstreamLogger;

/**
 * Creates and operates the shaded KafkaProducer and adapts records/callbacks
 * across the shading boundary.
 *
 * Responsibilities
 * - Build a shaded {@code KafkaProducer} from the application's (unshaded)
 *   config by invoking {@link ConfigTranslator#translate(Properties)}.
 * - Convert unshaded {@code ProducerRecord<K,V>} and headers to shaded
 *   equivalents for {@code send(...)}.
 * - Adapt shaded {@code Callback} and {@code Future<RecordMetadata>} results
 *   back to the unshaded API for the application caller.
 *
 * Why this is needed
 * - The shadow producer runs in a shaded namespace to avoid conflicts with the
 *   application's Kafka classes. We cannot pass unshaded classes/instances
 *   (records, callbacks, serializers) directly to the shaded API.
 * - This manager centralizes translation in both directions so the advice code
 *   remains small and the application does not need to change.
 *
 * Notes
 * - Only the public Kafka API surface is used (topic/partition/timestamp,
 *   headers, key, value). Reflection is used on unshaded inputs to avoid
 *   linking against unshaded Kafka at compile time.
 * - RecordMetadata mapping is best-effort for Kafka 3.3.x constructors; if a
 *   perfect match is not found, we return null metadata rather than fail the
 *   application.
 */
public final class ShadowProducerManager {

    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(ShadowProducerManager.class);
    private static final Map<Object, String> SHADOW_IDS = Collections.synchronizedMap(new WeakHashMap<>());

    private ShadowProducerManager() { }

    /**
     * Create a shaded KafkaProducer from the application's properties.
     * The config is translated to shaded-safe values (serializers/partitioner).
     *
     * @param originalProps The original unshaded producer properties
     * @return A shaded KafkaProducer instance wrapped as Object
     */
    public static Object createShadowProducer(Properties originalProps) {
        Properties translated = ConfigTranslator.translate(originalProps);
        KafkaProducer<Object, Object> shadowProducer =
                new KafkaProducer<>(translated);
        String id = "shadowProducer-" + UUID.randomUUID();
        SHADOW_IDS.put(shadowProducer, id);
        logger.debug("[SHADOW-ID] created {}", id);
        return shadowProducer;
    }

    /**
     * Get or generate a unique identifier for a shadow producer instance.
     *
     * @param shadowProducer The shadow producer instance, may be null
     * @return A unique identifier string for the shadow producer
     */
    public static String getShadowId(Object shadowProducer) {
        if (shadowProducer == null) return "shadowProducer-null";
        String id = SHADOW_IDS.get(shadowProducer);
        if (id == null) {
            id = "shadowProducer-" + Integer.toHexString(System.identityHashCode(shadowProducer));
            SHADOW_IDS.put(shadowProducer, id);
        }
        return id;
    }

    /**
     * Send a record by converting the unshaded {@code ProducerRecord} to the shaded
     * equivalent and returning a Future that adapts shaded {@code RecordMetadata}
     * back to the unshaded API on get().
     *
     * @param shadedProducer The shaded KafkaProducer instance
     * @param unshadedRecord The unshaded ProducerRecord to send
     * @return A Future that will yield unshaded RecordMetadata on get()
     * @throws IllegalArgumentException if unshadedRecord is null
     * @throws Exception if record conversion or send operation fails
     */
    public static Future<?> send(Object shadedProducer, Object unshadedRecord) throws Exception {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        if (unshadedRecord == null) {
            logger.debug().msg("[SHADOW-EXECUTION] Send(ProducerRecord) received null record; throwing IllegalArgumentException");
            throw new IllegalArgumentException("ProducerRecord must not be null");
        }
        ProducerRecord<Object, Object> shadedRecord = adaptRecordToShaded(unshadedRecord);
        String topic = null;
        try { topic = (String) unshadedRecord.getClass().getMethod("topic").invoke(unshadedRecord); } catch (Throwable ignored) {}
        logger.debug().forTopic(topic != null ? topic : "").msg("[SHADOW-EXECUTION] executed method Send(ProducerRecord)");
        Future<RecordMetadata> f = shadowProducer.send(shadedRecord);
        return new FutureAdapter(f, Thread.currentThread().getContextClassLoader());
    }

    /**
     * Send a record with callback. Wraps the application's callback so it receives
     * unshaded {@code RecordMetadata} and the original {@code Exception}.
     *
     * @param shadedProducer The shaded KafkaProducer instance
     * @param unshadedRecord The unshaded ProducerRecord to send
     * @param unshadedCallback The unshaded Callback to invoke on completion, may be null
     * @return A Future that will yield unshaded RecordMetadata on get()
     * @throws IllegalArgumentException if unshadedRecord is null
     * @throws Exception if record conversion or send operation fails
     */
    public static Future<?> send(Object shadedProducer, Object unshadedRecord, Object unshadedCallback) throws Exception {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        if (unshadedRecord == null) {
            logger.debug().msg("[SHADOW-EXECUTION] Send(ProducerRecord, Callback) received null record; throwing IllegalArgumentException");
            throw new IllegalArgumentException("ProducerRecord must not be null");
        }
        ProducerRecord<Object, Object> shadedRecord = adaptRecordToShaded(unshadedRecord);
        Callback shadedCb = (unshadedCallback != null) ? new CallbackAdapter(unshadedCallback) : null;
        String topic = null;
        try { topic = (String) unshadedRecord.getClass().getMethod("topic").invoke(unshadedRecord); } catch (Throwable ignored) {}
        logger.debug().forTopic(topic != null ? topic : "").msg("[SHADOW-EXECUTION] executed method Send(ProducerRecord, Callback)");
        Future<RecordMetadata> f = shadowProducer.send(shadedRecord, shadedCb);
        return new FutureAdapter(f, deriveAppClassLoader(unshadedCallback));
    }

    /**
     * Flush all buffered records in the shadow producer.
     *
     * @param shadedProducer The shaded KafkaProducer instance
     */
    public static void flush(Object shadedProducer) {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        logger.debug().msg("[SHADOW-EXECUTION] executed method Flush()");
        shadowProducer.flush();
    }

    /**
     * Close the shadow producer, releasing all resources.
     *
     * @param shadedProducer The shaded KafkaProducer instance
     */
    public static void close(Object shadedProducer) {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        logger.debug().msg("[SHADOW-EXECUTION] executed method Close()");
        shadowProducer.close();
    }

    // --- Transactional APIs ---

    /**
     * Initialize transactions for the shadow producer.
     *
     * @param shadedProducer The shaded KafkaProducer instance
     */
    public static void initTransactions(Object shadedProducer) {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        logger.debug().msg("[SHADOW-EXECUTION] executed method InitTransactions()");
        shadowProducer.initTransactions();
    }

    /**
     * Begin a new transaction in the shadow producer.
     *
     * @param shadedProducer The shaded KafkaProducer instance
     */
    public static void beginTransaction(Object shadedProducer) {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        logger.debug().msg("[SHADOW-EXECUTION] executed method BeginTransaction()");
        shadowProducer.beginTransaction();
    }

    /**
     * Commit the current transaction in the shadow producer.
     *
     * @param shadedProducer The shaded KafkaProducer instance
     */
    public static void commitTransaction(Object shadedProducer) {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        logger.debug().msg("[SHADOW-EXECUTION] executed method CommitTransaction()");
        shadowProducer.commitTransaction();
    }

    /**
     * Abort the current transaction in the shadow producer.
     *
     * @param shadedProducer The shaded KafkaProducer instance
     */
    public static void abortTransaction(Object shadedProducer) {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        logger.debug().msg("[SHADOW-EXECUTION] executed method AbortTransaction()");
        shadowProducer.abortTransaction();
    }

    /**
     * Send offsets to the current transaction. Translates unshaded TopicPartition and
     * OffsetAndMetadata objects to their shaded equivalents.
     *
     * @param shadedProducer The shaded KafkaProducer instance
     * @param offsets Map of unshaded TopicPartition to unshaded OffsetAndMetadata
     * @param groupOrMetadata Either a String (groupId) or ConsumerGroupMetadata instance
     * @throws Exception if translation or method invocation fails
     */
    public static void sendOffsetsToTransaction(Object shadedProducer, Object offsets, Object groupOrMetadata) throws Exception {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        // Translate map keys/values to shaded TopicPartition/OffsetAndMetadata when needed
        Map<?,?> input = (Map<?,?>) offsets;
        Map<Object,Object> translated = new HashMap<>();
        ClassLoader shadedCl = shadowProducer.getClass().getClassLoader();
        Class<?> shadedTpCls = Class.forName("org.apache.kafka.common.TopicPartition", false, shadedCl);
        Class<?> shadedOamCls = Class.forName("org.apache.kafka.clients.consumer.OffsetAndMetadata", false, shadedCl);
        Constructor<?> shadedTpCtor = shadedTpCls.getConstructor(String.class, int.class);
        Constructor<?> shadedOamCtor = null;
        try { shadedOamCtor = shadedOamCls.getConstructor(long.class); } catch (Throwable ignored) {}
        if (shadedOamCtor == null) { shadedOamCtor = shadedOamCls.getConstructors()[0]; }

        for (Map.Entry<?,?> e : input.entrySet()) {
            Object unTp = e.getKey();
            Object unOam = e.getValue();
            String topic = (String) unTp.getClass().getMethod("topic").invoke(unTp);
            int partition = (int) unTp.getClass().getMethod("partition").invoke(unTp);
            Object tpShaded = shadedTpCtor.newInstance(topic, partition);
            long offset = (long) unOam.getClass().getMethod("offset").invoke(unOam);
            Object oamShaded;
            if (shadedOamCtor.getParameterCount() == 1) {
                // Simple constructor: OffsetAndMetadata(long offset)
                oamShaded = shadedOamCtor.newInstance(offset);
            } else {
                // Extended constructor: OffsetAndMetadata(long offset, String metadata, long leaderEpoch)
                // Pass offset in first position, null for optional parameters
                Object[] params = new Object[shadedOamCtor.getParameterCount()];
                for (int i = 0; i < params.length; i++) params[i] = (i == 0) ? offset : null;
                oamShaded = shadedOamCtor.newInstance(params);
            }
            translated.put(tpShaded, oamShaded);
        }

        // Dispatch to the appropriate overload based on second argument type
        try {
            if (groupOrMetadata instanceof String) {
                // Overload: sendOffsetsToTransaction(Map, String)
                Method m = shadowProducer.getClass().getMethod("sendOffsetsToTransaction", Map.class, String.class);
                logger.debug().msg("[SHADOW-EXECUTION] executed method SendOffsetsToTransaction(Map,String)");
                m.invoke(shadowProducer, translated, (String) groupOrMetadata);
            } else {
                // Overload: sendOffsetsToTransaction(Map, ConsumerGroupMetadata)
                Class<?> shadedCgmCls = Class.forName("org.apache.kafka.clients.consumer.ConsumerGroupMetadata", false, shadedCl);
                Object shadedCgm = groupOrMetadata;
                if (!shadedCgmCls.isInstance(groupOrMetadata)) {
                    // Unshaded ConsumerGroupMetadata: extract groupId and construct shaded equivalent
                    try {
                        String gid = (String) groupOrMetadata.getClass().getMethod("groupId").invoke(groupOrMetadata);
                        Constructor<?> c = shadedCgmCls.getConstructor(String.class);
                        shadedCgm = c.newInstance(gid);
                    } catch (Throwable ignored) {}
                }
                Method m = shadowProducer.getClass().getMethod("sendOffsetsToTransaction", Map.class, shadedCgmCls);
                logger.debug().msg("[SHADOW-EXECUTION] executed method SendOffsetsToTransaction(Map,ConsumerGroupMetadata)");
                m.invoke(shadowProducer, translated, shadedCgm);
            }
        } catch (NoSuchMethodException nsme) {
            // Method not available in this Kafka version - gracefully skip (best-effort compatibility)
            // This allows the code to work across different Kafka versions
        }
    }

    // --- Read/Query translations ---

    /**
     * Get metrics from the shadow producer and adapt them to unshaded MetricName/Metric types.
     * Uses the application's classloader to resolve unshaded types.
     *
     * @param shadedProducer The shaded KafkaProducer instance
     * @param appCl The application's classloader for resolving unshaded types
     * @return Map of unshaded MetricName to unshaded Metric instances
     */
    @SuppressWarnings("unchecked")
    public static Map<Object,Object> metrics(Object shadedProducer, ClassLoader appCl) {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        logger.debug().msg("[SHADOW-EXECUTION] executed method Metrics()");
        Map<Object,Object> out = new HashMap<>();
        try {
            Map<?,?> shadedMap = shadowProducer.metrics();
            if (shadedMap == null) return out;

			// Resolve unshaded types using the application's classloader (prefer Spring's).
			// Build FQCNs without embedding relocatable literals to avoid shade rewriting.
			final String mnFqcn = new StringJoiner(".")
					.add("org").add("apache").add("kafka").add("common").add("MetricName")
					.toString();
			final String metricFqcn = new StringJoiner(".")
					.add("org").add("apache").add("kafka").add("common").add("Metric")
					.toString();
			ClassLoader appLoader = preferredAppLoader(appCl);
			Class<?> appMetricNameCls = Class.forName(mnFqcn, false, appLoader);
			Class<?> appMetricIface = Class.forName(metricFqcn, false, appLoader);
            try {
                CodeSource cs = appMetricNameCls.getProtectionDomain().getCodeSource();
                String src = (cs != null && cs.getLocation() != null) ? cs.getLocation().toString() : "unknown";
                logger.debug().msg("[METRICS-ADAPT] Resolved unshaded MetricName from {}", src);
            } catch (Throwable ignored) { }
            Constructor<?> mnCtor = null;
            try { mnCtor = appMetricNameCls.getConstructor(String.class, String.class, String.class, Map.class); } catch (Throwable ignored) {}

			int loggedKeys = 0;
            for (Map.Entry<?,?> e : shadedMap.entrySet()) {
                Object shadedName = e.getKey();
                Object shadedMetric = e.getValue();

                // Extract fields from shaded MetricName and construct unshaded MetricName
                Object appName = null;
                try {
                    String name = String.valueOf(shadedName.getClass().getMethod("name").invoke(shadedName));
                    String group = String.valueOf(shadedName.getClass().getMethod("group").invoke(shadedName));
                    String desc = String.valueOf(shadedName.getClass().getMethod("description").invoke(shadedName));
                    Object tagsObj = shadedName.getClass().getMethod("tags").invoke(shadedName);
                    Map<String,String> tags = new HashMap<>();
                    if (tagsObj instanceof Map) {
                    for (Map.Entry<?,?> te : ((Map<?,?>)tagsObj).entrySet()) {
                            if (te.getKey() != null) tags.put(String.valueOf(te.getKey()), (te.getValue() != null) ? String.valueOf(te.getValue()) : null);
                        }
                    }
                    if (mnCtor != null) {
                        appName = mnCtor.newInstance(name, group, desc, tags);
						if (loggedKeys < 3) {
							try {
								logger.debug().msg("[METRICS-ADAPT] Constructed MetricName key class={}", appName.getClass().getName());
							} catch (Throwable ignoredLog) { }
							loggedKeys++;
						}
                    }
                } catch (Throwable ignored) { }

                if (appName == null) {
                    // If we cannot build MetricName, skip this entry to avoid mixed classloader types
                    continue;
                }

                // Create a proxy that delegates Metric interface calls to the shaded Metric instance
                final Object appNameFinal = appName;
                final Object shadedMetricFinal = shadedMetric;
                Object appMetric = Proxy.newProxyInstance(appCl, new Class<?>[] { appMetricIface }, (proxy, method, args) -> {
                    String m = method.getName();
                    if ("metricName".equals(m) && (args == null || args.length == 0)) {
                        return appNameFinal;
                    }
                    if ("metricValue".equals(m) && (args == null || args.length == 0)) {
                        try {
                            Method mv = shadedMetricFinal.getClass().getMethod("metricValue");
                            return mv.invoke(shadedMetricFinal);
                        } catch (Throwable t) {
                            return null;
                        }
                    }
                    if ("toString".equals(m) && (args == null || args.length == 0)) {
                        return String.valueOf(shadedMetricFinal);
                    }
                    if ("hashCode".equals(m) && (args == null || args.length == 0)) {
                        return shadedMetricFinal.hashCode();
                    }
                    if ("equals".equals(m) && args != null && args.length == 1) {
                        return proxy == args[0];
                    }
                    // Default: no-op
                    return null;
                });

                out.put(appName, appMetric);
            }
        } catch (Throwable ignored) { }
        return out;
    }

    /**
     * Get partition information for a topic and adapt it to unshaded PartitionInfo types.
     * Uses the application's classloader to resolve unshaded types.
     *
     * @param shadedProducer The shaded KafkaProducer instance
     * @param topic The topic name
     * @param appCl The application's classloader for resolving unshaded types
     * @return List of unshaded PartitionInfo instances
     */
    @SuppressWarnings("unchecked")
    public static List<Object> partitionsFor(Object shadedProducer, String topic, ClassLoader appCl) {
        KafkaProducer<Object, Object> shadowProducer = castShadedProducer(shadedProducer);
        logger.debug().forTopic(topic).msg("[SHADOW-EXECUTION] executed method PartitionsFor(String) (topic={})", topic);
        List<Object> out = new ArrayList<>();
        try {
            List<?> shadedList = shadowProducer.partitionsFor(topic);
            if (shadedList == null) return out;

            Class<?> appNodeCls = Class.forName("org.apache.kafka.common.Node", false, appCl);
            Class<?> appPiCls = Class.forName("org.apache.kafka.common.PartitionInfo", false, appCl);
            Constructor<?> nodeCtor = null;
            try { nodeCtor = appNodeCls.getConstructor(int.class, String.class, int.class, String.class); } catch (Throwable ignored) { }
            if (nodeCtor == null) {
                try { nodeCtor = appNodeCls.getConstructor(int.class, String.class, int.class); } catch (Throwable ignored) { }
            }
            Constructor<?> piCtor = null;
            try { piCtor = appPiCls.getConstructor(String.class, int.class, appNodeCls, appNodeCls.arrayType(), appNodeCls.arrayType(), appNodeCls.arrayType()); } catch (Throwable ignored) {}

            for (Object shadedPi : shadedList) {
                try {
                    String tp = String.valueOf(shadedPi.getClass().getMethod("topic").invoke(shadedPi));
                    int part = (int) shadedPi.getClass().getMethod("partition").invoke(shadedPi);
                    Object shadedLeader = shadedPi.getClass().getMethod("leader").invoke(shadedPi);
                    Object shadedReplicas = shadedPi.getClass().getMethod("replicas").invoke(shadedPi);
                    Object shadedIsr = shadedPi.getClass().getMethod("inSyncReplicas").invoke(shadedPi);
                    Object shadedOffline = null;
                    try { shadedOffline = shadedPi.getClass().getMethod("offlineReplicas").invoke(shadedPi); } catch (Throwable ignored2) {}

                    final Constructor<?> nodeCtorFinal = nodeCtor;
                    final Class<?> appNodeClsFinal = appNodeCls;
                    Function<Object,Object> adaptNode = (obj) -> {
                        if (obj == null) return null;
                        try {
                            int id = (int) obj.getClass().getMethod("id").invoke(obj);
                            String host = String.valueOf(obj.getClass().getMethod("host").invoke(obj));
                            int port = (int) obj.getClass().getMethod("port").invoke(obj);
                            String rack = null;
                            try { Object r = obj.getClass().getMethod("rack").invoke(obj); rack = (r != null)? String.valueOf(r): null; } catch (Throwable ignored3) {}
                            if (nodeCtorFinal.getParameterCount() == 4) {
                                return nodeCtorFinal.newInstance(id, host, port, rack);
                            } else {
                                return nodeCtorFinal.newInstance(id, host, port);
                            }
                        } catch (Throwable t) { return null; }
                    };

                    Function<Object,Object[]> adaptNodeArray = (arr) -> {
                        try {
                            int len = Array.getLength(arr);
                            Object res = Array.newInstance(appNodeClsFinal, len);
                            for (int i = 0; i < len; i++) {
                                Object n = Array.get(arr, i);
                                Object an = adaptNode.apply(n);
                                Array.set(res, i, an);
                            }
                            return (Object[]) res;
                        } catch (Throwable t) { return (Object[]) Array.newInstance(appNodeClsFinal, 0); }
                    };

                    Object appLeader = adaptNode.apply(shadedLeader);
                    Object[] appReplicas = shadedReplicas != null ? adaptNodeArray.apply(shadedReplicas) : (Object[]) Array.newInstance(appNodeClsFinal, 0);
                    Object[] appIsr = shadedIsr != null ? adaptNodeArray.apply(shadedIsr) : (Object[]) Array.newInstance(appNodeClsFinal, 0);
                    Object[] appOffline = shadedOffline != null ? adaptNodeArray.apply(shadedOffline) : (Object[]) Array.newInstance(appNodeClsFinal, 0);

                    if (piCtor != null) {
                        Object appPi = piCtor.newInstance(tp, part, appLeader, appReplicas, appIsr, appOffline);
                        out.add(appPi);
                    }
                } catch (Throwable ignored) { }
            }
        } catch (Throwable ignored) { }
        return out;
    }

    /**
     * Cast an Object to a shaded KafkaProducer. This is safe because we control
     * the creation of shadow producers.
     *
     * @param p The object to cast (must be a shaded KafkaProducer)
     * @return The casted KafkaProducer instance
     */
    private static KafkaProducer<Object, Object> castShadedProducer(Object p) {
        return (KafkaProducer<Object, Object>) p;
    }

    /**
     * Derive the application's classloader from an object, falling back to
     * the thread context classloader if the object's classloader is null.
     *
     * @param any An object from the application's classloader, may be null
     * @return The application's classloader, or thread context classloader as fallback
     */
    private static ClassLoader deriveAppClassLoader(Object any) {
        ClassLoader cl = (any != null) ? any.getClass().getClassLoader() : null;
        if (cl != null) return cl;
        return Thread.currentThread().getContextClassLoader();
    }

	/**
	 * Select the most appropriate application classloader.
	 * Preference order:
	 * 1) Spring's DefaultKafkaProducerFactory classloader (if present on TCCL)
	 * 2) Thread context classloader
	 * 3) Provided fallback
	 * 4) This class's loader
	 *
	 * @param fallback Fallback classloader if TCCL is not available
	 * @return The selected classloader
	 */
	private static ClassLoader preferredAppLoader(ClassLoader fallback) {
		ClassLoader tccl = Thread.currentThread().getContextClassLoader();
		try {
			Class<?> df = Class.forName("org.springframework.kafka.core.DefaultKafkaProducerFactory", false, tccl);
			if (df != null && df.getClassLoader() != null) {
				return df.getClassLoader();
			}
		} catch (Throwable ignored) { }
		if (tccl != null) return tccl;
		if (fallback != null) return fallback;
		return ShadowProducerManager.class.getClassLoader();
	}

    /**
     * Convert an unshaded {@code ProducerRecord} to a shaded one. Copies topic,
     * partition, timestamp, key, value, and headers (key + bytes) 1:1.
     *
     * @param unshadedRecord The unshaded ProducerRecord to convert, may be null
     * @return A shaded ProducerRecord with the same data, or null if input is null
     * @throws Exception if reflection-based field access fails
     */
    private static ProducerRecord<Object, Object> adaptRecordToShaded(Object unshadedRecord) throws Exception {
        if (unshadedRecord == null) return null;
        Class<?> recCls = unshadedRecord.getClass();
        Method topicM = recCls.getMethod("topic");
        Method partitionM = safeMethod(recCls, "partition");
        Method timestampM = safeMethod(recCls, "timestamp");
        Method keyM = safeMethod(recCls, "key");
        Method valueM = safeMethod(recCls, "value");
        Method headersM = safeMethod(recCls, "headers");

        String topic = (String) topicM.invoke(unshadedRecord);
        Integer partition = partitionM != null ? (Integer) partitionM.invoke(unshadedRecord) : null;
        Long timestamp = timestampM != null ? (Long) timestampM.invoke(unshadedRecord) : null;
        Object key = keyM != null ? keyM.invoke(unshadedRecord) : null;
        Object value = valueM != null ? valueM.invoke(unshadedRecord) : null;

        Headers shadedHeaders = new RecordHeaders();
        if (headersM != null) {
            Object unHeaders = headersM.invoke(unshadedRecord);
            if (unHeaders != null) {
                // Iterate: headers.iterator() yields Header objects with key() and value()
                Method iterM = unHeaders.getClass().getMethod("iterator");
                Iterator<?> it = (Iterator<?>) iterM.invoke(unHeaders);
                while (it.hasNext()) {
                    Object h = it.next();
                    Method keyH = h.getClass().getMethod("key");
                    Method valH = h.getClass().getMethod("value");
                    String hk = (String) keyH.invoke(h);
                    byte[] hv = (byte[]) valH.invoke(h);
                    shadedHeaders.add(hk, hv);
                }
            }
        }

        if (partition == null && timestamp == null) {
            return new ProducerRecord<>(topic, (Integer) null, key, value, shadedHeaders);
        } else if (partition == null) {
            return new ProducerRecord<>(topic, (Integer) null, timestamp, key, value, shadedHeaders);
        } else if (timestamp == null) {
            return new ProducerRecord<>(topic, partition, key, value, shadedHeaders);
        } else {
            return new ProducerRecord<>(topic, partition, timestamp, key, value, shadedHeaders);
        }
    }

    /**
     * Safely get a method by name, returning null if not found instead of throwing.
     *
     * @param c The class to search for the method
     * @param name The method name
     * @return The Method if found, null otherwise
     */
    private static Method safeMethod(Class<?> c, String name) {
        try { return c.getMethod(name); } catch (Exception e) { return null; }
    }

    /**
     * Bridges shaded callback into the application's unshaded callback by mapping
     * shaded {@code RecordMetadata} to an unshaded instance reflectively.
     */
    private static class CallbackAdapter implements Callback {
        private final Object unshadedCallback;
        private final ClassLoader appCl;
        private final Class<?> unRecMetaCls; // org.apache.kafka.clients.producer.RecordMetadata from app CL
        private final Method ifaceMethod;    // Callback.onCompletion(RecordMetadata, Exception) loaded via app CL
        private final Method exactMethod;    // Exact method on concrete callback class if available

        CallbackAdapter(Object unshadedCallback) {
            this.unshadedCallback = unshadedCallback;
            this.appCl = deriveAppClassLoader(unshadedCallback);
            Class<?> tmpRecMeta = null;
            Method tmpIface = null;
            Method tmpExact = null;
            try {
                tmpRecMeta = Class.forName("org.apache.kafka.clients.producer.RecordMetadata", false, appCl);
                Class<?> cbIface = Class.forName("org.apache.kafka.clients.producer.Callback", false, appCl);
                tmpIface = cbIface.getMethod("onCompletion", tmpRecMeta, Exception.class);
            } catch (Throwable ignored) {
                // Interface path may be unavailable depending on classloader wiring; fall back to exact/compatible
            }
            if (unshadedCallback != null) {
                try {
                    if (tmpRecMeta == null) {
                        tmpRecMeta = Class.forName("org.apache.kafka.clients.producer.RecordMetadata", false, appCl);
                    }
                    tmpExact = unshadedCallback.getClass().getMethod("onCompletion", tmpRecMeta, Exception.class);
                    tmpExact.setAccessible(true);
                } catch (Throwable ignored) {
                    // Exact signature not available; we'll search compatible at call time
                }
            }
            this.unRecMetaCls = tmpRecMeta;
            this.ifaceMethod = tmpIface;
            this.exactMethod = tmpExact;
        }

        @Override
        public void onCompletion(RecordMetadata shaded, Exception exception) {
            // Log summary of shaded callback invocation
            try {
                String t = (shaded != null) ? shaded.topic() : "null";
                String p = (shaded != null) ? String.valueOf(shaded.partition()) : "null";
                String o = (shaded != null) ? String.valueOf(shaded.offset()) : "null";
                logger.debug("[ROUTED-CALLBACK] onCompletion(topic={}, partition={}, offset={}, hasException={})", t, p, o, String.valueOf(exception != null));
            } catch (Throwable ignoredLog) { }

            if (unshadedCallback == null) {
                logger.debug("[ROUTED-CALLBACK] onCompletion: unshadedCallback is null; returning early");
                return;
            }

            Object unMeta = null;
            try {
                // Build unshaded RecordMetadata instance if possible; allow null when not constructible
                Class<?> unTopicPartitionCls = Class.forName("org.apache.kafka.common.TopicPartition", false, appCl);
                Object unTp = (shaded != null)
                        ? unTopicPartitionCls.getConstructor(String.class, int.class).newInstance(shaded.topic(), shaded.partition())
                        : null;
                if (unRecMetaCls != null && shaded != null && unTp != null) {
                    unMeta = constructUnshadedRecordMetadata(unRecMetaCls, unTp, shaded);
                }

                // Strategy 1: Prefer interface signature in the app classloader (most reliable)
                if (ifaceMethod != null) {
                    try {
                        ifaceMethod.setAccessible(true);
                        ifaceMethod.invoke(unshadedCallback, unMeta, exception);
                        logger.debug("[ROUTED-CALLBACK] onCompletion: invoked interface signature");
                        return;
                    } catch (Throwable invokeErr) {
                        logger.debug("[ROUTED-CALLBACK] interface invocation failed: {}. Trying exact signature.", invokeErr.toString());
                    }
                }

                // Strategy 2: Try exact signature on the concrete callback class
                if (exactMethod != null) {
                    try {
                        exactMethod.setAccessible(true);
                        exactMethod.invoke(unshadedCallback, unMeta, exception);
                        logger.debug("[ROUTED-CALLBACK] onCompletion: invoked exact signature");
                        return;
                    } catch (Throwable invokeErr) {
                        logger.debug("[ROUTED-CALLBACK] exact invocation failed: {}. Falling back to compatible method.", invokeErr.toString());
                    }
                }

                // Strategy 3: Fallback - find any compatible method by name with second param assignable from Throwable/Exception
                Method chosen = null;
                for (Method m : unshadedCallback.getClass().getMethods()) {
                    if (!m.getName().equals("onCompletion")) continue;
                    if (m.getParameterCount() != 2) continue;
                    Class<?>[] pt = m.getParameterTypes();
                    if (!Throwable.class.isAssignableFrom(pt[1]) && !Exception.class.isAssignableFrom(pt[1])) continue;
                    chosen = m;
                    break;
                }
                if (chosen != null) {
                    try {
                        chosen.setAccessible(true);
                        chosen.invoke(unshadedCallback, unMeta, exception);
                        logger.debug("[ROUTED-CALLBACK] onCompletion: invoked compatible signature {}", chosen.toString());
                        return;
                    } catch (Throwable invokeErr) {
                        logger.error("[ERR-720] [ROUTED-CALLBACK] compatible invocation threw: {}", invokeErr.toString(), invokeErr);
                    }
                } else {
                    logger.debug("[ROUTED-CALLBACK] onCompletion: no compatible onCompletion method found; dropping");
                }
            } catch (Throwable outerErr) {
                // Never propagate into shaded producer thread: log and continue
                logger.error("[ERR-721] [ROUTED-CALLBACK] onCompletion handling failed: {}", outerErr.toString(), outerErr);
            }
        }
    }

    /**
     * Instantiate an unshaded {@code RecordMetadata} instance from its shaded
     * counterpart using known constructor shapes in Kafka 3.3.x.
     * Returns null when no compatible constructor is found.
     *
     * @param unRecMetaCls The unshaded RecordMetadata class
     * @param unTopicPartition The unshaded TopicPartition instance
     * @param shaded The shaded RecordMetadata to convert
     * @return An unshaded RecordMetadata instance, or null if no compatible constructor exists
     * @throws Exception if constructor invocation fails
     */
    private static Object constructUnshadedRecordMetadata(Class<?> unRecMetaCls, Object unTopicPartition, RecordMetadata shaded) throws Exception {
        // Try known constructor patterns for Kafka 3.3.x and later versions
        Constructor<?>[] ctors = unRecMetaCls.getConstructors();
        for (Constructor<?> ctor : ctors) {
            Class<?>[] p = ctor.getParameterTypes();
            try {
                boolean isTopicPartition = p.length == 6 
                    && p[0].getName().equals("org.apache.kafka.common.TopicPartition")
                    && p[1] == long.class 
                    && p[2] == long.class
                    && (p[3] == Integer.class || p[3] == int.class || p[3] == Long.class)
                    && p[4] == int.class 
                    && p[5] == int.class;
                if (isTopicPartition) {
                    int sk = shaded.serializedKeySize();
                    int sv = shaded.serializedValueSize();
                    // Constructor: RecordMetadata(TopicPartition, long offset, long timestamp, Integer leaderEpoch, int serializedKeySize, int serializedValueSize)
                    // Pass null for leaderEpoch to accept any epoch type (Integer, int, or Long)
                    return ctor.newInstance(unTopicPartition, shaded.offset(), shaded.timestamp(), null, sk, sv);
                }
            } catch (Throwable ignored) { }
        }
        // Fallback: try constructors by parameter count (for older Kafka versions or alternative signatures)
        for (Constructor<?> ctor : ctors) {
            if (ctor.getParameterCount() == 6) {
                try {
                    return ctor.newInstance(unTopicPartition, shaded.offset(), shaded.timestamp(), null, shaded.serializedKeySize(), shaded.serializedValueSize());
                } catch (Throwable ignored) { }
            }
            if (ctor.getParameterCount() == 5) {
                try {
                    // Try 5-parameter constructor: (TopicPartition, long offset, long timestamp, int keySize, int valueSize)
                    return ctor.newInstance(unTopicPartition, shaded.offset(), shaded.timestamp(), shaded.serializedKeySize(), shaded.serializedValueSize());
                } catch (Throwable ignored) { }
            }
        }
        // No compatible constructor found - return null (application callback will receive null metadata)
        return null;
    }

    /**
     * Wrap shaded {@code Future<RecordMetadata>} and expose the unshaded API to
     * the application by adapting the metadata on {@code get()}.
     */
    private static class FutureAdapter implements Future<Object> {
        private final Future<RecordMetadata> shadedFuture;
        private final ClassLoader appCl;

        FutureAdapter(Future<RecordMetadata> shadedFuture, ClassLoader appCl) {
            this.shadedFuture = shadedFuture;
            this.appCl = appCl;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return shadedFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return shadedFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return shadedFuture.isDone();
        }

        @Override
        public Object get() throws ExecutionException, InterruptedException {
            RecordMetadata shaded = shadedFuture.get();
            return adaptMetadata(shaded);
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
            RecordMetadata shaded = shadedFuture.get(timeout, unit);
            return adaptMetadata(shaded);
        }

        private Object adaptMetadata(RecordMetadata shaded) throws ExecutionException {
            if (shaded == null) return null;
            try {
                Class<?> unRecMetaCls = Class.forName("org.apache.kafka.clients.producer.RecordMetadata", false, appCl);
                Class<?> unTopicPartitionCls = Class.forName("org.apache.kafka.common.TopicPartition", false, appCl);
                Object unTp = unTopicPartitionCls.getConstructor(String.class, int.class).newInstance(shaded.topic(), shaded.partition());
                return constructUnshadedRecordMetadata(unRecMetaCls, unTp, shaded);
            } catch (Throwable e) {
                throw new ExecutionException(e);
            }
        }
    }
}


