package ai.superstream.agent;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import ai.superstream.core.SuperstreamManager;
import ai.superstream.model.MetadataMessage;
import ai.superstream.shadow.ShadowProducerManager;
import ai.superstream.util.SuperstreamLogger;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.implementation.bytecode.assign.Assigner;

/**
 * Advice layer for KafkaProducer.
 *
 * Responsibilities
 * - Constructor:
 *   - Extract and clone original Properties
 *   - Initialize per-producer registries (observed topics)
 *   - Mark readiness; a single global shadow is created when in SHADOW run mode
 * - Send:
 *   - Record the topic and route the call to the global shadow
 * - Read-only / transactional:
 *   - Operate on the global shadow (producer-wide)
 * - Hot-swap (one-shot, global):
 *   - After a learning period, compute an optimized configuration (based on observed topics + metadata)
 *   - Atomically swap the global shadow to a new instance with optimized configuration; flush and close the prior shadow
 * - Fallback:
 *   - When compatibility check fails globally (INIT_CONFIG mode), no shadow is created; routed methods delegate to the original producer
 */
public final class KafkaProducerShadowAdvice {
    public static final SuperstreamLogger logger = SuperstreamLogger.getLogger(KafkaProducerShadowAdvice.class);

    private KafkaProducerShadowAdvice() { }


    /**
     * Tracks readiness of each application producer for shadow routing.
     * Set to TRUE when the producer is ready for shadow routing, FALSE otherwise.
     * Maps application producer instance -> Boolean ready state.
     */
    public static final Map<Object, Boolean> READY = Collections.synchronizedMap(new WeakHashMap<>());

    /**
     * Stores a defensive copy of the original (unmodified) configuration properties 
     * used to construct each application producer.
     * Maps application producer instance -> original Properties.
     */
    public static final Map<Object, Properties> ORIGINAL_PROPS = Collections.synchronizedMap(new WeakHashMap<>());

    /**
     * Per-producer registry of topics that have been observed (via send calls).
     * Used to optimize configuration swaps and reporting.
     * Maps application producer instance -> set of topic names.
     */
    public static final Map<Object, Set<String>> OBSERVED_TOPICS = Collections.synchronizedMap(new WeakHashMap<>());

	/**
	 * Per-producer global shadow producer registry (single shadow per app producer instance).
	 * Maps application producer instance -> ShadowProducer.
	 */
	public static final Map<Object, ShadowProducer> SHADOW_PRODUCERS = Collections.synchronizedMap(new WeakHashMap<>());

    /**
     * Enum to represent the execution mode for a producer:
     * SHADOW - shadow routing is enabled; ON_INIT_CONFIG - only routing on original config.
     */
    public enum RunMode { SHADOW, INIT_CONFIG }

    /**
     * Stores the active RunMode for each producer instance.
     * Maps application producer instance -> RunMode.
     */
    public static final Map<Object, RunMode> RUN_MODE = Collections.synchronizedMap(new WeakHashMap<>());

    /**
     * Internal: Tracks whether the one-shot global shadow swap has been performed for a producer.
     * Prevents repeated swaps after the first successful optimized configuration application.
     * Maps application producer instance -> Boolean swap done.
     */
    private static final Map<Object, Boolean> SHADOW_SWAP_DONE = Collections.synchronizedMap(new WeakHashMap<>());

    /**
     * Background scheduler for deferring and running the one-shot global shadow swap in a separate thread pool.
     * Used to avoid blocking main producer logic during optimization.
     */
    private static final ScheduledExecutorService SHADOW_SWAP_SCHEDULER = Executors.newScheduledThreadPool(1, r -> {
        Thread t = new Thread(r, "superstream-one-shot-swap");
        t.setDaemon(true);
        return t;
    });

  public static void setRunModeForInstance(Object instance, RunMode mode) {
      if (instance == null || mode == null) return;
      RUN_MODE.put(instance, mode);
  }

    public static final class ShadowProducer {
        private final Object shadowProducer;
        private final Properties appliedProperties;
        private final String id;
        private final String configFingerprint;
        private final String internalUuid;
        private final AtomicInteger activeSends = new AtomicInteger(0);

        ShadowProducer(Object shadowProducer, Properties appliedProperties, String id, String configFingerprint) {
            this.shadowProducer = shadowProducer;
            this.appliedProperties = cloneProperties(appliedProperties);
            this.id = id;
            this.configFingerprint = configFingerprint;
            this.internalUuid = UUID.randomUUID().toString();
        }

        public Object getShadowProducer() {
            return shadowProducer;
        }

        public Properties getAppliedProperties() {
            Properties copy = new Properties();
            copy.putAll(appliedProperties);
            return copy;
        }

        public String getId() {
            return id;
        }

        public String getConfigFingerprint() {
            return configFingerprint;
        }

        public String getInternalUuid() {
            return internalUuid;
        }

        public void close() {
            ShadowProducerManager.close(shadowProducer);
        }

        public void beginSend() {
            activeSends.incrementAndGet();
        }

        public void endSend() {
            int v = activeSends.decrementAndGet();
            if (v < 0) {
                activeSends.compareAndSet(v, 0);
            }
        }

        public int getActiveSends() {
            return activeSends.get();
        }
    }

    public static class SendAdvice {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static Object onEnter(@Advice.This Object instance, @Advice.AllArguments Object[] args, @Advice.Local("result") Future<?> resultHolder) {
            try {
                if (isInitConfigMode(instance)) return null; // not in shadow mode

                if (!isShadowReady(instance)) {
                    throw new IllegalStateException("Shadow not ready for send()");
                }

                // record the topic
                Object record = (args != null && args.length > 0) ? args[0] : null;
                String topic = extractTopic(record);
                if (topic == null) {
                    throw new IllegalStateException("Unable to determine topic for ProducerRecord");
                }
                recordTopic(instance, topic);

				ShadowProducer shadow = getGlobalShadowProducer(instance);
                if (shadow == null) {
                    throw new IllegalStateException("No ShadowProducer available");
                }
                
                // Execute the shadow send with appropriate arguments
                String cid = getOriginalClientId(instance);
                if (args != null && args.length == 1) {
                    logger.debug().withClientId(cid).forTopic(topic).msg("[SHADOW-ROUTING-STARTED] Send(ProducerRecord)");
                } else if (args != null && args.length == 2) {
                    logger.debug().withClientId(cid).forTopic(topic).msg("[SHADOW-ROUTING-STARTED] Send(ProducerRecord, Callback)");
                } else {
                    logger.debug().withClientId(cid).forTopic(topic).msg("[SHADOW-ROUTING-STARTED] Send(…unexpected signature…)");
                }

                Future<?> res;
                shadow.beginSend();
                try {
                    if (args.length == 1) { // send(ProducerRecord)
                        res = ShadowProducerManager.send(shadow.getShadowProducer(), args[0]);
                    } else if (args.length == 2) { // send(ProducerRecord, Callback)
                        res = ShadowProducerManager.send(shadow.getShadowProducer(), args[0], args[1]);
                    } else {
                        return null; // unexpected; do not intercept
                    }
                } finally {
                    shadow.endSend();
                }
                resultHolder = res;
                return Boolean.TRUE;
            } catch (Throwable t) {
                String cid = getOriginalClientId(instance);
                String topic = extractTopic(args != null && args.length > 0 ? args[0] : null);
                logger.error().withClientId(cid).forTopic(topic).msg("[ERR-502] Shadow send failed: {}", t.getMessage(), t);
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }

        /**
         * Called after the original send() method (or after it was skipped).
         * When shadow routing was active (skipToken == Boolean.TRUE), replaces the original
         * method's return value with the shadow producer's Future that was captured in onEnter.
         * This ensures the application receives the Future from the shadow producer instead of
         * the original producer when shadow routing is enabled.
         */
        @Advice.OnMethodExit
        public static void onExit(@Advice.Enter Object skipToken, @Advice.Local("result") Future<?> resultHolder, @Advice.Return(readOnly = false, typing = Assigner.Typing.DYNAMIC) Future<?> returned) {
            if (skipToken == Boolean.TRUE) {
                returned = resultHolder;
            }
        }
    }

    /**
     * Advice for intercepting KafkaProducer.flush() calls.
     * In shadow mode, we flush the shadow producer and skip the original flush,
     * since the original producer never receives messages (all sends are routed to shadow).
     * In shadow mode, throws exceptions instead of falling back to original flush, for consistency with send().
     */
    public static class FlushAdvice {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static Object onEnter(@Advice.This Object instance) {
            String cid = getOriginalClientId(instance);

            try {
                if (isInitConfigMode(instance)) {
                    return null; // not in shadow mode; let original flush execute
                }
                logger.debug().withClientId(cid).msg("[SHADOW-ROUTING-STARTED] Flush()");
               
                if (!isShadowReady(instance)) {
                    throw new IllegalStateException("Shadow not ready for flush()");
                }
                
				ShadowProducer shadow = getGlobalShadowProducer(instance);
				if (shadow == null) {
					throw new IllegalStateException("No ShadowProducer available for flush()");
				}
				
				ShadowProducerManager.flush(shadow.getShadowProducer());
				return Boolean.TRUE;
            } catch (Throwable t) {
                logger.error().withClientId(cid).msg("[ERR-510] Shadow flush failed: {}", t.getMessage(), t);
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }
    }

    public static class CloseAdvice {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static Object onEnter(@Advice.This Object instance, @Advice.AllArguments Object[] args) {
            try {
                if (isInitConfigMode(instance)) return null;
                // Distinguish overloads by args
                if (args == null || args.length == 0) {
                    logger.debug().withClientId(getOriginalClientId(instance))
                            .msg("[SHADOW-ROUTING-STARTED] Close()");
                } else if (args.length == 1) {
                    logger.debug().withClientId(getOriginalClientId(instance))
                            .msg("[SHADOW-ROUTING-STARTED] Close(Duration)");
                } else if (args.length == 2) {
                    logger.debug().withClientId(getOriginalClientId(instance))
                            .msg("[SHADOW-ROUTING-STARTED] Close(long, TimeUnit)");
                } else {
                    logger.debug().withClientId(getOriginalClientId(instance))
                            .msg("[SHADOW-ROUTING-STARTED] Close(…unexpected signature…)");
                }
                String cid = getOriginalClientId(instance);
                if (!isShadowReady(instance)) {
                    logger.debug().withClientId(cid).msg("[SHADOW-ROUTING-STARTED] Close(...) - shadow not ready; allowing original close()");
                    return null; 
                }

                closeShadowProducerForInstance(instance);
                if (!trySetBooleanField(instance, "__superstreamShadowReady", false)) {
                    READY.put(instance, Boolean.FALSE);
                }
                // Let original close execute as well to prevent resource leaks
                return null;
            } catch (Throwable t) {
                String cid = getOriginalClientId(instance);
                // On any error, do not block original close
                logger.warn().withClientId(cid).msg("Shadow close failed: {}. Allowing original close()", t.getMessage(), t);
                return null;
            }
        }
    }

    public static class InitTransactionsAdvice {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static Object onEnter(@Advice.This Object instance) {
            try {
                if (isInitConfigMode(instance)) return null;

                logger.debug().withClientId(getOriginalClientId(instance)).msg("[SHADOW-ROUTING-STARTED] InitTransactions()");
                if (!isShadowReady(instance)) {
                    throw new IllegalStateException("Shadow not ready for initTransactions()");
                }
				ShadowProducer shadow = getGlobalShadowProducer(instance);
				if (shadow == null) {
					throw new IllegalStateException("No shadow available for initTransactions()");
				}

				ShadowProducerManager.initTransactions(shadow.getShadowProducer());
                return Boolean.TRUE;
            } catch (Throwable t) {
                logger.error().withClientId(getOriginalClientId(instance)).msg("[ERR-503] Shadow initTransactions failed: {}. Not executing original", t.getMessage(), t);
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }
    }

    public static class BeginTransactionAdvice {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static Object onEnter(@Advice.This Object instance) {
            try {
                if (isInitConfigMode(instance)) return null;

                logger.debug().withClientId(getOriginalClientId(instance)).msg("[SHADOW-ROUTING-STARTED] BeginTransaction()");
                if (!isShadowReady(instance)) {
                    throw new IllegalStateException("Shadow not ready for beginTransaction()");
                }
				ShadowProducer shadow = getGlobalShadowProducer(instance);
				if (shadow == null) {
					throw new IllegalStateException("No shadow available for beginTransaction()");
				}

				ShadowProducerManager.beginTransaction(shadow.getShadowProducer());
                return Boolean.TRUE;
            } catch (Throwable t) {
                String cid = getOriginalClientId(instance);
                logger.error().withClientId(cid).msg("[ERR-504] Shadow beginTransaction failed: {}. Not executing original", t.getMessage(), t);
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }
    }

    public static class CommitTransactionAdvice {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static Object onEnter(@Advice.This Object instance) {
            try {
                if (isInitConfigMode(instance)) return null;

                logger.debug().withClientId(getOriginalClientId(instance)).msg("[SHADOW-ROUTING-STARTED] CommitTransaction()");
                if (!isShadowReady(instance)) {
                    throw new IllegalStateException("Shadow not ready for commitTransaction()");
                }

				ShadowProducer shadow = getGlobalShadowProducer(instance);
				if (shadow == null) {
					throw new IllegalStateException("No shadow available for commitTransaction()");
				}

				ShadowProducerManager.commitTransaction(shadow.getShadowProducer());
                return Boolean.TRUE;
            } catch (Throwable t) {
                String cid = getOriginalClientId(instance);
                logger.error().withClientId(cid).msg("[ERR-505] Shadow commitTransaction failed: {}. Not executing original", t.getMessage(), t);
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }
    }

    public static class AbortTransactionAdvice {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static Object onEnter(@Advice.This Object instance) {
            try {
                if (isInitConfigMode(instance)) return null;

                logger.debug().withClientId(getOriginalClientId(instance)).msg("[SHADOW-ROUTING-STARTED] AbortTransaction()");
                if (!isShadowReady(instance)) {
                    throw new IllegalStateException("Shadow not ready for abortTransaction()");
                }
				ShadowProducer shadow = getGlobalShadowProducer(instance);
				if (shadow == null) {
					throw new IllegalStateException("No shadow available for abortTransaction()");
				}

				ShadowProducerManager.abortTransaction(shadow.getShadowProducer());
                return Boolean.TRUE;
            } catch (Throwable t) {
                String cid = getOriginalClientId(instance);
                logger.error().withClientId(cid).msg("[ERR-506] Shadow abortTransaction failed: {}. Not executing original", t.getMessage(), t);
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }
    }

    public static class SendOffsetsToTransactionAdvice {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static Object onEnter(@Advice.This Object instance, @Advice.AllArguments Object[] args) {
            try {
                if (isInitConfigMode(instance)) return null;

                if (args != null && args.length == 2) {
                    if (args[1] instanceof String) {
                        logger.debug().withClientId(getOriginalClientId(instance))
                                .msg("[SHADOW-ROUTING-STARTED] SendOffsetsToTransaction(Map, String)");
                    } else {
                        logger.debug().withClientId(getOriginalClientId(instance))
                                .msg("[SHADOW-ROUTING-STARTED] SendOffsetsToTransaction(Map, ConsumerGroupMetadata)");
                    }
                } else {
                    logger.debug().withClientId(getOriginalClientId(instance))
                            .msg("[SHADOW-ROUTING-STARTED] SendOffsetsToTransaction(…unexpected signature…)");
                }
                if (!isShadowReady(instance)) {
                    throw new IllegalStateException("Shadow not ready for sendOffsetsToTransaction()");
                }
                if (args == null || args.length != 2) return null;

				ShadowProducer shadow = getGlobalShadowProducer(instance);
				if (shadow == null) {
					throw new IllegalStateException("No shadow available for sendOffsetsToTransaction()");
				}

				ShadowProducerManager.sendOffsetsToTransaction(shadow.getShadowProducer(), args[0], args[1]);
                return Boolean.TRUE;
            } catch (Throwable t) {
                String cid = getOriginalClientId(instance);
                logger.error().withClientId(cid).msg("[ERR-507] Shadow sendOffsetsToTransaction failed: {}. Not executing original", t.getMessage(), t);
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }
    }

    public static class MetricsAdvice {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static Object onEnter(@Advice.This Object instance, @Advice.Local("result") Map<Object,Object> resultHolder) {
            try {
                if (isInitConfigMode(instance)) return null;

                logger.debug().withClientId(getOriginalClientId(instance)) .msg("[SHADOW-ROUTING-STARTED] Metrics()");
                if (!isShadowReady(instance)) {
                    throw new IllegalStateException("Shadow not ready for metrics()");
                }

				ShadowProducer shadow = getGlobalShadowProducer(instance);
                if (shadow == null) {
					throw new IllegalStateException("No shadow available for metrics()");
                }

                Map<Object,Object> adapted = ShadowProducerManager.metrics(shadow.getShadowProducer(), instance.getClass().getClassLoader());
                resultHolder = adapted;
                return Boolean.TRUE;
            } catch (Throwable t) {
                logger.error().withClientId(getOriginalClientId(instance)).msg("[ERR-508] Shadow metrics failed: {}. Not executing original", t.getMessage(), t);
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }

        /**
         * Called after the original metrics() method (or after it was skipped).
         * When shadow routing was active (skipToken == Boolean.TRUE), replaces the original
         * method's return value with the shadow producer's metrics Map that was captured in onEnter.
         * This ensures the application receives metrics from the shadow producer instead of
         * the original producer when shadow routing is enabled.
         */
        @Advice.OnMethodExit
        public static void onExit(@Advice.Enter Object skipToken, @Advice.Local("result") Map<Object,Object> resultHolder, @Advice.Return(readOnly=false, typing = Assigner.Typing.DYNAMIC) Object returned) {
            if (skipToken == Boolean.TRUE) {
                returned = resultHolder;
            }
        }
    }

    public static class PartitionsForAdvice {
        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static Object onEnter(@Advice.This Object instance, @Advice.AllArguments Object[] args, @Advice.Local("result") List<Object> resultHolder) {
            try {
                if (isInitConfigMode(instance)) return null;

                String topic = (args != null && args.length > 0 && args[0] != null) ? String.valueOf(args[0]) : null;

                logger.debug().withClientId(getOriginalClientId(instance))
                        .forTopic(topic)
                        .msg("[SHADOW-ROUTING-STARTED] PartitionsFor(String) (topic={})", topic);
                if (!isShadowReady(instance)) {
                    throw new IllegalStateException("Shadow not ready for partitionsFor()");
                }

				ShadowProducer shadow = getGlobalShadowProducer(instance);
                if (shadow == null) {
					throw new IllegalStateException("No shadow available for partitionsFor()");
                }

                List<Object> adapted = ShadowProducerManager.partitionsFor(shadow.getShadowProducer(), topic, instance.getClass().getClassLoader());
                resultHolder = adapted;
                return Boolean.TRUE;
            } catch (Throwable t) {
                logger.error().withClientId(getOriginalClientId(instance)).msg("[ERR-509] Shadow partitionsFor failed: {}. Not executing original", t.getMessage(), t);
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }

        /**
         * Called after the original partitionsFor() method (or after it was skipped).
         * When shadow routing was active (skipToken == Boolean.TRUE), replaces the original
         * method's return value with the shadow producer's partition list that was captured in onEnter.
         * This ensures the application receives partition information from the shadow producer instead of
         * the original producer when shadow routing is enabled.
         */
        @Advice.OnMethodExit
        public static void onExit(@Advice.Enter Object skipToken, @Advice.Local("result") List<Object> resultHolder, @Advice.Return(readOnly=false, typing = Assigner.Typing.DYNAMIC) Object returned) {
            if (skipToken == Boolean.TRUE) {
                returned = resultHolder;
            }
        }
    }


    public static boolean isShadowReady(Object instance) throws Exception {
        Object v = tryGetField(instance, "__superstreamShadowReady");
        if (v instanceof Boolean) return (Boolean) v;
        Boolean r = READY.get(instance);
        return Boolean.TRUE.equals(r);
    }

    public static RunMode getRunMode(Object instance) {
        RunMode m = RUN_MODE.get(instance);
        return m != null ? m : RunMode.INIT_CONFIG;
    }

    public static boolean isInitConfigMode(Object instance) {
        return getRunMode(instance) == RunMode.INIT_CONFIG;
    }

    /**
     * Returns the active shadow producer object associated with the given application producer instance.
     *
     * Resolution order:
     * 1) Try the injected instance field "__superstreamShadow" (preferred, fastest path)
     * 2) Fall back to the per-instance global shadow wrapper (and return its underlying producer)
     *
     * May return null when no shadow has been created for the instance yet.
     */
    public static Object getShadow(Object instance) throws Exception {
        Object f = tryGetField(instance, "__superstreamShadow");
        if (f != null) {
            return f;
        }
		ShadowProducer global = getGlobalShadowProducer(instance);
		if (global != null) return global.getShadowProducer();
        return null;
    }

	public static ShadowProducer getGlobalShadowProducer(Object instance) {
		return SHADOW_PRODUCERS.get(instance);
	}

    public static void closeShadowProducerForInstance(Object instance) {
		ShadowProducer shadow = SHADOW_PRODUCERS.remove(instance);
		if (shadow != null) {
			shadow.close();
		}
        OBSERVED_TOPICS.remove(instance);
        ORIGINAL_PROPS.remove(instance);
    }

    public static void recordTopic(Object instance, String topic) {
        if (topic == null) {
            return;
        }
        Set<String> topics = OBSERVED_TOPICS.get(instance);
        if (topics == null) {
            topics = Collections.newSetFromMap(new ConcurrentHashMap<>());
            OBSERVED_TOPICS.put(instance, topics);
        }
        topics.add(topic);
    }

    public static List<String> getObservedTopics(Object instance) {
        Set<String> topics = OBSERVED_TOPICS.get(instance);
        if (topics == null || topics.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(topics);
    }

    private static Properties cloneProperties(Properties props) {
        Properties copy = new Properties();
        if (props != null) {
            copy.putAll(props);
        }
        return copy;
    }

	public static String getTopicConfigFingerprint(Object instance, String topic) {
		ShadowProducer shadow = getGlobalShadowProducer(instance);
        return shadow != null ? shadow.getConfigFingerprint() : null;
    }

    private static String generateFingerprint(Properties props) {
        if (props == null) {
            return "{}";
        }
        TreeMap<String, String> sorted = new TreeMap<>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            if (entry.getKey() == null) continue;
            sorted.put(String.valueOf(entry.getKey()), entry.getValue() == null ? "null" : String.valueOf(entry.getValue()));
        }
        return sorted.toString();
    }

    public static String extractTopic(Object record) {
        if (record == null) {
            return null;
        }
        try {
            Method topicMethod = record.getClass().getMethod("topic");
            Object topic = topicMethod.invoke(record);
            return topic != null ? String.valueOf(topic) : null;
        } catch (Exception e) {
            logger.debug("[TOPIC-SHADOW] Unable to extract topic from ProducerRecord: {}", e.getMessage());
            return null;
        }
    }

    public static void runModeInit(Object instance, Properties props) {
        RunMode mode = RUN_MODE.get(instance);
        if (mode == null) {
            mode = RunMode.INIT_CONFIG;
            RUN_MODE.put(instance, mode);
        }
        if (mode == RunMode.INIT_CONFIG) {
            boolean alreadyOptimized = false;
            try {
                if (props != null) {
                    String f = props.getProperty("superstream.optimizedOnInit");
                    alreadyOptimized = "true".equalsIgnoreCase(f);
                }
            } catch (Throwable t) {
                logger.debug().msg("[ON-INIT-CONFIG] Failed to read optimizedOnInit flag: {}", t.getMessage(), t);
            }
            if (!alreadyOptimized) {
                applyOnInitConfigOptimizations(props);
            }
            if (!trySetBooleanField(instance, "__superstreamShadowReady", true)) {
                READY.put(instance, Boolean.TRUE);
            }
            return;
        }
        // Shadow mode for this instance
        Properties cloned = cloneProperties(props);
		ORIGINAL_PROPS.put(instance, cloned);
		OBSERVED_TOPICS.put(instance, Collections.newSetFromMap(new ConcurrentHashMap<>()));
        // Create a single global shadow producer immediately
        ensureGlobalShadow(instance);
        if (!trySetBooleanField(instance, "__superstreamShadowReady", true)) {
            READY.put(instance, Boolean.TRUE);
        }
        logger.debug().withClientId(getOriginalClientId(instance)).msg("[RUN-MODE] Shadow enabled; global shadow initialized");

        // Schedule one-shot swap after learning period
        try {
            long delaySeconds = computeLearningDelaySeconds(ORIGINAL_PROPS.get(instance));
            logger.debug().withClientId(getOriginalClientId(instance))
                    .msg("[ONE-SHOT-SWAP] Scheduling one-shot shadow swap after {} seconds learning period", delaySeconds);
            SHADOW_SWAP_SCHEDULER.schedule(() -> {
                try {
                    performOneShotSwap(instance);
                } catch (Throwable t) {
                    logger.error().withClientId(getOriginalClientId(instance)).msg("[ERR-511] [ONE-SHOT-SWAP] execution failed: {}", t.getMessage(), t);
                }
            }, delaySeconds, TimeUnit.SECONDS);
        } catch (Throwable t) {
            logger.debug().withClientId(getOriginalClientId(instance)).msg("[ONE-SHOT-SWAP] scheduling failed: {}", t.getMessage(), t);
        }
    }

    private static long computeLearningDelaySeconds(Properties originalProps) {
        // Default: 10 hours in minutes -> seconds
        long defaultMinutes = 10L * 60L;
        try {
            if (originalProps == null) {
                return defaultMinutes * 60L;
            }
            String bootstrap = originalProps.getProperty("bootstrap.servers");
            if (bootstrap == null || bootstrap.isEmpty()) {
                return defaultMinutes * 60L;
            }
            AbstractMap.SimpleEntry<MetadataMessage, String> res =
                    SuperstreamManager.getInstance().getOrFetchMetadataMessage(bootstrap, originalProps);
            MetadataMessage mm = (res != null) ? res.getKey() : null;
            if (mm == null) {
                return defaultMinutes * 60L;
            }
            Long mins = mm.getAgentLearnPeriodMins();
            if (mins == null) {
                return defaultMinutes * 60L;
            }
            if (mins <= 0) {
                return defaultMinutes * 60L;
            }
            return mins * 60L;
        } catch (Throwable t) {
            logger.debug().msg("[LEARN-PERIOD] Using default delay; failed to compute from metadata: {}", t.getMessage(), t);
            return defaultMinutes * 60L;
        }
    }

    private static void applyOnInitConfigOptimizations(Properties props) {
        if (props == null) {
            return;
        }
        try {
            String bootstrap = props.getProperty("bootstrap.servers");
            if (bootstrap == null || bootstrap.trim().isEmpty()) {
                logger.warn().msg("[ON-INIT-CONFIG] bootstrap.servers missing; cannot apply init-time optimization");
                return;
            }
            String clientId = props.getProperty("client.id", "");
            boolean optimized = ai.superstream.core.SuperstreamManager.getInstance()
                    .optimizeProducer(bootstrap, clientId, props);
            if (optimized) {
                logger.info().withClientId(clientId).msg("[ON-INIT-CONFIG] Applied init-time config optimizations via SuperstreamManager");
            } else {
                logger.debug().withClientId(clientId).msg("[ON-INIT-CONFIG] SuperstreamManager.optimizeProducer returned false; no changes applied");
            }
        } catch (Throwable t) {
            logger.error().msg("[ERR-512] [ON-INIT-CONFIG] Failed to apply init-time optimizations: {}", t.getMessage(), t);
        }
    }
    
    // Ensure a single global shadow exists for this producer
    public static ShadowProducer ensureGlobalShadow(Object instance) {
		ShadowProducer existing = SHADOW_PRODUCERS.get(instance);
        if (existing != null) {
            return existing;
        }
		synchronized (SHADOW_PRODUCERS) {
			existing = SHADOW_PRODUCERS.get(instance);
            if (existing != null) return existing;

            Properties original = ORIGINAL_PROPS.get(instance);
            if (original == null) {
                throw new IllegalStateException("Original properties not registered for producer " + instance);
            }
            Properties inherited = cloneProperties(original);
            Object shadowObj = ShadowProducerManager.createShadowProducer(inherited);
            String id = ShadowProducerManager.getShadowId(shadowObj);
            ShadowProducer created = new ShadowProducer(shadowObj, inherited, id, generateFingerprint(inherited));
			SHADOW_PRODUCERS.put(instance, created);
            // Expose to metrics collector (field injection preferred, but SHADOW_PRODUCERS serves as fallback)
            trySetField(instance, "__superstreamShadow", shadowObj);
            logger.info().withClientId(getOriginalClientId(instance))
                    .msg("[GLOBAL-SHADOW] initialized (inherited app configuration)");
            return created;
        }
    }

    private static boolean isLatencySensitive() {
        try {
            String s = System.getenv("SUPERSTREAM_LATENCY_SENSITIVE");
            return s != null && ("true".equalsIgnoreCase(s) || "1".equals(s));
        } catch (Throwable t) {
            logger.debug().msg("[ENV] SUPERSTREAM_LATENCY_SENSITIVE read failed: {}", t.getMessage(), t);
            return false;
        }
    }

    /**
     * Performs a one-time atomic swap of the shadow producer with an optimized configuration.
     * This method is scheduled to run once after a learning period (typically 10 hours) to apply
     * topic-specific optimizations based on observed producer behavior.
     * 
     * Process:
     * 1. Checks if swap was already performed (idempotent)
     * 2. Fetches metadata from SuperstreamManager containing topic-specific optimizations
     * 3. Builds optimized configuration using the most impactful observed topic
     * 4. Falls back to default optimizations if no topic-specific config is available
     * 5. Atomically creates new shadow producer with optimized config and swaps it in
     * 6. Gracefully retires the previous shadow by:
     *    - Waiting for in-flight sends to complete (up to 2 seconds)
     *    - Flushing any remaining buffered messages
     *    - Closing the old shadow producer
     * 7. Updates metrics reporter with the new optimized configuration
     * 
     * The swap is atomic and thread-safe - all new send() calls immediately use the optimized
     * shadow producer, while in-flight operations on the old shadow complete gracefully.
     * 
     * @param instance The application KafkaProducer instance to swap the shadow for
     */
    private static void performOneShotSwap(Object instance) {
        if (Boolean.TRUE.equals(SHADOW_SWAP_DONE.get(instance))) {
            return;
        }
        final String clientId = getOriginalClientId(instance);

        try {
            logger.info().withClientId(clientId).msg("[ONE-SHOT-SWAP] start");
            Properties originalProps = ORIGINAL_PROPS.get(instance);
            if (originalProps == null) {
                logger.warn().withClientId(clientId).msg("[GLOBAL-SHADOW] One-shot swap aborted: original properties missing");
                return;
            }

            String bootstrap = originalProps.getProperty("bootstrap.servers");
            if (bootstrap == null || bootstrap.isEmpty()) {
                logger.warn().withClientId(clientId).msg("[GLOBAL-SHADOW] One-shot swap aborted: bootstrap.servers missing");
                return;
            }

            // get the metadata message
            AbstractMap.SimpleEntry<MetadataMessage, String> res =
                    SuperstreamManager.getInstance().getOrFetchMetadataMessage(bootstrap, originalProps);
            MetadataMessage mm = res.getKey();

            // find topic specific optimizations
            List<String> observedTopics = getObservedTopics(instance);
		SwapConfigManager.SwapResult swapCfg = SwapConfigManager.buildSwapOptimizedConfig(
					(getGlobalShadowProducer(instance) != null ? getGlobalShadowProducer(instance).getAppliedProperties() : originalProps),
                    mm,
                    observedTopics,
                    isLatencySensitive());
            if (swapCfg == null) {
                logger.info().withClientId(clientId)
                        .msg("[GLOBAL-SHADOW] No topic-specific optimized configuration found; applying default optimization");
                Properties baseForDefaults = (getGlobalShadowProducer(instance) != null
                        ? getGlobalShadowProducer(instance).getAppliedProperties()
                        : originalProps);
                swapCfg = SwapConfigManager.buildDefaultSwapOptimizedConfig(baseForDefaults, isLatencySensitive());
            }

			// Create replacement and atomically swap
			ShadowProducer previous;
			ShadowProducer replacement;
			synchronized (SHADOW_PRODUCERS) {
				previous = SHADOW_PRODUCERS.get(instance);
                Object shadowObj = ShadowProducerManager.createShadowProducer(swapCfg.appliedProperties);
                String id = ShadowProducerManager.getShadowId(shadowObj);
                replacement = new ShadowProducer(shadowObj, swapCfg.appliedProperties, id, swapCfg.fingerprint);
				SHADOW_PRODUCERS.put(instance, replacement);
                // Update metrics-visible shadow pointer (field injection preferred, but SHADOW_PRODUCERS serves as fallback)
                trySetField(instance, "__superstreamShadow", shadowObj);
            }

            // Gracefully retire previous shadow
            if (previous != null) {
                final long deadline = System.currentTimeMillis() + 2000L;
                while (previous.getActiveSends() > 0 && System.currentTimeMillis() < deadline) {
                    try {
                        Thread.sleep(5L);
                    } catch (InterruptedException ie) {
                        logger.warn().withClientId(clientId).msg("[GLOBAL-SHADOW] Sleep interrupted while waiting in-flight sends to finish");
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                try {
                    ShadowProducerManager.flush(previous.getShadowProducer());
                } catch (Throwable flushErr) {
                    logger.warn().withClientId(clientId).msg("[GLOBAL-SHADOW] Flush of previous shadow failed: {}", flushErr.getMessage(), flushErr);
                }
                try {
                    previous.close();
                } catch (Throwable closeErr) {
                    logger.warn().withClientId(clientId).msg("[GLOBAL-SHADOW] Close of previous shadow failed: {}", closeErr.getMessage(), closeErr);
                }
            }

            // Mark swap done
            SHADOW_SWAP_DONE.put(instance, Boolean.TRUE);
            String prevId = (previous != null) ? previous.getInternalUuid() : "none";
            String newId = replacement.getInternalUuid();
            logger.info().withClientId(clientId).msg("[SHADOW-SWAP-DONE] prevShadowId={}, newShadowId={}, fingerprint={}", prevId, newId, replacement.getConfigFingerprint());

            // Update reporter with optimized configuration for this app producer instance after the one-shot swap
            try {
                ai.superstream.agent.KafkaProducerInterceptor.updateReporterOptimizedConfigFor(
                        instance,
                        ORIGINAL_PROPS.get(instance),
                        swapCfg.appliedProperties);
            } catch (Throwable reportErr) {
                logger.warn().withClientId(clientId).msg("[GLOBAL-SHADOW] Failed to update reporter with optimized configuration: {}", reportErr.getMessage(), reportErr);
            }
        } catch (Throwable t) {
            logger.error().withClientId(clientId).msg("[ERR-501] One-shot swap failed: {}", t.getMessage(), t);
        }

        logger.info().withClientId(clientId).msg("[GLOBAL-SHADOW] one-shot optimized configuration applied");
    }

    public static Object tryGetField(Object instance, String name) {
        try {
            Field f = instance.getClass().getDeclaredField(name);
            f.setAccessible(true);
            return f.get(instance);
        } catch (Throwable t) {
            if (!"__superstreamShadow".equals(name) && !"__superstreamShadowReady".equals(name)) {
                logger.debug().msg("[REFLECT] tryGetField failed for {}: {}", name, t.getMessage(), t);
            }
            return null;
        }
    }

    public static boolean trySetField(Object instance, String name, Object value) {
        try {
            Field f = instance.getClass().getDeclaredField(name);
            f.setAccessible(true);
            f.set(instance, value);
            return true;
        } catch (Throwable t) {
            if (!"__superstreamShadow".equals(name) && !"__superstreamShadowReady".equals(name)) {
                logger.debug().msg("[REFLECT] trySetField failed for {}: {}", name, t.getMessage(), t);
            }
            return false;
        }
    }

    public static boolean trySetBooleanField(Object instance, String name, boolean value) {
        try {
            Field f = instance.getClass().getDeclaredField(name);
            f.setAccessible(true);
            f.setBoolean(instance, value);
            return true;
        } catch (Throwable t) {
            if (!"__superstreamShadow".equals(name) && !"__superstreamShadowReady".equals(name)) {
                logger.debug().msg("[REFLECT] trySetBooleanField failed for {}: {}", name, t.getMessage(), t);
            }
            return false;
        }
    }

    public static String getOriginalClientId(Object instance) {
        try {
            Properties p = ORIGINAL_PROPS.get(instance);
            if (p != null) {
                Object cid = p.get("client.id");
                return cid != null ? String.valueOf(cid) : "";
            }
        } catch (Throwable t) {
            logger.debug().msg("[CLIENT-ID] Failed to derive original client.id: {}", t.getMessage(), t);
        }
        return "";
    }
}

