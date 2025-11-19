package ai.superstream.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import ai.superstream.agent.KafkaProducerInterceptor;
import ai.superstream.model.ClientStatsMessage;
import ai.superstream.util.ClientUtils;
import ai.superstream.util.KafkaPropertiesUtils;
import ai.superstream.util.NetworkUtils;
import ai.superstream.util.SuperstreamLogger;

/**
 * Reports client statistics to the superstream.clients topic periodically.
 */
public class ClientStatsReporter {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(ClientStatsReporter.class);
    private static final String CLIENTS_TOPIC = "superstream.clients";
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    // Default reporting interval (5 minutes) – overridden when metadata provides a different value
    private static final long DEFAULT_REPORT_INTERVAL_MS = 300000; // 5 minutes
    private static final String DISABLED_ENV_VAR = "SUPERSTREAM_DISABLED";

    /**
     * Baseline producer-level metrics that should always be present in reports.
     *
     * Semantics:
     * - compression-rate-avg = 1.0 → no compression
     * - outgoing-byte-total = 0.0  → no bytes sent yet
     * - record-send-total   = 0.0  → no records sent yet
     * - record-size-avg     = 0.0  → no records yet
     */
    private static final Map<String, Double> BASELINE_PRODUCER_METRICS;
    static {
        Map<String, Double> m = new HashMap<>();
        m.put("compression-rate-avg", 1.0);
        m.put("outgoing-byte-total", 0.0);
        m.put("record-send-total", 0.0);
        m.put("record-size-avg", 0.0);
        BASELINE_PRODUCER_METRICS = Collections.unmodifiableMap(m);
    }

    // Shared scheduler for all reporters to minimize thread usage
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "superstream-client-stats-reporter");
        t.setDaemon(true);
        return t;
    });

    // Coordinator per cluster to minimise producer usage
    private static final ConcurrentHashMap<String, ClusterStatsCoordinator> coordinators = new ConcurrentHashMap<>();

    private final ClientStatsCollector statsCollector;
    private final Properties producerProperties;
    private final String clientId;
    private final AtomicBoolean registered = new AtomicBoolean(false);
    private final boolean disabled;
    private final String producerUuid;

    private final AtomicReference<Map<String, Double>> latestMetrics = new AtomicReference<>(
            new HashMap<>(BASELINE_PRODUCER_METRICS));
    private java.util.Map<String, java.util.Map<String, Double>> latestTopicMetrics = new java.util.HashMap<>();
    private java.util.Map<String, java.util.Map<String, Double>> latestNodeMetrics = new java.util.HashMap<>();
    private java.util.Map<String, String> latestAppInfoMetrics = new java.util.HashMap<>();
    private final ConcurrentSkipListSet<String> topicsWritten = new ConcurrentSkipListSet<>();
    private volatile java.util.Map<String, Object> originalConfig = null;
    private volatile java.util.Map<String, Object> optimizedConfig = null;
    private String mostImpactfulTopic;
    private String error;

    /**
     * Creates a new client stats reporter.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param clientProperties Producer properties to use for authentication
     * @param clientId         The client ID to include in reports
     * @param producerUuid     The producer UUID
     */
    public ClientStatsReporter(String bootstrapServers, Properties clientProperties, String clientId, String producerUuid) {
        this.clientId = clientId;
        this.disabled = Boolean.parseBoolean(System.getenv(DISABLED_ENV_VAR));
        this.producerUuid = producerUuid;

        if (this.disabled) {
            logger.debug("Superstream stats reporting is disabled via environment variable");
        }

        this.statsCollector = new ClientStatsCollector();

        // Copy essential client configuration properties from the original client
        this.producerProperties = new Properties();
        KafkaPropertiesUtils.copyClientConfigurationProperties(clientProperties, this.producerProperties);

        // Set up basic producer properties
        this.producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG,
                KafkaProducerInterceptor.SUPERSTREAM_LIBRARY_PREFIX + "client-stats-reporter");

        // Use efficient compression settings for the reporter itself
        this.producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        this.producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        this.producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1000);

        // Mark as registered for recordBatch logic
        this.registered.set(true);

        // Register with per-cluster coordinator
        String clusterKey = normalizeBootstrapServers(bootstrapServers);
        ClusterStatsCoordinator coord = coordinators.computeIfAbsent(clusterKey,
                k -> new ClusterStatsCoordinator(bootstrapServers, producerProperties));
        coord.addReporter(this);
    }

    /**
     * Records compression statistics for a batch of messages.
     * This method should be called by the producer each time it sends a batch.
     *
     * @param uncompressedSize Size of batch before compression (in bytes)
     * @param compressedSize   Size of batch after compression (in bytes)
     */
    public void recordBatch(long uncompressedSize, long compressedSize) {
        // Only record if we're actually running and not disabled
        if (registered.get() && !disabled) {
            statsCollector.recordBatch(uncompressedSize, compressedSize);
        }
    }

    // Drain stats into producer, called by coordinator
    void drainInto(Producer<String, String> producer) {
        if (disabled || !registered.get()) {
            return; // Do not send stats when disabled or deactivated
        }

        try {
            ClientStatsCollector.Stats stats = statsCollector.captureAndReset();
            long totalBytesBefore = stats.getBytesBeforeCompression();
            long totalBytesAfter = stats.getBytesAfterCompression();

            ClientStatsMessage message = new ClientStatsMessage(
                    clientId,
                    NetworkUtils.getLocalIpAddress(),
                    totalBytesBefore,
                    totalBytesAfter,
                    ClientUtils.getClientVersion(),
                    NetworkUtils.getHostname(),
                    producerUuid);

            // Always attach metric snapshots, defaulting to empty maps if no data available
            java.util.Map<String, Double> metricsSnapshot = latestMetrics.get();
            message.setProducerMetrics(metricsSnapshot != null ? metricsSnapshot : new java.util.HashMap<>());

            java.util.Map<String, java.util.Map<String, Double>> topicMetricsSnapshot = latestTopicMetrics;
            message.setTopicMetrics(topicMetricsSnapshot != null ? topicMetricsSnapshot : new java.util.HashMap<>());

            java.util.Map<String, java.util.Map<String, Double>> nodeMetricsSnapshot = latestNodeMetrics;
            message.setNodeMetrics(nodeMetricsSnapshot != null ? nodeMetricsSnapshot : new java.util.HashMap<>());

            java.util.Map<String, String> appInfoMetricsSnapshot = latestAppInfoMetrics;
            message.setAppInfoMetrics(appInfoMetricsSnapshot != null ? appInfoMetricsSnapshot : new java.util.HashMap<>());

            // Always set originalConfig and optimizedConfig, defaulting to empty map if null
            message.setOriginalConfiguration(originalConfig != null ? originalConfig : new java.util.HashMap<>());
            message.setOptimizedConfiguration(optimizedConfig != null ? optimizedConfig : new java.util.HashMap<>());

            // Attach topics list - always set it, empty if no topics
            message.setTopics(new java.util.ArrayList<>(topicsWritten));

            // When building the ClientStatsMessage, set the most impactful topic if available
            if (mostImpactfulTopic != null) {
                message.setMostImpactfulTopic(mostImpactfulTopic);
            }

            // Set language and error fields
            message.setLanguage("Java");
            message.setError(error != null ? error : "");

            String json = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(CLIENTS_TOPIC, json);
            producer.send(record);

            logger.debug("Producer {} stats sent: before={} bytes, after={} bytes",
                    clientId, totalBytesBefore, totalBytesAfter);
        } catch (Exception e) {
            logger.error("[ERR-021] Failed to drain stats for client {}: {}", clientId, e.getMessage(), e);
        }
    }

    private static String normalizeBootstrapServers(String servers) {
        if (servers == null)
            return "";
        String[] parts = servers.split(",");
        java.util.Arrays.sort(parts);
        return String.join(",", parts).trim();
    }

    /**
     * Merge the latest producer-level metrics into the cached snapshot.
     * <p>
     * We <strong>merge</strong> instead of replacing the whole map so that metrics which are
     * temporarily absent (e.g. become NaN / not reported while the producer is idle)
     * still appear in the next heartbeat with their <em>last known</em> value.  Any key
     * present in {@code metrics} overwrites the previous value – even when the new
     * value is {@code 0.0}, negative or otherwise – but keys that are <em>missing</em>
     * are left untouched.
     * <p>
     * Special handling for compression-rate-avg and record-size-avg: if the new value is 0 and the previous
     * value is greater than 0, preserve the previous value.
     */
    public void updateProducerMetrics(java.util.Map<String, Double> metrics) {
        if (!disabled && metrics != null) {
            latestMetrics.updateAndGet(prev -> {
                java.util.Map<String, Double> merged = new java.util.HashMap<>(prev);
                
                // Special handling for compression-rate-avg and record-size-avg
                for (java.util.Map.Entry<String, Double> entry : metrics.entrySet()) {
                    String key = entry.getKey();
                    Double newValue = entry.getValue();
                    
                    if (("compression-rate-avg".equals(key) || "record-size-avg".equals(key)) && newValue != null && newValue == 0.0) {
                        Double prevValue = merged.get(key);
                        if (prevValue != null && prevValue > 0.0) {
                            // Keep the previous non-zero value instead of overwriting with 0
                            continue;
                        }
                    }
                    
                    merged.put(key, newValue);
                }
                
                return merged;
            });
        }
    }

    /**
     * Merge the latest per-topic metrics.  Same rationale as above, but we first
     * locate / create the nested map for each topic, then merge its individual
     * metric values.
     * <p>
     * Special handling for compression-rate: if the new value is 0 and the previous
     * value is greater than 0, preserve the previous value.
     */
    public void updateTopicMetrics(java.util.Map<String, java.util.Map<String, Double>> topicMetrics) {
        if (!disabled && topicMetrics != null) {
            topicMetrics.forEach((topic, metricMap) -> {
                java.util.Map<String, Double> existing = latestTopicMetrics.computeIfAbsent(topic, k -> new java.util.HashMap<>());
                
                // Special handling for compression-rate
                for (java.util.Map.Entry<String, Double> entry : metricMap.entrySet()) {
                    String key = entry.getKey();
                    Double newValue = entry.getValue();
                    
                    if ("compression-rate".equals(key) && newValue != null && newValue == 0.0) {
                        Double prevValue = existing.get(key);
                        if (prevValue != null && prevValue > 0.0) {
                            // Keep the previous non-zero value instead of overwriting with 0
                            continue;
                        }
                    }
                    
                    existing.put(key, newValue);
                }
            });
        }
    }

    /**
     * Merge the latest per-node metrics (broker-level statistics).  Behaviour is
     * analogous to {@link #updateTopicMetrics}.
     */
    public void updateNodeMetrics(java.util.Map<String, java.util.Map<String, Double>> nodeMetrics) {
        if (!disabled && nodeMetrics != null) {
            nodeMetrics.forEach((node, metricMap) -> {
                java.util.Map<String, Double> existing = latestNodeMetrics.computeIfAbsent(node, k -> new java.util.HashMap<>());
                existing.putAll(metricMap);
            });
        }
    }

    /**
     * Merge the latest <code>app-info</code> gauge values. These are string
     * properties (Kafka version, client id, etc.), so we store them as
     * {@code Map&lt;String,String&gt;}.  As with the numeric maps we merge to retain
     * previously-seen keys that might be absent in the current snapshot.
     */
    public void updateAppInfoMetrics(java.util.Map<String, String> appInfoMetrics) {
        if (appInfoMetrics != null && !disabled) {
            if (this.latestAppInfoMetrics == null) {
                this.latestAppInfoMetrics = new java.util.HashMap<>();
            }
            this.latestAppInfoMetrics.putAll(appInfoMetrics);
        }
    }

    public void addTopics(java.util.Collection<String> topics) {
        if (!disabled && topics != null) {
            topicsWritten.addAll(topics);
        }
    }

    public void setConfigurations(java.util.Map<String, Object> originalCfg,
            java.util.Map<String, Object> optimizedCfg) {
        if (!disabled) {
            this.originalConfig = (originalCfg != null) ? originalCfg : new java.util.HashMap<>();
            this.optimizedConfig = (optimizedCfg != null) ? optimizedCfg : new java.util.HashMap<>();
        }
    }

    public void updateMostImpactfulTopic(String topic) {
        this.mostImpactfulTopic = topic;
    }

    public void updateError(String error) {
        this.error = error;
    }

    // Coordinator class per cluster
    private static class ClusterStatsCoordinator {
        private final String bootstrapServers;
        private final Properties baseProps;
        private final CopyOnWriteArrayList<ClientStatsReporter> reporters = new CopyOnWriteArrayList<>();
        private final AtomicBoolean scheduled = new AtomicBoolean(false);

        // Report interval for this cluster (milliseconds)
        private final long reportIntervalMs;

        ClusterStatsCoordinator(String bootstrapServers, Properties baseProps) {
            this.bootstrapServers = bootstrapServers;
            this.baseProps = baseProps;

            long interval = DEFAULT_REPORT_INTERVAL_MS;
            try {
                ai.superstream.model.MetadataMessage meta = ai.superstream.core.SuperstreamManager.getInstance()
                        .getOrFetchMetadataMessage(bootstrapServers, baseProps).getKey();
                if (meta != null && meta.getReportIntervalMs() != null && meta.getReportIntervalMs() > 0) {
                    interval = meta.getReportIntervalMs();
                }
            } catch (Exception e) {
                logger.warn("Could not obtain report interval from metadata: {}. Using default {} ms", e.getMessage(), DEFAULT_REPORT_INTERVAL_MS);
            }

            this.reportIntervalMs = interval;
        }

        void addReporter(ClientStatsReporter r) {
            reporters.add(r);
            // Schedule immediate run for this specific reporter
            scheduler.schedule(() -> {
                try (Producer<String, String> producer = new KafkaProducer<>(baseProps)) {
                    r.drainInto(producer);
                    producer.flush();
                } catch (Exception e) {
                    logger.error("[ERR-046] Failed to send immediate stats for new reporter: {}", e.getMessage(), e);
                }
            }, 0, TimeUnit.MILLISECONDS);

            // Only schedule the periodic task if not already scheduled
            if (scheduled.compareAndSet(false, true)) {
                scheduler.scheduleAtFixedRate(this::run, reportIntervalMs, reportIntervalMs, TimeUnit.MILLISECONDS);
            }
        }

        // Allows outer class to remove a reporter when the underlying KafkaProducer is closed
        void removeReporter(ClientStatsReporter r) {
            reporters.remove(r);
        }

        private void run() {
            if (reporters.isEmpty())
                return;
            
            // Log the configuration before creating producer
            if (SuperstreamLogger.isDebugEnabled()) {
                StringBuilder configLog = new StringBuilder("Creating internal ClientStatsReporter producer with configuration: ");
                baseProps.forEach((key, value) -> {
                    // Mask sensitive values
                    if (key.toString().toLowerCase().contains("password") || 
                        key.toString().toLowerCase().contains("sasl.jaas.config") ||
                        key.toString().equals("basic.auth.user.info")) {
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
            
            try (Producer<String, String> producer = new KafkaProducer<>(baseProps)) {
                for (ClientStatsReporter r : reporters) {
                    r.drainInto(producer);
                }
                producer.flush();
                logger.debug("Successfully reported cluster stats to {}", CLIENTS_TOPIC);
            } catch (Exception e) {
                logger.error("[ERR-022] Cluster stats coordinator failed for {}, please make sure the Kafka user has read/write/describe permissions on superstream.* topics: {}", bootstrapServers, e.getMessage(), e);
            }
        }
    }

    /**
     * Deactivate this reporter so that it no longer emits statistics.  The reporter
     * remains registered in the coordinator list but {@link #drainInto} becomes a
     * no-op which is inexpensive and avoids extra allocations.
     */
    public void deactivate() {
        registered.set(false);
    }

    /**
     * Remove the given reporter instance from all cluster coordinators. Called by the
     * agent when the application closes its <code>KafkaProducer</code> so that we
     * do not retain references to obsolete reporter objects.
     */
    public static void deregisterReporter(ClientStatsReporter reporter) {
        if (reporter == null) {
            return;
        }
        coordinators.values().forEach(coord -> coord.removeReporter(reporter));
    }
}