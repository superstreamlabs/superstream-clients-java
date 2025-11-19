package ai.superstream.shadow;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Translates application's Kafka producer configuration for the shaded shadow producer.
 * 
 * Rewrites Kafka class names (org.apache.kafka.*) to shaded equivalents (ai.superstream.shaded.org.apache.kafka.*).
 * Throws error if serializer is missing or custom (non-Kafka) serializer is detected.
 */
public final class ConfigTranslator {

    private static final String SHADED_PREFIX = "ai.superstream.shaded.";
    private static final Pattern KAFKA_NAMESPACE_PATTERN =
            Pattern.compile("(?i)(^|\\.)org\\.apache\\.kafka(\\.|$)");

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String PARTITIONER_CLASS = "partitioner.class";
    private static final String INTERCEPTOR_CLASSES = "interceptor.classes";
    private static final String METRIC_REPORTER_CLASSES = "metric.reporters";

    private static final String DEFAULT_PARTITIONER = "org.apache.kafka.clients.producer.internals.DefaultPartitioner";

    private static final String[] BUILTIN_SERIALIZERS = new String[] {
        "org.apache.kafka.common.serialization.StringSerializer",
        "org.apache.kafka.common.serialization.ByteArraySerializer",
        "org.apache.kafka.common.serialization.ByteBufferSerializer",
        "org.apache.kafka.common.serialization.BytesSerializer",
        "org.apache.kafka.common.serialization.LongSerializer",
        "org.apache.kafka.common.serialization.IntegerSerializer",
        "org.apache.kafka.common.serialization.ShortSerializer",
        "org.apache.kafka.common.serialization.DoubleSerializer"
    };

    private ConfigTranslator() { }

    public static Properties translate(Properties original) {
        Properties shaded = new Properties();
        if (original != null) {
            shaded.putAll(original);
        }

        // Always normalize serializers and partitioner
        shaded.put(KEY_SERIALIZER, chooseSerializer(shaded.get(KEY_SERIALIZER)));
        shaded.put(VALUE_SERIALIZER, chooseSerializer(shaded.get(VALUE_SERIALIZER)));
        shaded.put(PARTITIONER_CLASS, choosePartitioner(shaded.get(PARTITIONER_CLASS)));

        // Drop interceptors and metric reporters in phase 1
        shaded.remove(INTERCEPTOR_CLASSES);
        shaded.remove(METRIC_REPORTER_CLASSES);

        return shaded;
    }

    private static Object chooseSerializer(Object configured) {
        String className = valueToClassName(configured);
        if (className == null || className.trim().isEmpty()) {
            throw new IllegalStateException("Serializer must be configured. Kafka requires explicit serializer configuration.");
        }
        // If it's from Kafka namespace, shade it and use it
        if (KAFKA_NAMESPACE_PATTERN.matcher(className).find()) {
            return toShaded(className);
        }
        // Custom serializer - this shouldn't happen in SHADOW mode (would trigger INIT_CONFIG)
        throw new IllegalStateException("Custom serializer detected in SHADOW mode: " + className + 
            ". Custom serializers should trigger INIT_CONFIG mode.");
    }

    private static Object choosePartitioner(Object configured) {
        String className = valueToClassName(configured);
        String effective = (className == null) ? DEFAULT_PARTITIONER : className;
        if (!isKafkaBuiltin(effective)) {
            effective = DEFAULT_PARTITIONER;
        }
        return toShaded(effective);
    }

    private static boolean isKafkaBuiltin(String className) {
        if (className == null) return false;
        if (!KAFKA_NAMESPACE_PATTERN.matcher(className).find()) return false;
        for (String b : BUILTIN_SERIALIZERS) {
            if (b.equals(className)) return true;
        }
        // also treat default partitioner as builtin
        if (DEFAULT_PARTITIONER.equals(className)) return true;
        return false;
    }

    private static String toShaded(String className) {
        if (className == null) return null;
        if (className.startsWith(SHADED_PREFIX)) return className;
        if (KAFKA_NAMESPACE_PATTERN.matcher(className).find()) {
            return SHADED_PREFIX + className;
        }
        return className; // Already shaded or not a Kafka class
    }

    private static String valueToClassName(Object value) {
        if (value == null) return null;
        if (value instanceof Class<?>) return ((Class<?>) value).getName();
        return String.valueOf(value);
    }
}


