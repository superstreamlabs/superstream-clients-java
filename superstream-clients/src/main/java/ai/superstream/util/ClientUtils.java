package ai.superstream.util;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class for client-related operations.
 */
public class ClientUtils {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(ClientUtils.class);

    /**
     * Get the complete producer configuration including default values.
     * @param explicitConfig The explicitly set configuration
     * @return A map containing all configurations including defaults
     */
    public static Map<String, Object> getCompleteProducerConfig(Map<String, Object> explicitConfig) {
        Map<String, Object> completeConfig = new HashMap<>();

        try {
            // Get the ProducerConfig class via reflection
            Class<?> producerConfigClass = Class.forName("org.apache.kafka.clients.producer.ProducerConfig");

            // Get access to the CONFIG static field which contains all default configurations
            Field configField = producerConfigClass.getDeclaredField("CONFIG");
            configField.setAccessible(true);
            Object configDef = configField.get(null);

            // Get the map of ConfigKey objects
            Field configKeysField = configDef.getClass().getDeclaredField("configKeys");
            configKeysField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, Object> configKeys = (Map<String, Object>) configKeysField.get(configDef);

            // For each config key, extract the default value
            for (Map.Entry<String, Object> entry : configKeys.entrySet()) {
                String configName = entry.getKey();
                Object configKey = entry.getValue();

                // Get the default value from the ConfigKey
                Field defaultValueField = configKey.getClass().getDeclaredField("defaultValue");
                defaultValueField.setAccessible(true);
                Object defaultValue = defaultValueField.get(configKey);

                if (defaultValue != null) {
                    completeConfig.put(configName, defaultValue);
                }
            }

            // Override defaults with explicitly set configurations
            completeConfig.putAll(explicitConfig);

            // Remove sensitive authentication information
            completeConfig.remove("ssl.keystore.password");
            completeConfig.remove("ssl.key.password");
            completeConfig.remove("ssl.truststore.password");
            completeConfig.remove("basic.auth.user.info");
            completeConfig.remove("sasl.jaas.config");
            completeConfig.remove("sasl.client.callback.handler.class");
            completeConfig.remove("sasl.login.callback.handler.class");

        } catch (Exception e) {
            logger.warn("Failed to extract default producer configs: " + e.getMessage(), e);
        }

        // If we couldn't get any defaults, just use the explicit config
        if (completeConfig.isEmpty()) {
            return new HashMap<>(explicitConfig);
        }

        return completeConfig;
    }

    /**
     * Get the version of the Superstream Clients library.
     * @return The version string
     */
    public static String getClientVersion() {
        // Option 1: Get version from package information (MANIFEST Implementation-Version)
        Package pkg = ClientUtils.class.getPackage();
        String version = (pkg != null) ? pkg.getImplementationVersion() : null;

        // Option 2: If option 1 returns null (e.g., when running from IDE), try to read from a properties file
        if (version == null) {
            // First attempt: properties file located under META-INF (the path used during packaging)
            version = readVersionFromProperties("/META-INF/superstream-version.properties");

            // Second attempt: fallback to root path (older builds)
            if (version == null) {
                version = readVersionFromProperties("/superstream-version.properties");
            }
        }

        // Option 3: If still null, use a hard-coded fallback
        if (version == null) {
            version = ""; // Default version if not found
        }

        return version;
    }

    /**
     * Helper that tries to load the version property from the given resource path.
     *
     * @param resourcePath classpath resource path, e.g. "/META-INF/superstream-version.properties"
     * @return the version value or null if not found / unreadable
     */
    private static String readVersionFromProperties(String resourcePath) {
        try (InputStream input = ClientUtils.class.getResourceAsStream(resourcePath)) {
            if (input != null) {
                Properties props = new Properties();
                props.load(input);
                String v = props.getProperty("version");
                if (v != null && !v.trim().isEmpty()) {
                    return v.trim();
                }
            }
        } catch (IOException ignored) {
            // ignore and let caller handle fallback
        }
        return null;
    }
} 