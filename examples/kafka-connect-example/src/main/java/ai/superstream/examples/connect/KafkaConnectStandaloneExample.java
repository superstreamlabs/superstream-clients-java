package ai.superstream.examples.connect;

import org.apache.kafka.connect.cli.ConnectStandalone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal Kafka Connect standalone example that runs a Connect worker
 * and a single connector in this JVM. When launched with the Superstream agent
 * (-javaagent), all Kafka producers created inside Connect will be routed
 * through the Superstream shadow layer.
 *
 * By default this uses the configuration files from the {@code config}
 * directory in this module:
 *   - connect-standalone.properties
 *   - connect-file-source.properties
 *
 * You can override the locations via:
 *   - System property: connect.example.config.dir (base dir)
 *   - Env: CONNECT_WORKER_CONFIG
 *   - Env: CONNECT_CONNECTOR_CONFIG
 */
public class KafkaConnectStandaloneExample {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectStandaloneExample.class);

    public static void main(String[] args) throws Exception {
        String workerConfig;
        String connectorConfig;

        if (args.length >= 2) {
            workerConfig = args[0];
            connectorConfig = args[1];
        } else {
            String baseDir = System.getProperty(
                    "connect.example.config.dir",
                    "examples/kafka-connect-example/config"
            );
            workerConfig = getenvOrDefault("CONNECT_WORKER_CONFIG", baseDir + "/connect-standalone.properties");
            connectorConfig = getenvOrDefault("CONNECT_CONNECTOR_CONFIG", baseDir + "/connect-file-source.properties");
        }

        LOG.info("Starting Kafka Connect Standalone with:");
        LOG.info("  worker config   = {}", workerConfig);
        LOG.info("  connector config= {}", connectorConfig);
        LOG.info("  (launch this JVM with -javaagent:...superstream-clients-2.0.0.jar to enable the Superstream agent)");

        // Launch a Kafka Connect standalone worker in this JVM.
        ConnectStandalone.main(new String[] { workerConfig, connectorConfig });
    }

    private static String getenvOrDefault(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? defaultValue : v;
    }
}


