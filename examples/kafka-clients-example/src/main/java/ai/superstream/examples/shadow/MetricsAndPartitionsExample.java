package ai.superstream.examples.shadow;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsAndPartitionsExample {
	private static final Logger LOG = LoggerFactory.getLogger(MetricsAndPartitionsExample.class);

	public static void main(String[] args) throws Exception {
		String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
		String topic = System.getenv().getOrDefault("TOPIC", "example-topic");

		Properties p = new Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.CLIENT_ID_CONFIG, "shadow-metrics-partitions");

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
			Map<?, ?> metrics = producer.metrics();
			LOG.info("metrics size={}", (metrics != null ? metrics.size() : 0));
			if (metrics != null) {
				int printed = 0;
				for (Map.Entry<?, ?> e : metrics.entrySet()) {
					LOG.debug("metric: {} = {}", String.valueOf(e.getKey()), String.valueOf(e.getValue()));
					if (++printed >= 5) break;
				}
			}

			List<?> partitions = producer.partitionsFor(topic);
			LOG.info("partitionsFor({}) size={}", topic, (partitions != null ? partitions.size() : 0));
			if (partitions != null) {
				for (Object pInfo : partitions) {
					LOG.debug("partitionInfo: {}", String.valueOf(pInfo));
				}
			}
		}
	}
}


