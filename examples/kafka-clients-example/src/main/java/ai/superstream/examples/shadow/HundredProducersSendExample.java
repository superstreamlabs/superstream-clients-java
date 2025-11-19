package ai.superstream.examples.shadow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class HundredProducersSendExample {
	public static void main(String[] args) throws Exception {
		String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
		String topic = System.getenv().getOrDefault("TOPIC", "example-topic");

		List<KafkaProducer<String, String>> producers = new ArrayList<>();
		try {
			// Create 100 producers
			for (int i = 0; i < 100; i++) {
				Properties p = new Properties();
				p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
				p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				p.put(ProducerConfig.CLIENT_ID_CONFIG, "shadow-100-" + i);
				KafkaProducer<String, String> producer = new KafkaProducer<>(p);
				producers.add(producer);
			}

			long startNs = System.nanoTime();
			int ticks = 12; // every 10s for 2 minutes total
			for (int t = 0; t < ticks; t++) {
				// Each producer sends once per tick
				for (int i = 0; i < producers.size(); i++) {
					producers.get(i).send(new ProducerRecord<>(topic, "many-" + i, "value-t" + t));
				}
				// Flush all producers after the wave
				for (KafkaProducer<String, String> producer : producers) {
					producer.flush();
				}
				// Sleep 10s between ticks except after the last wave
				if (t < ticks - 1) {
					Thread.sleep(10_000L);
				}
			}
			// Ensure total runtime ~2 minutes
			long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;
			long remaining = 120_000L - elapsedMs;
			if (remaining > 0) {
				Thread.sleep(remaining);
			}
		} finally {
			for (KafkaProducer<String, String> producer : producers) {
				try { producer.close(Duration.ofSeconds(1)); } catch (Throwable ignored) { }
			}
		}
	}
}


