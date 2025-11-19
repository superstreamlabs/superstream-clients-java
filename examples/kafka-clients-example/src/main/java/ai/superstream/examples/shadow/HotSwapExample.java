package ai.superstream.examples.shadow;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HotSwapExample {
	private static final Logger LOG = LoggerFactory.getLogger(HotSwapExample.class);

	public static void main(String[] args) throws Exception {
		String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
		String topic = System.getenv().getOrDefault("TOPIC", "example-topic");

		Properties p = new Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.CLIENT_ID_CONFIG, "shadow-hot-swap");

		KafkaProducer<String, String> producer = new KafkaProducer<>(p);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				LOG.info("Shutdown: flushing and closing producer");
				producer.flush();
				producer.close(Duration.ofSeconds(1));
			} catch (Throwable ignored) { }
		}));

		long i = 0;
		while (true) {
			producer.send(new ProducerRecord<>(topic, "loop-key", "loop-" + (i++)));
			Thread.sleep(5_000L);
		}
	}
}


