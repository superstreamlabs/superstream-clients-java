package ai.superstream.examples.shadow;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class InitConfigCustomSerializerExample {
	public static void main(String[] args) throws Exception {
		String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
		String topic = System.getenv().getOrDefault("TOPIC", "example-topic");

		Properties p = new Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		// Using custom (non-Kafka-namespace) serializers forces INIT_CONFIG run mode
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, CustomStringSerializer.class.getName());
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomStringSerializer.class.getName());
		p.put(ProducerConfig.CLIENT_ID_CONFIG, "init-config-custom-serializer");

		KafkaProducer<String, String> producer = new KafkaProducer<>(p);
		try {
			producer.send(new ProducerRecord<>(topic, "initcfg-key", "initcfg-value")).get();
			producer.flush();
		} finally {
			producer.close(Duration.ofSeconds(1));
		}
	}
}


