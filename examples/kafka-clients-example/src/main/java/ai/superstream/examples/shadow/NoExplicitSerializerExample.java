package ai.superstream.examples.shadow;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class NoExplicitSerializerExample {
	public static void main(String[] args) throws Exception {
		String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
		String topic = System.getenv().getOrDefault("TOPIC", "example-topic");

		Properties p = new Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		// rely on class-based serializer configs
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		p.put(ProducerConfig.CLIENT_ID_CONFIG, "shadow-no-explicit-serializer");

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
			producer.send(new ProducerRecord<>(topic, "k", "v")).get();
			producer.flush();
		}
	}
}


