package ai.superstream.examples.shadow;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AbortTransactionExample {
	public static void main(String[] args) throws Exception {
		String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
		String topic = System.getenv().getOrDefault("TOPIC", "example-topic");

		Properties p = new Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.CLIENT_ID_CONFIG, "shadow-abort");
		p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "abort-" + System.currentTimeMillis());

		KafkaProducer<String, String> producer = new KafkaProducer<>(p);
		try {
			producer.initTransactions();
			producer.beginTransaction();
			producer.send(new ProducerRecord<>(topic, "abort-key", "abort-value"));
			producer.abortTransaction();
		} finally {
			producer.close(Duration.ofSeconds(1));
		}
	}
}


