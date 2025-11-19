package ai.superstream.examples.shadow;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncCallbackExample {
	private static final Logger LOG = LoggerFactory.getLogger(AsyncCallbackExample.class);

	public static void main(String[] args) throws Exception {
		String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
		String topic = System.getenv().getOrDefault("TOPIC", "example-topic");

		Properties p = new Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.CLIENT_ID_CONFIG, "shadow-async-callback");

		KafkaProducer<String, String> producer = new KafkaProducer<>(p);
		try {
			producer.send(new ProducerRecord<>(topic, "async-key", "async-value"), new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null) {
						LOG.warn("async send error", exception);
					} else if (metadata != null) {
						LOG.info("async send success: topic={}, partition={}, offset={}",
								metadata.topic(), metadata.partition(), metadata.offset());
					} else {
						LOG.info("async send completed without metadata");
					}
				}
			});
			producer.flush();
		} finally {
			producer.close(Duration.ofSeconds(1));
		}
	}
}


