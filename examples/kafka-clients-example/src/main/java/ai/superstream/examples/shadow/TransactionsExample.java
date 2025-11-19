package ai.superstream.examples.shadow;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

public class TransactionsExample {
	public static void main(String[] args) throws Exception {
		String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
		String topic = System.getenv().getOrDefault("TOPIC", "example-topic");

		Properties p = new Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		p.put(ProducerConfig.CLIENT_ID_CONFIG, "shadow-transactions");
		p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-" + System.currentTimeMillis());

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
			producer.initTransactions();
			producer.beginTransaction();

			producer.send(new ProducerRecord<>(topic, "tx-key", "tx-value")).get();

			Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
			offsets.put(new TopicPartition(topic, 0), new OffsetAndMetadata(1L));
			try {
				producer.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata("shadow-tx-group"));
			} catch (Throwable ignored) {
				producer.sendOffsetsToTransaction(offsets, "shadow-tx-group");
			}

			producer.commitTransaction();
			producer.flush();
			producer.close(Duration.ofSeconds(1));
		}
	}
}


