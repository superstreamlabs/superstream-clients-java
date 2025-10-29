package ai.superstream.examples.shadow;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenProducersRandomRateExample {
	private static final Logger logger = LoggerFactory.getLogger(TenProducersRandomRateExample.class);

	public static void main(String[] args) throws Exception {
		final String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
		final String topic = System.getenv().getOrDefault("TOPIC", "example-topic");

		logger.info("Starting TenProducersRandomRateExample");
		logger.info("Bootstrap servers: {}", bootstrap);
		logger.info("Topic: {}", topic);

		final int numProducers = 10;
		final long totalRunMillis = TimeUnit.MINUTES.toMillis(5); // 5 minutes
		final long minDelayMillis = TimeUnit.SECONDS.toMillis(1);
		final long maxDelayMillis = TimeUnit.SECONDS.toMillis(10);
		final long startMs = System.currentTimeMillis();
		final long endMs = startMs + totalRunMillis;

		final AtomicBoolean running = new AtomicBoolean(true);
		final CountDownLatch workersDone = new CountDownLatch(numProducers);
		final Random random = new Random();

		// Create producers
		final List<KafkaProducer<String, String>> producers = new ArrayList<>(numProducers);
		try {
			for (int i = 0; i < numProducers; i++) {
				Properties p = new Properties();
				p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
				p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				p.put(ProducerConfig.CLIENT_ID_CONFIG, "ten-random-" + i);
				// Connection timeouts to avoid indefinite hangs
				p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
				p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
				// Reasonable batching defaults
				p.put(ProducerConfig.LINGER_MS_CONFIG, "10");
				p.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
				KafkaProducer<String, String> producer = new KafkaProducer<>(p);
				producers.add(producer);
				if ((i + 1) % 5 == 0) {
					logger.info("Created {}/{} producers", i + 1, numProducers);
				}
			}
			logger.info("All {} producers created", numProducers);

			ScheduledExecutorService pool = Executors.newScheduledThreadPool(numProducers, new ThreadFactory() {
				private int idx = 0;
				@Override
				public Thread newThread(Runnable r) {
					Thread t = new Thread(r, "ten-producers-worker-" + (idx++));
					t.setDaemon(true);
					return t;
				}
			});

			for (int i = 0; i < numProducers; i++) {
				final int producerIndex = i;
				final KafkaProducer<String, String> producer = producers.get(i);
				pool.execute(() -> {
					try {
						long localSends = 0;
						while (running.get() && System.currentTimeMillis() < endMs) {
							long delay = minDelayMillis + (long) (random.nextDouble() * (maxDelayMillis - minDelayMillis));
							try {
								Thread.sleep(delay);
							} catch (InterruptedException ie) {
								Thread.currentThread().interrupt();
								break;
							}
							if (!running.get() || System.currentTimeMillis() >= endMs) break;

							String key = "p" + producerIndex;
							String value = "msg-" + localSends + "-" + Instant.now();
							producer.send(new ProducerRecord<>(topic, key, value), (md, ex) -> {
								if (ex != null) {
									logger.warn("Producer {} send failed: {}", producerIndex, ex.toString());
								}
							});
							localSends++;
							if (localSends % 10 == 0) {
								logger.info("Producer {} sent {} messages so far", producerIndex, localSends);
							}
						}
						logger.info("Producer {} exiting loop, total sent={}", producerIndex, localSends);
					} finally {
						workersDone.countDown();
					}
				});
			}

			// Allow to run for 5 minutes
			long remaining = endMs - System.currentTimeMillis();
			if (remaining > 0) {
				logger.info("Running for ~{} ms", remaining);
				try {
					Thread.sleep(remaining);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}
			}
			running.set(false);

			// Wait for workers to finish
			if (!workersDone.await(30, TimeUnit.SECONDS)) {
				logger.warn("Workers did not finish within timeout; proceeding to shutdown");
			}

		} catch (Exception e) {
			logger.error("Error during TenProducersRandomRateExample", e);
			throw e;
		} finally {
			logger.info("Closing all {} producers...", producers.size());
			for (int i = 0; i < producers.size(); i++) {
				try {
					producers.get(i).close(Duration.ofSeconds(2));
					if ((i + 1) % 5 == 0) {
						logger.info("Closed {}/{} producers", i + 1, producers.size());
					}
				} catch (Throwable ignored) { }
			}
			logger.info("All producers closed. Exiting.");
		}
	}
}


