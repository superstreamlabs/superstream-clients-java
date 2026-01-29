package ai.superstream.agent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for KafkaConsumerInterceptor.
 * These tests simulate more realistic scenarios with actual Kafka consumer configurations.
 */
public class KafkaConsumerInterceptorIntegrationTest {

    private ByteArrayOutputStream outputStreamCaptor;
    private PrintStream standardOut;

    @BeforeEach
    public void setUp() {
        // Capture System.out to verify console output
        outputStreamCaptor = new ByteArrayOutputStream();
        standardOut = System.out;
        System.setOut(new PrintStream(outputStreamCaptor));
    }

    @AfterEach
    public void tearDown() {
        // Restore original System.out
        System.setOut(standardOut);
    }

    @Test
    public void testInterceptor_WithTypicalConsumerProperties() {
        // Arrange - typical Kafka consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                         "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                         "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer");

        Object[] args = new Object[]{consumerProps};

        // Act
        KafkaConsumerInterceptor.onEnter(args);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should intercept consumer with typical properties");
    }

    @Test
    public void testInterceptor_WithMinimalConsumerProperties() {
        // Arrange - minimal Kafka consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "minimal-group");

        Object[] args = new Object[]{consumerProps};

        // Act
        KafkaConsumerInterceptor.onEnter(args);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should intercept consumer with minimal properties");
    }

    @Test
    public void testInterceptor_SimulateConsumerCreationLifecycle() {
        // Arrange
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "lifecycle-test-group");
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "lifecycle-consumer");

        Object[] enterArgs = new Object[]{consumerProps};
        Object mockConsumer = new MockKafkaConsumer();

        // Act - simulate the full constructor lifecycle
        KafkaConsumerInterceptor.onEnter(enterArgs);
        KafkaConsumerInterceptor.onExit(mockConsumer);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should intercept during consumer creation lifecycle");
    }

    @Test
    public void testInterceptor_WithMultipleConsumers() {
        // Arrange
        Properties props1 = new Properties();
        props1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props1.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
        props1.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-1");

        Properties props2 = new Properties();
        props2.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props2.put(ConsumerConfig.GROUP_ID_CONFIG, "group-2");
        props2.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-2");

        Object[] args1 = new Object[]{props1};
        Object[] args2 = new Object[]{props2};

        // Act
        KafkaConsumerInterceptor.onEnter(args1);
        KafkaConsumerInterceptor.onEnter(args2);

        // Assert
        String output = outputStreamCaptor.toString();
        int messageCount = output.split("hello world - KafkaConsumer intercepted!", -1).length - 1;
        assertEquals(2, messageCount, "Should intercept both consumers");
    }

    @Test
    public void testInterceptor_PerformanceImpact() {
        // Arrange
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "performance-test-group");

        Object[] args = new Object[]{consumerProps};

        // Act & Assert - measure performance impact
        long startTime = System.nanoTime();
        
        for (int i = 0; i < 100; i++) {
            KafkaConsumerInterceptor.onEnter(args);
        }
        
        long endTime = System.nanoTime();
        long totalTime = endTime - startTime;
        
        // Each call should be very fast (less than 1ms on average)
        double averageTimeMs = (totalTime / 100.0) / 1_000_000.0;
        assertTrue(averageTimeMs < 1.0, 
                   "Interceptor should have minimal performance impact, average time: " + averageTimeMs + "ms");
    }

    /**
     * Mock KafkaConsumer class for testing purposes.
     */
    private static class MockKafkaConsumer {
        // Empty mock class to simulate a KafkaConsumer
    }
} 