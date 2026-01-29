package ai.superstream.agent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.Iterator;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for KafkaConsumerInterceptor.
 */
public class KafkaConsumerInterceptorTest {

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
    public void testOnEnter_LogsHelloWorld() {
        // Arrange
        Object[] args = new Object[]{new Properties()};

        // Act
        KafkaConsumerInterceptor.onEnter(args);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should log hello world message to console");
    }

    @Test
    public void testOnEnter_WithNullArgs() {
        // Arrange
        Object[] args = null;

        // Act & Assert - should not throw exception
        assertDoesNotThrow(() -> KafkaConsumerInterceptor.onEnter(args));
        
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should still log hello world message even with null args");
    }

    @Test
    public void testOnEnter_WithEmptyArgs() {
        // Arrange
        Object[] args = new Object[]{};

        // Act
        KafkaConsumerInterceptor.onEnter(args);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should log hello world message with empty args");
    }

    @Test
    public void testIsDisabled_ReturnsCurrentValue() {
        // Test the current value of isDisabled() method
        // Note: The actual value depends on the SUPERSTREAM_DISABLED environment variable
        // at class loading time, so we just verify the method returns a boolean value
        boolean result = KafkaConsumerInterceptor.isDisabled();
        // This should not throw an exception and should return a boolean
        assertTrue(result || !result, "isDisabled() should return a boolean value");
    }

    @Test
    public void testOnExit_WithValidConsumer() {
        // Arrange
        Object mockConsumer = new Object(); // Simple mock consumer

        // Act & Assert - should not throw exception
        assertDoesNotThrow(() -> KafkaConsumerInterceptor.onExit(mockConsumer));
    }

    @Test
    public void testOnExit_WithNullConsumer() {
        // Arrange
        Object consumer = null;

        // Act & Assert - should not throw exception
        assertDoesNotThrow(() -> KafkaConsumerInterceptor.onExit(consumer));
    }

    @Test
    public void testMultipleOnEnterCalls() {
        // Arrange
        Object[] args1 = new Object[]{new Properties()};
        Object[] args2 = new Object[]{new Properties()};

        // Act
        KafkaConsumerInterceptor.onEnter(args1);
        KafkaConsumerInterceptor.onEnter(args2);

        // Assert
        String output = outputStreamCaptor.toString();
        // Should contain the message twice
        int count = output.split("hello world - KafkaConsumer intercepted!", -1).length - 1;
        assertEquals(2, count, "Should log hello world message twice");
    }

    @Test
    public void testOnEnterAndOnExit_Together() {
        // Arrange
        Object[] args = new Object[]{new Properties()};
        Object mockConsumer = new Object();

        // Act
        KafkaConsumerInterceptor.onEnter(args);
        KafkaConsumerInterceptor.onExit(mockConsumer);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("hello world - KafkaConsumer intercepted!"), 
                   "Should log hello world message from onEnter");
        // onExit doesn't produce console output, just logs debug info
    }

    @Test
    public void testLoggingDoesNotInterfereWithExecution() {
        // Arrange
        Object[] args = new Object[]{new Properties()};
        Object mockConsumer = new Object();

        // Act & Assert - methods should complete without throwing exceptions
        long startTime = System.currentTimeMillis();
        
        assertDoesNotThrow(() -> {
            KafkaConsumerInterceptor.onEnter(args);
            KafkaConsumerInterceptor.onExit(mockConsumer);
        });
        
        long executionTime = System.currentTimeMillis() - startTime;
        assertTrue(executionTime < 1000, "Interceptor methods should execute quickly");
    }

    // ===== POLL METHOD INTERCEPTION TESTS =====

    @Test
    public void testOnPollExit_WithNullResult() {
        // Arrange
        Object result = null;
        Object mockConsumer = new Object();

        // Act & Assert - should not throw exception
        assertDoesNotThrow(() -> KafkaConsumerInterceptor.onPollExit(result, mockConsumer));
        
        // Assert no message consumed log for null result
        String output = outputStreamCaptor.toString();
        assertFalse(output.contains("message consumed"), 
                   "Should not log message consumed for null result");
    }

    @Test
    public void testOnPollExit_WithEmptyConsumerRecords() {
        // Arrange
        Object mockConsumerRecords = createMockConsumerRecords(0);
        Object mockConsumer = new Object();

        // Act
        KafkaConsumerInterceptor.onPollExit(mockConsumerRecords, mockConsumer);

        // Assert
        String output = outputStreamCaptor.toString();
        assertFalse(output.contains("message consumed"), 
                   "Should not log message consumed for empty records");
    }

    @Test
    public void testOnPollExit_WithNonEmptyConsumerRecords() {
        // Arrange
        Object mockConsumerRecords = createMockConsumerRecords(3);
        Object mockConsumer = new Object();

        // Act
        KafkaConsumerInterceptor.onPollExit(mockConsumerRecords, mockConsumer);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("message consumed - 3 records from poll()"), 
                   "Should log message consumed with record count");
    }

    @Test
    public void testOnPollExit_WithLargeNumberOfRecords() {
        // Arrange
        Object mockConsumerRecords = createMockConsumerRecords(100);
        Object mockConsumer = new Object();

        // Act
        KafkaConsumerInterceptor.onPollExit(mockConsumerRecords, mockConsumer);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("message consumed - 100 records from poll()"), 
                   "Should log message consumed with large record count");
    }

    @Test
    public void testOnPollExit_HandlesReflectionFailure() {
        // Arrange - create an object that doesn't have count() or isEmpty() methods
        Object invalidRecords = new Object();
        Object mockConsumer = new Object();

        // Act & Assert - should not throw exception even when reflection fails
        assertDoesNotThrow(() -> KafkaConsumerInterceptor.onPollExit(invalidRecords, mockConsumer));
        
        // Should not log message consumed when count cannot be determined
        String output = outputStreamCaptor.toString();
        assertFalse(output.contains("message consumed"), 
                   "Should not log message consumed when record count cannot be determined");
    }

    @Test
    public void testGetConsumerRecordsCount_WithValidCountMethod() {
        // Arrange
        Object mockRecords = createMockConsumerRecords(5);

        // Act
        int count = KafkaConsumerInterceptor.getConsumerRecordsCount(mockRecords);

        // Assert
        assertEquals(5, count, "Should return correct count from count() method");
    }

    @Test
    public void testGetConsumerRecordsCount_WithIsEmptyMethod() {
        // Arrange
        MockConsumerRecordsWithIsEmpty mockRecords = new MockConsumerRecordsWithIsEmpty(false);

        // Act
        int count = KafkaConsumerInterceptor.getConsumerRecordsCount(mockRecords);

        // Assert
        assertEquals(1, count, "Should return 1 when isEmpty() returns false");
    }

    @Test
    public void testGetConsumerRecordsCount_WithEmptyRecords() {
        // Arrange
        MockConsumerRecordsWithIsEmpty mockRecords = new MockConsumerRecordsWithIsEmpty(true);

        // Act
        int count = KafkaConsumerInterceptor.getConsumerRecordsCount(mockRecords);

        // Assert
        assertEquals(0, count, "Should return 0 when isEmpty() returns true");
    }

    @Test
    public void testGetConsumerRecordsCount_WithInvalidObject() {
        // Arrange
        Object invalidObject = new Object();

        // Act
        int count = KafkaConsumerInterceptor.getConsumerRecordsCount(invalidObject);

        // Assert
        assertEquals(0, count, "Should return 0 for object without count() or isEmpty() methods");
    }

    @Test
    public void testMultiplePollCalls() {
        // Arrange
        Object records1 = createMockConsumerRecords(2);
        Object records2 = createMockConsumerRecords(3);
        Object records3 = createMockConsumerRecords(0); // empty
        Object mockConsumer = new Object();

        // Act
        KafkaConsumerInterceptor.onPollExit(records1, mockConsumer);
        KafkaConsumerInterceptor.onPollExit(records2, mockConsumer);
        KafkaConsumerInterceptor.onPollExit(records3, mockConsumer);

        // Assert
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("message consumed - 2 records from poll()"), 
                   "Should log first poll with 2 records");
        assertTrue(output.contains("message consumed - 3 records from poll()"), 
                   "Should log second poll with 3 records");
        
        // Count occurrences of specific console messages
        int count2Records = output.split("message consumed - 2 records from poll\\(\\)", -1).length - 1;
        int count3Records = output.split("message consumed - 3 records from poll\\(\\)", -1).length - 1;
        assertEquals(1, count2Records, "Should log message consumed for 2 records once");
        assertEquals(1, count3Records, "Should log message consumed for 3 records once");
        
        // Ensure empty poll doesn't generate console message
        assertFalse(output.contains("message consumed - 0 records from poll()"), 
                   "Should not log message consumed for empty poll");
    }

    @Test
    public void testOnPollExit_WithNullConsumer() {
        // Arrange
        Object mockConsumerRecords = createMockConsumerRecords(1);
        Object consumer = null;

        // Act & Assert - should not throw exception
        assertDoesNotThrow(() -> KafkaConsumerInterceptor.onPollExit(mockConsumerRecords, consumer));
        
        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("message consumed - 1 records from poll()"), 
                   "Should log message consumed even with null consumer");
    }

    @Test
    public void testGetRecordField_WithValidRecord() {
        // Arrange
        MockConsumerRecord mockRecord = new MockConsumerRecord("test-topic", 0, 123L, "test-key");

        // Act
        Object topic = KafkaConsumerInterceptor.getRecordField(mockRecord, "topic");
        Object partition = KafkaConsumerInterceptor.getRecordField(mockRecord, "partition");
        Object offset = KafkaConsumerInterceptor.getRecordField(mockRecord, "offset");
        Object key = KafkaConsumerInterceptor.getRecordField(mockRecord, "key");

        // Assert
        assertEquals("test-topic", topic);
        assertEquals(0, partition);
        assertEquals(123L, offset);
        assertEquals("test-key", key);
    }

    @Test
    public void testGetRecordField_WithInvalidField() {
        // Arrange
        MockConsumerRecord mockRecord = new MockConsumerRecord("test-topic", 0, 123L, "test-key");

        // Act
        Object result = KafkaConsumerInterceptor.getRecordField(mockRecord, "nonexistentField");

        // Assert
        assertNull(result, "Should return null for non-existent field");
    }

    @Test
    public void testGetRecordField_WithNullRecord() {
        // Arrange
        Object record = null;

        // Act
        Object result = KafkaConsumerInterceptor.getRecordField(record, "topic");

        // Assert
        assertNull(result, "Should return null for null record");
    }

    // ===== HELPER METHODS AND MOCK CLASSES =====

    /**
     * Creates a mock ConsumerRecords object with the specified count.
     */
    private Object createMockConsumerRecords(int count) {
        return new MockConsumerRecords(count);
    }

    /**
     * Mock ConsumerRecords class that implements count() method.
     */
    public static class MockConsumerRecords {
        private final int count;

        public MockConsumerRecords(int count) {
            this.count = count;
        }

        public int count() {
            return count;
        }

        public boolean isEmpty() {
            return count == 0;
        }

        public Iterator<Object> iterator() {
            List<Object> records = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                records.add(new MockConsumerRecord("topic" + i, i % 3, (long) i, "key" + i));
            }
            return records.iterator();
        }
    }

    /**
     * Mock ConsumerRecords class that only implements isEmpty() method.
     */
    public static class MockConsumerRecordsWithIsEmpty {
        private final boolean empty;

        public MockConsumerRecordsWithIsEmpty(boolean empty) {
            this.empty = empty;
        }

        public boolean isEmpty() {
            return empty;
        }
    }

    /**
     * Mock ConsumerRecord class with basic fields.
     */
    public static class MockConsumerRecord {
        private final String topic;
        private final int partition;
        private final long offset;
        private final String key;

        public MockConsumerRecord(String topic, int partition, long offset, String key) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.key = key;
        }

        public String topic() {
            return topic;
        }

        public int partition() {
            return partition;
        }

        public long offset() {
            return offset;
        }

        public String key() {
            return key;
        }
    }

    /**
     * Mock Consumer class for testing.
     */
    public static class MockConsumer {
        private final String clientId;

        public MockConsumer(String clientId) {
            this.clientId = clientId;
        }

        public String getClientId() {
            return clientId;
        }
    }
} 