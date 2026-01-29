package ai.superstream.agent;

import ai.superstream.util.SuperstreamLogger;
import net.bytebuddy.asm.Advice;

/**
 * Intercepts KafkaConsumer constructor calls and poll() method to collect metrics and log consumption.
 */
public class KafkaConsumerInterceptor {
    public static final SuperstreamLogger logger = SuperstreamLogger.getLogger(KafkaConsumerInterceptor.class);

    // Environment variable to check if Superstream is disabled
    private static final String DISABLED_ENV_VAR = "SUPERSTREAM_DISABLED";
    public static final boolean DISABLED = Boolean.parseBoolean(System.getenv(DISABLED_ENV_VAR));

    /**
     * Check if Superstream is disabled via environment variable.
     * 
     * @return true if Superstream is disabled, false otherwise
     */
    public static boolean isDisabled() {
        return DISABLED;
    }

    /**
     * Called before the KafkaConsumer constructor.
     * Used to intercept consumer creation.
     *
     * @param args The consumer arguments
     */
    @Advice.OnMethodEnter
    public static void onEnter(@Advice.AllArguments Object[] args) {
        // Skip if Superstream is disabled via environment variable
        if (isDisabled()) {
            return;
        }

        try {
            logger.info("hello world");
            System.out.println("hello world - KafkaConsumer intercepted!");
        } catch (Exception e) {
            logger.error("Error in KafkaConsumer interceptor: " + e.getMessage(), e);
        }
    }

    /**
     * Called after the KafkaConsumer constructor.
     * Used to register the consumer for future processing.
     * 
     * @param consumer The KafkaConsumer instance that was just created
     */
    @Advice.OnMethodExit
    public static void onExit(@Advice.This Object consumer) {
        // Skip if Superstream is disabled via environment variable
        if (isDisabled()) {
            return;
        }

        try {
            if (consumer != null) {
                logger.debug("KafkaConsumer constructor completed for: {}", consumer.getClass().getName());
            } else {
                logger.debug("KafkaConsumer constructor completed (consumer is null)");
            }
        } catch (Exception e) {
            logger.error("Error in KafkaConsumer exit interceptor: " + e.getMessage(), e);
        }
    }

    /**
     * Called after the KafkaConsumer poll() method.
     * Used to log consumed messages.
     * 
     * @param result The ConsumerRecords returned by poll()
     * @param consumer The KafkaConsumer instance
     */
    @Advice.OnMethodExit
    public static void onPollExit(@Advice.Return Object result, @Advice.This Object consumer) {
        // Skip if Superstream is disabled via environment variable
        if (isDisabled()) {
            return;
        }

        try {
            if (result != null) {
                // Get the count of messages using reflection
                int messageCount = getConsumerRecordsCount(result);
                
                if (messageCount > 0) {
                    logger.info("message consumed");
                    System.out.println("message consumed - " + messageCount + " records from poll()");
                    
                    // Log detailed information if debug is enabled
                    if (logger.isDebugEnabled()) {
                        logConsumerRecordsDetails(result, consumer);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in KafkaConsumer poll interceptor: " + e.getMessage(), e);
        }
    }

    /**
     * Get the count of records in ConsumerRecords using reflection.
     */
    static int getConsumerRecordsCount(Object consumerRecords) {
        try {
            // Try to call count() method
            java.lang.reflect.Method countMethod = consumerRecords.getClass().getMethod("count");
            Object countResult = countMethod.invoke(consumerRecords);
            if (countResult instanceof Integer) {
                return (Integer) countResult;
            }
        } catch (Exception e) {
            logger.debug("Could not get record count via count() method: {}", e.getMessage());
        }

        try {
            // Try to call isEmpty() method and return 0 if empty, 1 if not empty
            java.lang.reflect.Method isEmptyMethod = consumerRecords.getClass().getMethod("isEmpty");
            Object isEmptyResult = isEmptyMethod.invoke(consumerRecords);
            if (isEmptyResult instanceof Boolean) {
                return ((Boolean) isEmptyResult) ? 0 : 1; // If not empty, assume at least 1 record
            }
        } catch (Exception e) {
            logger.debug("Could not check isEmpty(): {}", e.getMessage());
        }

        return 0;
    }

    /**
     * Log detailed information about consumed records (debug mode only).
     */
    private static void logConsumerRecordsDetails(Object consumerRecords, Object consumer) {
        try {
            // Get consumer client ID for logging context
            String clientId = getConsumerClientId(consumer);
            
            // Try to iterate through records for detailed logging
            java.lang.reflect.Method iteratorMethod = consumerRecords.getClass().getMethod("iterator");
            Object iterator = iteratorMethod.invoke(consumerRecords);
            
            if (iterator instanceof java.util.Iterator) {
                @SuppressWarnings("unchecked")
                java.util.Iterator<Object> recordIterator = (java.util.Iterator<Object>) iterator;
                
                int recordIndex = 0;
                while (recordIterator.hasNext() && recordIndex < 5) { // Log max 5 records to avoid spam
                    Object record = recordIterator.next();
                    recordIndex++;
                    
                    try {
                        Object topicObj = getRecordField(record, "topic");
                        Object partitionObj = getRecordField(record, "partition");
                        Object offsetObj = getRecordField(record, "offset");
                        Object key = getRecordField(record, "key");
                        
                        String topic = topicObj != null ? topicObj.toString() : null;
                        Integer partition = partitionObj instanceof Integer ? (Integer) partitionObj : null;
                        Long offset = offsetObj instanceof Long ? (Long) offsetObj : null;
                        
                        logger.debug("Consumer {} consumed record {}: topic={}, partition={}, offset={}, key={}", 
                                   clientId, recordIndex, topic, partition, offset, key);
                    } catch (Exception e) {
                        logger.debug("Could not extract record details for record {}: {}", recordIndex, e.getMessage());
                    }
                }
                
                if (recordIterator.hasNext()) {
                    logger.debug("... and more records (showing only first 5)");
                }
            }
        } catch (Exception e) {
            logger.debug("Could not log consumer records details: {}", e.getMessage());
        }
    }

    /**
     * Get consumer client ID for logging context.
     */
    private static String getConsumerClientId(Object consumer) {
        try {
            // Try to get clientId from the consumer
            java.lang.reflect.Field clientIdField = findField(consumer.getClass(), "clientId");
            if (clientIdField != null) {
                clientIdField.setAccessible(true);
                Object clientId = clientIdField.get(consumer);
                return clientId != null ? clientId.toString() : "unknown";
            }
        } catch (Exception e) {
            logger.debug("Could not get client ID: {}", e.getMessage());
        }
        return "unknown";
    }

    /**
     * Get a field value from a ConsumerRecord using reflection.
     */
    static Object getRecordField(Object record, String fieldName) {
        try {
            java.lang.reflect.Method method = record.getClass().getMethod(fieldName);
            return method.invoke(record);
        } catch (Exception e) {
            logger.debug("Could not get field {} from record: {}", fieldName, e.getMessage());
            return null;
        }
    }

    /**
     * Find a field in a class or its superclasses.
     */
    private static java.lang.reflect.Field findField(Class<?> clazz, String fieldName) {
        if (clazz == null || clazz == Object.class) {
            return null;
        }

        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            return findField(clazz.getSuperclass(), fieldName);
        }
    }
} 