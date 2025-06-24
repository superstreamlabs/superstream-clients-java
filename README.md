<div align="center">

<img src="https://github.com/user-attachments/assets/35899c78-24eb-4507-97ed-e87e84c49fea#gh-dark-mode-only" width="300">
<img src="https://github.com/user-attachments/assets/8a7bca49-c362-4a8c-945e-a331fb26d8eb#gh-light-mode-only" width="300">

</div>

# Superstream Client For Java

A Java library for automatically optimizing Kafka producer configurations based on topic-specific recommendations.

## Overview

Superstream Clients works as a Java agent that intercepts Kafka producer creation and applies optimized configurations without requiring any code changes in your application. It dynamically retrieves optimization recommendations from Superstream and applies them based on impact analysis.

## Supported Libraries

Works with any Java library that depends on `kafka-clients`, including:

- Apache Kafka Clients
- Spring Kafka
- Alpakka Kafka (Akka Kafka)
- Pekko Kafka (Apache Pekko)
- Kafka Streams
- Kafka Connect
- Any custom wrapper around the Kafka Java client

## Features

- **Zero-code integration**: No code changes required in your application
- **Dynamic configuration**: Applies optimized settings based on topic-specific recommendations
- **Intelligent optimization**: Identifies the most impactful topics to optimize
- **Graceful fallback**: Falls back to default settings if optimization fails

## Java Version Compatibility

The library fully supports Java versions 11 through 21.

## Important: Producer Configuration Requirements

When initializing your Kafka producers, please ensure you pass the configuration as a mutable object. The Superstream library needs to modify the producer configuration to apply optimizations. The following initialization patterns are supported:

✅ **Supported (Recommended)**:
```java
// Using Properties (recommended)
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
// ... other properties ...
KafkaProducer<String, String> producer = new KafkaProducer<>(props);

// Using a regular HashMap
Map<String, Object> config = new HashMap<>();
config.put("bootstrap.servers", "localhost:9092");
// ... other properties ...
KafkaProducer<String, String> producer = new KafkaProducer<>(config);

// Using Spring's @Value annotations and configuration loading
@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    // ... other properties ...

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // ... other properties ...
        return new DefaultKafkaProducerFactory<>(configProps);
    }
}
```

❌ **Not Supported**:
```java
// Using Collections.unmodifiableMap
Map<String, Object> config = Collections.unmodifiableMap(new HashMap<>());
KafkaProducer<String, String> producer = new KafkaProducer<>(config);

// Using Map.of() (creates unmodifiable map)
KafkaProducer<String, String> producer = new KafkaProducer<>(
    Map.of("bootstrap.servers", "localhost:9092")
);

// Using KafkaTemplate's getProducerFactory().getConfigurationProperties()
// which returns an unmodifiable map
KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
KafkaProducer<String, String> producer = new KafkaProducer<>(
    template.getProducerFactory().getConfigurationProperties()
);
```

### Spring Applications
Spring applications that use `@Value` annotations and Spring's configuration loading (like `application.yml` or `application.properties`) are fully supported. The Superstream library will be able to modify the configuration when it's loaded into a mutable `Map` or `Properties` object in your Spring configuration class.

Example of supported Spring configuration:
```yaml
# application.yml
spring:
  kafka:
    producer:
      properties:
        compression.type: snappy
        batch.size: 16384
        linger.ms: 1
```

```java
@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.producer.properties.compression.type}")
    private String compressionType;
    
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
}
```

### Pekko/Akka Kafka Applications
Pekko and Akka Kafka applications typically use immutable configuration maps internally, which prevents Superstream from applying optimizations. To enable Superstream optimizations with Pekko/Akka, you need to create the KafkaProducer manually with a mutable configuration.

✅ **Superstream-optimized pattern**:
```java
// Add these lines to create a mutable producer
Map<String, Object> configProps = new HashMap<>();
configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

org.apache.kafka.clients.producer.Producer<String, String> kafkaProducer = new KafkaProducer<>(configProps);

ProducerSettings<String, String> producerSettings = ProducerSettings
    .create(system, new StringSerializer(), new StringSerializer())
    .withProducer(kafkaProducer);

Source.single(ProducerMessage.single(record))
    .via(Producer.flexiFlow(producerSettings))
    .runWith(Sink.ignore, system);
```

❌ **Native Pekko/Akka pattern (optimizations won't be applied)**:
```java
ProducerSettings<String, String> producerSettings = ProducerSettings
    .create(system, new StringSerializer(), new StringSerializer())
    .withBootstrapServers("localhost:9092");

Source.single(ProducerMessage.single(record))
    .via(Producer.flexiFlow(producerSettings))
    .runWith(Sink.ignore, system);
```

### Why This Matters
The Superstream library needs to modify your producer's configuration to apply optimizations based on your cluster's characteristics. This includes adjusting settings like compression, batch size, and other performance parameters. When the configuration is immutable, these optimizations cannot be applied.

## Installation

*Superstream package*: https://central.sonatype.com/artifact/ai.superstream/superstream-clients-java/overview

### Step 1: Add Superstream Jar to your application

#### Download from GitHub

https://github.com/superstreamlabs/superstream-clients-java/releases

#### Available also in Maven Central

https://central.sonatype.com/artifact/ai.superstream/superstream-clients

### Step 2: Run

Add the Java agent to your application's startup command:

```bash
java -javaagent:/path/to/superstream-clients-1.0.16.jar -jar your-application.jar
```

### Docker Integration

When using Superstream Clients with containerized applications, include the agent in your Dockerfile:

```dockerfile
FROM openjdk:11-jre

WORKDIR /app

# Copy your application
COPY target/your-application.jar app.jar

# Copy the Superstream agent
COPY path/to/superstream-clients-1.0.16.jar superstream-agent.jar

# Run with the Java agent
ENTRYPOINT ["java", "-javaagent:/app/superstream-agent.jar", "-jar", "/app/app.jar"]
```

Alternatively, you can use a multi-stage build to download the agent from Maven Central:


### Required Environment Variables

- `SUPERSTREAM_TOPICS_LIST`: Comma-separated list of topics your application produces to

### Optional Environment Variables

- `SUPERSTREAM_LATENCY_SENSITIVE`: Set to "true" to prevent any modification to linger.ms values
- `SUPERSTREAM_DISABLED`: Set to "true" to disable optimization
- `SUPERSTREAM_DEBUG`: Set to "true" to enable debug logs

Example:
```bash
export SUPERSTREAM_TOPICS_LIST=orders,payments,user-events
export SUPERSTREAM_LATENCY_SENSITIVE=true
```

## Prerequisites

- Java 11 or higher
- Kafka cluster that is connected to the Superstream's console
- Read and write permissions to the `superstream.*` topics

## License

This project is licensed under the Apache License 2.0.
