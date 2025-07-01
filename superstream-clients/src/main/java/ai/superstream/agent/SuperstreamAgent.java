package ai.superstream.agent;

import ai.superstream.util.SuperstreamLogger;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.instrument.Instrumentation;
import java.util.HashMap;
import java.util.Map;

/**
 * Java agent entry point for the Superstream library.
 */
public class SuperstreamAgent {
    public static final SuperstreamLogger logger = SuperstreamLogger.getLogger(SuperstreamAgent.class);

    /**
     * Premain method, called when the agent is loaded during JVM startup.
     *
     * @param arguments       Agent arguments
     * @param instrumentation Instrumentation instance
     */
    public static void premain(String arguments, Instrumentation instrumentation) {
        // Check environment variable
        String debugEnv = System.getenv("SUPERSTREAM_DEBUG");
        if ("true".equalsIgnoreCase(debugEnv)) {
            SuperstreamLogger.setDebugEnabled(true);
        }

        install(instrumentation);

        // Log all SUPERSTREAM_ environment variables
        Map<String, String> superstreamEnvVars = new HashMap<>();
        System.getenv().forEach((key, value) -> {
            if (key.startsWith("SUPERSTREAM_")) {
                superstreamEnvVars.put(key, value);
            }
        });
        logger.info("Superstream Agent initialized with environment variables: {}", superstreamEnvVars);
    }

    /**
     * AgentMain method, called when the agent is loaded after JVM startup.
     *
     * @param arguments       Agent arguments
     * @param instrumentation Instrumentation instance
     */
    public static void agentmain(String arguments, Instrumentation instrumentation) {
        install(instrumentation);

        // Log all SUPERSTREAM_ environment variables
        Map<String, String> superstreamEnvVars = new HashMap<>();
        System.getenv().forEach((key, value) -> {
            if (key.startsWith("SUPERSTREAM_")) {
                superstreamEnvVars.put(key, value);
            }
        });
        logger.info("Superstream Agent initialized (dynamic attach) with environment variables: {}", superstreamEnvVars);
    }

    /**
     * Install the agent instrumentation.
     *
     * @param instrumentation Instrumentation instance
     */
    private static void install(Instrumentation instrumentation) {
        // Intercept KafkaProducer constructor for both configuration optimization and
        // metrics collection
        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .type(ElementMatchers.nameEndsWith(".KafkaProducer")
                        .and(ElementMatchers.not(ElementMatchers.nameContains("ai.superstream")))) // prevent instrumenting superstream's own KafkaProducer
                .transform((builder, td, cl, module, pd) -> builder
                        .visit(Advice.to(KafkaProducerInterceptor.class)
                                .on(ElementMatchers.isConstructor()))
                        .visit(Advice.to(KafkaProducerCloseInterceptor.class)
                                .on(ElementMatchers.named("close"))))
                .installOn(instrumentation);

        // Intercept KafkaConsumer constructor and poll() method for metrics collection
        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .type(ElementMatchers.nameEndsWith("KafkaConsumer"))
                .transform((builder, typeDescription, classLoader, module,
                        protectionDomain) -> builder
                            .visit(Advice.to(KafkaConsumerInterceptor.class)
                                .on(ElementMatchers.isConstructor()))
                            .visit(Advice.to(KafkaConsumerInterceptor.class)
                                .on(ElementMatchers.named("poll"))))
                .installOn(instrumentation);

        logger.info("Superstream Agent successfully installed instrumentation");
    }
}