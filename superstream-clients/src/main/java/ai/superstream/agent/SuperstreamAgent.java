package ai.superstream.agent;

import java.lang.instrument.Instrumentation;
import java.util.HashMap;
import java.util.Map;

import ai.superstream.util.SuperstreamLogger;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;

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
        logger.info().msg("Superstream Agent initialized with environment variables: {}", superstreamEnvVars);
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
        logger.info().msg("Superstream Agent initialized (dynamic attach) with environment variables: {}", superstreamEnvVars);
    }

    /**
     * Install the agent instrumentation.
     *
     * @param instrumentation Instrumentation instance
     */
    private static void install(Instrumentation instrumentation) {
        // Intercept KafkaProducer to:
        // - initialize shadow routing (send/flush/close) so we can hot-swap the underlying sender,
        // - register/deregister producers for metrics collection (constructor/close) to drive per-producer optimization.
        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .with(new AgentBuilder.Listener() {
                    @Override
                    public void onDiscovery(String typeName, ClassLoader classLoader, JavaModule module, boolean loaded) {}
                    @Override
                    public void onTransformation(TypeDescription typeDescription, ClassLoader classLoader, JavaModule module, boolean loaded, DynamicType dynamicType) {
                        logger.info().msg("Transformed {} for shadow routing", typeDescription.getName());
                    }
                    @Override
                    public void onIgnored(TypeDescription typeDescription, ClassLoader classLoader, JavaModule module, boolean loaded) {}
                    @Override
                    public void onError(String typeName, ClassLoader classLoader, JavaModule module, boolean loaded, Throwable throwable) {
                        logger.error().msg("[ERR-601] Instrumentation error for {}: {}", typeName, throwable);
                    }
                    @Override
                    public void onComplete(String typeName, ClassLoader classLoader, JavaModule module, boolean loaded) {}
                })
                .type(ElementMatchers.nameEndsWith(".KafkaProducer")
                        .and(ElementMatchers.not(ElementMatchers.nameContains("ai.superstream")))) // prevent instrumenting superstream's own KafkaProducer
                .transform((builder, td, cl, module, pd) -> builder
                        // Capture properties + register producer for metrics on constructor
                        .visit(Advice.to(KafkaProducerInterceptor.class)
                                .on(ElementMatchers.isConstructor()))
                        // route send(..) overloads
                        .visit(Advice.to(KafkaProducerShadowAdvice.SendAdvice.class)
                                .on(ElementMatchers.named("send")))
                        // route flush()
                        .visit(Advice.to(KafkaProducerShadowAdvice.FlushAdvice.class)
                                .on(ElementMatchers.named("flush")))
                        // route transactional APIs
                        .visit(Advice.to(KafkaProducerShadowAdvice.InitTransactionsAdvice.class)
                                .on(ElementMatchers.named("initTransactions")))
                        .visit(Advice.to(KafkaProducerShadowAdvice.BeginTransactionAdvice.class)
                                .on(ElementMatchers.named("beginTransaction")))
                        .visit(Advice.to(KafkaProducerShadowAdvice.CommitTransactionAdvice.class)
                                .on(ElementMatchers.named("commitTransaction")))
                        .visit(Advice.to(KafkaProducerShadowAdvice.AbortTransactionAdvice.class)
                                .on(ElementMatchers.named("abortTransaction")))
                        .visit(Advice.to(KafkaProducerShadowAdvice.SendOffsetsToTransactionAdvice.class)
                                .on(ElementMatchers.named("sendOffsetsToTransaction")))
                        // log-only for read-only helpers
                        .visit(Advice.to(KafkaProducerShadowAdvice.MetricsAdvice.class)
                                .on(ElementMatchers.named("metrics")))
                        .visit(Advice.to(KafkaProducerShadowAdvice.PartitionsForAdvice.class)
                                .on(ElementMatchers.named("partitionsFor")))
                        // mark closed for metrics registry on close(..)
                        .visit(Advice.to(KafkaProducerCloseInterceptor.class)
                                .on(ElementMatchers.named("close")))
                        // route close(..) to shadow
                        .visit(Advice.to(KafkaProducerShadowAdvice.CloseAdvice.class)
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