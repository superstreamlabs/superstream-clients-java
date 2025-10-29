package ai.superstream.util;

/**
 * Custom logger for the Superstream library that falls back to System.out/System.err.
 * 
 * This logger provides a simple, lightweight logging solution that doesn't require
 * external logging frameworks. It supports:
 * - Standard log levels: DEBUG, INFO, WARN, ERROR
 * - Fluent API for structured logging with client ID and topic context
 * - Placeholder-based message formatting using {} syntax
 * - Automatic exception formatting with stack traces
 * 
 * Debug logging can be enabled via:
 * - System property: superstream.debug=true
 * - Environment variable: SUPERSTREAM_DEBUG=true
 * - Programmatically: setDebugEnabled(boolean)
 */
public class SuperstreamLogger {
    private static final String PREFIX = "superstream";
    private final String className;
    // Flag to control debug logging - default to false to hide debug logs
    // Volatile for thread-safe visibility across threads
    private static volatile boolean debugEnabled = false;

    static {
        // Check if debug logging is enabled via system property or environment variable
        String debugFlag = System.getProperty("superstream.debug");
        if (debugFlag == null) {
            debugFlag = System.getenv("SUPERSTREAM_DEBUG");
        }
        debugEnabled = "true".equalsIgnoreCase(debugFlag);
    }

    /**
     * Enable or disable debug logging programmatically.
     * Changes take effect immediately across all logger instances.
     *
     * @param enabled true to enable debug logging, false to disable
     */
    public static void setDebugEnabled(boolean enabled) {
        debugEnabled = enabled;
    }

    private SuperstreamLogger(Class<?> clazz) {
        this.className = clazz.getSimpleName();
    }

    /**
     * Get a logger for the specified class.
     *
     * @param clazz The class to get the logger for
     * @return A new SuperstreamLogger instance
     */
    public static SuperstreamLogger getLogger(Class<?> clazz) {
        return new SuperstreamLogger(clazz);
    }

    /**
     * Log an info message.
     *
     * @param message The message to log, must not be null
     */
    public void info(String message) {
        if (message == null) return;
        System.out.println(formatLogMessage("INFO", message));
    }

    /**
     * Log an info message with parameters.
     * Placeholders {} in the message will be replaced with the provided arguments.
     *
     * @param message The message template with {} placeholders, must not be null
     * @param args Arguments to replace placeholders in the message
     */
    public void info(String message, Object... args) {
        if (message == null) return;
        System.out.println(formatLogMessage("INFO", formatArgs(message, args)));
    }

    /**
     * Log a warning message.
     *
     * @param message The message to log, must not be null
     */
    public void warn(String message) {
        if (message == null) return;
        System.out.println(formatLogMessage("WARN", message));
    }

    /**
     * Log a warning message with parameters.
     * Placeholders {} in the message will be replaced with the provided arguments.
     *
     * @param message The message template with {} placeholders, must not be null
     * @param args Arguments to replace placeholders in the message
     */
    public void warn(String message, Object... args) {
        if (message == null) return;
        System.out.println(formatLogMessage("WARN", formatArgs(message, args)));
    }

    /**
     * Log an error message.
     *
     * @param message The message to log, must not be null
     */
    public void error(String message) {
        if (message == null) return;
        System.err.println(formatLogMessage("ERROR", message));
    }

    /**
     * Log an error message with parameters.
     * If the last argument is a Throwable, it will be formatted with stack trace.
     * Placeholders {} in the message will be replaced with the provided arguments.
     *
     * @param message The message template with {} placeholders, must not be null
     * @param args Arguments to replace placeholders in the message. If the last argument
     *             is a Throwable, it will be formatted with stack trace
     */
    public void error(String message, Object... args) {
        if (message == null) return;
        if (args != null && args.length > 0 && args[args.length - 1] instanceof Throwable) {
            // If the last argument is a Throwable, format it properly
            Throwable throwable = (Throwable) args[args.length - 1];
            // Remove the Throwable from args array
            Object[] messageArgs = new Object[args.length - 1];
            System.arraycopy(args, 0, messageArgs, 0, args.length - 1);
            // Format the message with the remaining args
            String formattedMessage = formatArgs(message, messageArgs);
            // Format the exception message
            String formattedExceptionMessage = formatExceptionMessage(formattedMessage, throwable);
            System.err.println(formatLogMessage("ERROR", formattedExceptionMessage));
        } else {
            System.err.println(formatLogMessage("ERROR", formatArgs(message, args)));
        }
    }

    /**
     * Log a debug message.
     * Debug messages are only logged if debug logging is enabled.
     *
     * @param message The message to log, must not be null
     */
    public void debug(String message) {
        if (debugEnabled && message != null) {
            System.out.println(formatLogMessage("DEBUG", message));
        }
    }

    /**
     * Log a debug message with parameters.
     * Debug messages are only logged if debug logging is enabled.
     * Placeholders {} in the message will be replaced with the provided arguments.
     *
     * @param message The message template with {} placeholders, must not be null
     * @param args Arguments to replace placeholders in the message
     */
    public void debug(String message, Object... args) {
        if (debugEnabled && message != null) {
            System.out.println(formatLogMessage("DEBUG", formatArgs(message, args)));
        }
    }

    /**
     * Get a fluent logger builder for debug level.
     *
     * @return A Fluent logger instance for chaining
     */
    public Fluent debug() { return new Fluent(this, Level.DEBUG); }

    /**
     * Get a fluent logger builder for info level.
     *
     * @return A Fluent logger instance for chaining
     */
    public Fluent info() { return new Fluent(this, Level.INFO); }

    /**
     * Get a fluent logger builder for warn level.
     *
     * @return A Fluent logger instance for chaining
     */
    public Fluent warn() { return new Fluent(this, Level.WARN); }

    /**
     * Get a fluent logger builder for error level.
     *
     * @return A Fluent logger instance for chaining
     */
    public Fluent error() { return new Fluent(this, Level.ERROR); }

    /**
     * Check if debug logging is currently enabled.
     *
     * @return true if debug logging is enabled, false otherwise
     */
    public static boolean isDebugEnabled() {
        return debugEnabled;
    }

    /**
     * Format a log message with the Superstream prefix and class name.
     *
     * @param level The log level (DEBUG, INFO, WARN, ERROR)
     * @param message The message to format, must not be null
     * @return Formatted log message
     */
    private String formatLogMessage(String level, String message) {
        if (message == null) {
            message = "null";
        }
        return String.format("[%s][%s][%s] %s", PREFIX, level, className, message);
    }

    /**
     * Replace placeholder {} with actual values.
     * This method safely handles null messages and arguments.
     *
     * @param message The message template with {} placeholders, may be null
     * @param args Arguments to replace placeholders, may be null
     * @return The formatted message with placeholders replaced
     */
    String formatArgs(String message, Object... args) {
        if (message == null) {
            return "null";
        }
        if (args == null || args.length == 0) {
            return message;
        }

        String result = message;
        for (Object arg : args) {
            int idx = result.indexOf("{}");
            if (idx >= 0) {
                String argStr = (arg == null) ? "null" : String.valueOf(arg);
                result = result.substring(0, idx) + argStr + result.substring(idx + 2);
            } else {
                // No more placeholders found - stop processing
                break;
            }
        }
        return result;
    }

    /**
     * Log level enumeration.
     */
    public enum Level { DEBUG, INFO, WARN, ERROR }

    /**
     * Fluent API for structured logging with context (client ID, topic).
     * Allows chaining of context methods before logging the message.
     * 
     * Example usage:
     * logger.info().withClientId("my-client").forTopic("my-topic").msg("Message with {}", arg);
     */
    public static final class Fluent {
        private final SuperstreamLogger parent;
        private final Level level;
        private String clientId;
        private String topic;

        Fluent(SuperstreamLogger parent, Level level) {
            this.parent = parent;
            this.level = level;
        }

        /**
         * Add client ID context to the log message.
         *
         * @param clientId The client ID to include in the log message
         * @return This Fluent instance for method chaining
         */
        public Fluent withClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        /**
         * Add topic context to the log message.
         *
         * @param topic The topic name to include in the log message
         * @return This Fluent instance for method chaining
         */
        public Fluent forTopic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         * Build the context prefix from client ID and topic.
         *
         * @return The formatted prefix string, or empty string if no context
         */
        private String prefix() {
            if ((clientId == null || clientId.isEmpty()) && (topic == null || topic.isEmpty())) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            if (clientId != null && !clientId.isEmpty()) {
                sb.append("[client: ").append(clientId).append("] ");
            }
            if (topic != null && !topic.isEmpty()) {
                sb.append("[topic: ").append(topic).append("] ");
            }
            return sb.toString();
        }

        /**
         * Log a message with the configured context.
         *
         * @param message The message to log, must not be null
         */
        public void msg(String message) {
            if (message == null) return;
            String m = prefix() + message;
            switch (level) {
                case DEBUG:
                    parent.debug(m);
                    break;
                case INFO:
                    parent.info(m);
                    break;
                case WARN:
                    parent.warn(m);
                    break;
                case ERROR:
                    parent.error(m);
                    break;
            }
        }

        /**
         * Log a message with parameters and the configured context.
         * Placeholders {} in the message will be replaced with the provided arguments.
         *
         * @param message The message template with {} placeholders, must not be null
         * @param args Arguments to replace placeholders in the message
         */
        public void msg(String message, Object... args) {
            if (message == null) return;
            String m = prefix() + parent.formatArgs(message, args);
            switch (level) {
                case DEBUG:
                    parent.debug(m);
                    break;
                case INFO:
                    parent.info(m);
                    break;
                case WARN:
                    parent.warn(m);
                    break;
                case ERROR:
                    parent.error(m);
                    break;
            }
        }
    }

    /**
     * Format an exception message with class name, message and stack trace.
     * This is a standardized way to format exception messages across the codebase.
     * We format the stack trace on a single line because some logging systems
     * don't include multi-line stack traces unless they appear without newlines.
     *
     * @param message The base error message, must not be null
     * @param throwable The exception to format, must not be null
     * @return Formatted exception message with stack trace
     */
    private String formatExceptionMessage(String message, Throwable throwable) {
        if (throwable == null) {
            return message;
        }
        // Convert stack trace to string
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        throwable.printStackTrace(pw);
        String stackTrace = sw.toString().replaceAll("\\r?\\n", " ");
        
        String exceptionMessage = throwable.getMessage();
        if (exceptionMessage == null) {
            exceptionMessage = "(no message)";
        }
        
        return String.format("%s. Error: %s - %s. Stack trace: %s",
            message,
            throwable.getClass().getName(),
            exceptionMessage,
            stackTrace);
    }
}