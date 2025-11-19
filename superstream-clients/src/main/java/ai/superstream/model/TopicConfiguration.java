package ai.superstream.model;

import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the configuration for a specific Kafka topic.
 */
public class TopicConfiguration {
    private String topicName;
    private int potentialReductionPercentage;
    private long dailyWritesBytes;
    private Map<String, Object> optimizedConfiguration;

    public TopicConfiguration() {
        // Default constructor for Jackson
    }

    public TopicConfiguration(String topicName, int potentialReductionPercentage, long dailyWritesBytes,
                              Map<String, Object> optimizedConfiguration) {
        this.topicName = topicName;
        this.potentialReductionPercentage = potentialReductionPercentage;
        this.dailyWritesBytes = dailyWritesBytes;
        this.optimizedConfiguration = optimizedConfiguration;
    }

    @JsonProperty("topic_name")
    public String getTopicName() {
        return topicName;
    }

    @JsonProperty("topic_name")
    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    @JsonProperty("potential_reduction_percentage")
    public int getPotentialReductionPercentage() {
        return potentialReductionPercentage;
    }

    @JsonProperty("potential_reduction_percentage")
    public void setPotentialReductionPercentage(int potentialReductionPercentage) {
        this.potentialReductionPercentage = potentialReductionPercentage;
    }

    @JsonProperty("daily_writes_bytes")
    public long getDailyWritesBytes() {
        return dailyWritesBytes;
    }

    @JsonProperty("daily_writes_bytes")
    public void setDailyWritesBytes(long dailyWritesBytes) {
        this.dailyWritesBytes = dailyWritesBytes;
    }

    @JsonProperty("optimized_configuration")
    public Map<String, Object> getOptimizedConfiguration() {
        return optimizedConfiguration;
    }

    @JsonProperty("optimized_configuration")
    public void setOptimizedConfiguration(Map<String, Object> optimizedConfiguration) {
        this.optimizedConfiguration = optimizedConfiguration;
    }

    /**
     * Calculate the potential impact of optimization for this topic.
     * @return The impact score, calculated as potentialReductionPercentage * dailyWritesBytes
     */
    public long calculateImpactScore() {
        return (long) (potentialReductionPercentage/100) * dailyWritesBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicConfiguration that = (TopicConfiguration) o;
        return potentialReductionPercentage == that.potentialReductionPercentage &&
                dailyWritesBytes == that.dailyWritesBytes &&
                Objects.equals(topicName, that.topicName) &&
                Objects.equals(optimizedConfiguration, that.optimizedConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, potentialReductionPercentage, dailyWritesBytes, optimizedConfiguration);
    }

    @Override
    public String toString() {
        return "TopicConfiguration{" +
                "topic_name='" + topicName + '\'' +
                ", potential_reduction_percentage=" + potentialReductionPercentage +
                ", daily_writes_bytes=" + dailyWritesBytes +
                ", optimized_configuration=" + optimizedConfiguration +
                '}';
    }
}