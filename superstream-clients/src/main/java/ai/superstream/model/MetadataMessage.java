package ai.superstream.model;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a message from the superstream.metadata_v1 topic.
 */
public class MetadataMessage {
    private int superstreamClusterId;
    private boolean active;
    private List<TopicConfiguration> topicsConfiguration;

    // Optional: override for client stats reporting interval (milliseconds). Can be absent (null)
    @JsonProperty("report_interval_ms")
    private Long reportIntervalMs;

    // Optional: agent learning period (minutes). If absent, agent will use default.
    @JsonProperty("agent_learn_period_mins")
    private Long agentLearnPeriodMins;

    public MetadataMessage() {
        // Default constructor for Jackson
    }

    public MetadataMessage(int superstreamClusterId, boolean active, List<TopicConfiguration> topicsConfiguration) {
        this.superstreamClusterId = superstreamClusterId;
        this.active = active;
        this.topicsConfiguration = topicsConfiguration;
    }

    @JsonProperty("superstream_cluster_id")
    public int getSuperstreamClusterId() {
        return superstreamClusterId;
    }

    @JsonProperty("superstream_cluster_id")
    public void setSuperstreamClusterId(int superstreamClusterId) {
        this.superstreamClusterId = superstreamClusterId;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @JsonProperty("topics_configuration")
    public List<TopicConfiguration> getTopicsConfiguration() {
        return topicsConfiguration;
    }

    @JsonProperty("topics_configuration")
    public void setTopicsConfiguration(List<TopicConfiguration> topicsConfiguration) {
        this.topicsConfiguration = topicsConfiguration;
    }

    // Getter & setter for new report interval field
    @JsonProperty("report_interval_ms")
    public Long getReportIntervalMs() {
        return reportIntervalMs;
    }

    @JsonProperty("report_interval_ms")
    public void setReportIntervalMs(Long reportIntervalMs) {
        this.reportIntervalMs = reportIntervalMs;
    }

    @JsonProperty("agent_learn_period_mins")
    public Long getAgentLearnPeriodMins() {
        return agentLearnPeriodMins;
    }

    @JsonProperty("agent_learn_period_mins")
    public void setAgentLearnPeriodMins(Long agentLearnPeriodMins) {
        this.agentLearnPeriodMins = agentLearnPeriodMins;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetadataMessage that = (MetadataMessage) o;
        return superstreamClusterId == that.superstreamClusterId &&
                active == that.active &&
                Objects.equals(topicsConfiguration, that.topicsConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(superstreamClusterId, active, topicsConfiguration);
    }

    @Override
    public String toString() {
        return "MetadataMessage{" +
                "superstream_cluster_id=" + superstreamClusterId +
                ", active=" + active +
                ", topics_configuration=" + topicsConfiguration +
                '}';
    }
}