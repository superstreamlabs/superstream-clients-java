package ai.superstream.examples.shadow;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class CustomStringSerializer implements Serializer<String> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public byte[] serialize(String topic, String data) {
        if (data == null) return null;
        return data.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        // no-op
    }
}


