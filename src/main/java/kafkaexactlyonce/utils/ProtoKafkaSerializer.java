package kafkaexactlyonce.utils;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.google.protobuf.GeneratedMessageV3;

public class ProtoKafkaSerializer<T extends GeneratedMessageV3> implements Serializer<T> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override public byte[] serialize(String topic, T data) {
        return data.toByteArray();
    }

    @Override public void close() {}
}
