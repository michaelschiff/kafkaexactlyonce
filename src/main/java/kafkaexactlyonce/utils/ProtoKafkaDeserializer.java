package kafkaexactlyonce.utils;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;

public class ProtoKafkaDeserializer<T extends GeneratedMessageV3> implements Deserializer<T> {

    private final Parser<T> parser;

    public ProtoKafkaDeserializer(Parser<T> parser) {
        this.parser = parser;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return parser.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    @Override
    public void close() {}
}
