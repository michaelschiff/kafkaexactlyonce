package kafkaexactlyonce.utils;

public class ProducerState {
    private String producerId;
    private long offset;

    public ProducerState(String producerState) {
        String[] split = producerState.split(":");
        this.producerId = split[0];
        this.offset = Long.valueOf(split[1]);
    }

    public ProducerState(String producerId, long offset) {
        this.producerId = producerId;
        this.offset = offset;
    }

    public String toString() {
        return producerId + ":" + offset;
    }

    public String getProducerId() {
        return producerId;
    }

    public long getOffset() {
        return offset;
    }
}
