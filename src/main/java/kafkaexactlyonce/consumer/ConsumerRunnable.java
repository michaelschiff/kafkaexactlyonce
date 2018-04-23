package kafkaexactlyonce.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafkaexactlyonce.Event;
import kafkaexactlyonce.ProducerState;

public class ConsumerRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunnable.class);

    private final KafkaConsumer<ProducerState, Event> consumer;
    private final String statePath;
    private final List<TopicPartition> partitions;
    private final CuratorFramework zk;
    private final ObjectMapper mapper = new ObjectMapper();

    public ConsumerRunnable(KafkaConsumer consumer, String statePath, List<TopicPartition> partitions,
            CuratorFramework zk) {
        this.consumer = consumer;
        this.statePath = statePath;
        this.partitions = partitions;
        this.zk = zk;
    }

    @Override
    public void run() {

        // Assign the partitions to our consumer so that polls fetch from these partitions
        consumer.assign(partitions);
        try {
            Map<TopicPartition, State> states = initializeState();
            for (TopicPartition topicPartition : states.keySet()) {
                consumer.seek(topicPartition, states.get(topicPartition).offset);
            }
            boolean emptyPoll = false;
            while (!emptyPoll) {
                emptyPoll = true;
                for (ConsumerRecord<ProducerState, Event> record : consumer.poll(1000)) {
                    emptyPoll = false;
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    State state = states.get(topicPartition);
                    Event value = record.value();
                    // Increment our aggregates based on this record value
                    Integer current = state.aggregates.getOrDefault(value.getMessage(), 0);
                    state.aggregates.put(value.getMessage(), current + 1);
                    // Update the offset for this partition to the next offset we should fetch from
                    state.offset = record.offset() + 1;
                }
                commitState(states);
            }
        } catch (Throwable t) {
            LOGGER.error("Error in consumer thread", t);
        }
    }

    /**
     * Initialize state - in this example we are always starting from scratch so no state will be found,
     * but in general we can expect that there will be prior consumer state.
     * @return initialized map of states
     */
    private Map<TopicPartition, State> initializeState() throws Exception {
        Map<TopicPartition, State> states = new HashMap<>();
        for (TopicPartition topicPartition : partitions) {
            String path = statePath + topicPartition.toString();
            State state;
            if (zk.checkExists().forPath(path) != null) {
                state = mapper.readValue(zk.getData().forPath(path), State.class);
            } else {
                state = new State();
            }
            states.put(topicPartition, state);
        }
        return states;
    }

    private void commitState(Map<TopicPartition, State> states) throws Exception {
        for (TopicPartition topicPartition : states.keySet()) {
            String path = statePath + topicPartition.toString();
            byte[] bytes = mapper.writeValueAsBytes(states.get(topicPartition));
            if (zk.checkExists().forPath(path) != null) {
                zk.setData().forPath(path, bytes);
            } else {
                zk.create().creatingParentsIfNeeded().forPath(path, bytes);
            }
        }
    }

    public static class State {
        private long offset;
        private Map<String, Long> producerState = new HashMap<>();
        // If this consumer was also a producer we would store our output producerOffets with this state
        //Map<TopicPartition, Long> outputProducerState = new HashMap<>();

        /*
         * map of aggregates. The product of what we are consuming.  Generally large mappings should not be stored in
         * a single zookeeper ZNode.  We will do this for the sake of simplicity in this example.
         */
        private Map<String, Integer> aggregates = new HashMap<>();

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            State state = (State) o;

            if (offset != state.offset)
                return false;
            if (producerState != null ? !producerState.equals(state.producerState) : state.producerState != null)
                return false;
            return aggregates != null ? aggregates.equals(state.aggregates) : state.aggregates == null;
        }

        @Override
        public int hashCode() {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + (producerState != null ? producerState.hashCode() : 0);
            result = 31 * result + (aggregates != null ? aggregates.hashCode() : 0);
            return result;
        }

        @Override public String toString() {
            return "State{" +
                    "offset=" + offset +
                    ", producerState=" + producerState +
                    ", aggregates=" + aggregates +
                    '}';
        }
    }

}
