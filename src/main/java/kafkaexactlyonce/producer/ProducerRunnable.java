package kafkaexactlyonce.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafkaexactlyonce.utils.ProducerState;

public class ProducerRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerRunnable.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final String producerId;
    private final int totalMessages;
    private final int batchSize;

    public ProducerRunnable(KafkaProducer<String, String> producer, String topic, String producerId, int totalMessages, int batchSize) {
        this.producer = producer;
        this.topic = topic;
        this.producerId = producerId;
        this.totalMessages = totalMessages;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        /*
         * Simulates our offset if we were consuming from another topic
         */
        int i = 0;
        /*
         * Stores our output producer offsets.  This would be commited with our upstream offsets.
         */
        Map<TopicPartition, Long> outputProducerOffsets = new HashMap<>();
        while (i < totalMessages && !Thread.currentThread().isInterrupted()) {
            /*
             * Batch to store the results of sending this batch
             */
            List<Future<RecordMetadata>> batch = new ArrayList<>();
            int k;
            Map<TopicPartition, Long> batchOutputOffsets = new HashMap<>(outputProducerOffsets);
            for (k = 0; k < batchSize; k++) {
                /*
                 * partition according to the mod of our message offset. This will produce a consistent destination
                 * partition for this particular message, assuming no changes in the number of partitions.
                 */
                int partition = k % producer.partitionsFor(topic).size();
                TopicPartition outputTopicPartition = new TopicPartition(topic, partition);
                long producerOffset = batchOutputOffsets.getOrDefault(outputTopicPartition, 0L);

                String key = new ProducerState(producerId, producerOffset).toString();
                String value = k % 2 == 0 ? "A" : "B";
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, value);

                batch.add(producer.send(record));
                /*
                 * Simulates possibility of duplicate records
                 */
                if (Math.random() > .9) {
                    batch.add(producer.send(record));
                }

                /*
                 * Store the updated producer offset
                 */
                batchOutputOffsets.put(outputTopicPartition, producerOffset + 1);
            }

            /*
             * Wait for acks from all messages in this batch
             */
            try {
                for (Future<RecordMetadata> future : batch) {
                    future.get();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                /*
                 * In the event of an error, repeat the batch. continue with i and outputProducerOffsets unmodified
                 */
                LOGGER.error("Error in send, repeating batch", e);
                continue;
            }
            i += k;
            outputProducerOffsets.putAll(batchOutputOffsets);
        }
    }
}
