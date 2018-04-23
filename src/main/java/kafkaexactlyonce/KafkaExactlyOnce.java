package kafkaexactlyonce;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import kafkaexactlyonce.consumer.ConsumerRunnable;
import kafkaexactlyonce.producer.ProducerRunnable;

public class KafkaExactlyOnce {

    private final kafkaexactlyonce.utils.KafkaServer kafkaServer = new kafkaexactlyonce.utils.KafkaServer();

    private final ProducerRunnable producerRunnable = new ProducerRunnable();
    private final ConsumerRunnable consumerRunnable = new ConsumerRunnable();

    private KafkaProducer<ProducerState, Event> createProducer() {

    }

    private KafkaConsumer<ProducerState, Event> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        /* Disable kafka offset management */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        /* Make consumer block until some data is available */
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, kafkaConfig.getConsumerFetchMaxWaitMs());
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, kafkaConfig.getConsumerFetchMinBytes());
        /* Limit number of bytes fetched per partition */
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                Integer.toString(kafkaConfig.getConsumerFetchBytes()));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kcls);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, vcls);
        return new KafkaConsumer<>(properties);
    }

}
