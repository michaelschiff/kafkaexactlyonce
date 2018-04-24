package kafkaexactlyonce;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafkaexactlyonce.consumer.ConsumerRunnable;
import kafkaexactlyonce.producer.ProducerRunnable;
import kafkaexactlyonce.utils.KafkaServer;
import kafkaexactlyonce.utils.ZookeeperServer;

public class KafkaExactlyOnce {

    private static String TOPIC = "example_topic";
    private static String PRODUCER_ID = "example_producer_id_1";
    private static int PARTITIONS = 4;
    private static String STATE_PATH = "/consumer_state/";

    private static ZookeeperServer zookeeperServer;
    private static KafkaServer kafkaServer;
    private static ProducerRunnable producerRunnable;
    private static ConsumerRunnable consumerRunnable;

    public static void main(String[] args) throws Throwable {
        File zkTmp = Files.createTempDirectory("zookeeper").toFile();
        File kafkaTmp = Files.createTempDirectory("kafka").toFile();
        zkTmp.deleteOnExit();
        kafkaTmp.deleteOnExit();
        zookeeperServer = new ZookeeperServer(zkTmp);
        zookeeperServer.start();
        kafkaServer = new KafkaServer(zookeeperServer.getServer().getConnectString(), kafkaTmp);
        kafkaServer.start();
        kafkaServer.createTopic(TOPIC, PARTITIONS);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int i = 0; i < PARTITIONS; i++) {
            topicPartitions.add(new TopicPartition(TOPIC, i));
        }

        CuratorFramework client = zookeeperServer.getClient();
        KafkaProducer<String, String> producer = createProducer();
        KafkaConsumer<String, String> consumer = createConsumer();
        producerRunnable = new ProducerRunnable(producer, TOPIC, PRODUCER_ID, 100, 10);
        consumerRunnable = new ConsumerRunnable(consumer, STATE_PATH, topicPartitions, client, 100);
        Thread producerThread = new Thread(producerRunnable);
        Thread consumerThread = new Thread(consumerRunnable);
        System.out.println("Starting Producer");
        producerThread.start();
        System.out.println("Starting Consumer");
        consumerThread.start();
        producerThread.join();
        consumerThread.join();

        ObjectMapper mapper = new ObjectMapper();
        for (TopicPartition topicPartition : topicPartitions) {
            ConsumerRunnable.State state = mapper
                    .readValue(client.getData().forPath(STATE_PATH + topicPartition.toString()),
                            ConsumerRunnable.State.class);
            StringBuilder builder = new StringBuilder();
            builder.append("TopicPartition: " + topicPartition.toString() + "\n");
            builder.append("\t" + state.toString());
            System.out.println(builder.toString());

        }

        client.close();
        producer.close();
        consumer.close();
        kafkaServer.close();
        zookeeperServer.close();
        System.exit(0);
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getConnectionString());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer(properties);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getConnectionString());
        /* Disable kafka offset management */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        /* Make consumer block until some data is available */
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 10_000);
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 32 * 1024);
        /* Limit number of bytes fetched per partition */
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024 * 1024);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

}
