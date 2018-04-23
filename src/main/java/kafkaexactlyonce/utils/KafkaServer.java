package kafkaexactlyonce.utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;

public class KafkaServer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServer.class);

    private final String zkConnectString;
    private final File logDir;

    private Properties config = new Properties();
    private KafkaServerStartable broker;
    private int port;

    public KafkaServer(final String zkConnectString, final File logDir) {
        this.zkConnectString = zkConnectString;
        this.logDir = logDir;
    }

    public void start() throws IOException {

        this.port = InstanceSpec.getRandomPort();

        config.setProperty("zookeeper.connect", zkConnectString);
        config.setProperty("broker.id", "1");
        config.setProperty("host.name", "localhost");
        config.setProperty("port", Integer.toString(port));
        config.setProperty("log.dir", logDir.getAbsolutePath());
        config.setProperty("log.flush.interval.messages", "1");
        config.setProperty("default.replication.factor", "1");
        config.setProperty("background.threads", "2");
        config.setProperty("num.io.threads", "1");
        config.setProperty("log.segment.bytes", "" + 1024 * 1024);
        config.setProperty("offsets.topic.num.partitions", "1");
        config.setProperty("offsets.topic.segment.bytes", "" + 1024 * 1024);
        config.setProperty("offsets.topic.replication.factor", "1");
        config.setProperty("controlled.shutdown.enable", "false");

        broker = new KafkaServerStartable(new KafkaConfig(config));
        broker.startup();
    }

    @Override
    public void close() throws IOException {
        if (broker != null) {
            broker.shutdown();
            broker = null;
        }
    }

    public void createTopic(String topicName, Integer numPartitions) {
        createTopic(topicName, numPartitions, false);
    }

    public void createTopic(String topicName, Integer numPartitions, boolean compacted) {
        // setup
        List<String> arguments = Lists.newArrayList(
                "--zookeeper", zkConnectString,
                "--create",
                "--topic", topicName,
                "--partitions", Integer.toString(numPartitions),
                "--replication-factor", "1");
        if (compacted) {
            arguments.add("--config");
            arguments.add("cleanup.policy=compact");
        }

        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(
                arguments.toArray(new String[] {}));

        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
                30000, 30000, JaasUtils.isZkSecurityEnabled());

        // run
        LOGGER.info("Creating topic. Arguments={{}]", arguments);
        try {
            TopicCommand.createTopic(zkUtils, opts);
        } catch (Throwable ignore) {
            // Ignore
        }

    }

    public int getPort() {
        return port;
    }

    public String getConnectionString() {
        return "localhost:" + port;
    }

}
