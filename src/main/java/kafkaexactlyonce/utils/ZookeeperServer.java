package kafkaexactlyonce;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperServer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperServer.class);

    private final File zkDir;
    private TestingServer server = null;
    private CuratorFramework client = null;

    public ZookeeperServer(final File zkDir) {
        this.zkDir = zkDir;
    }

    public void start() throws Throwable {
        LOGGER.info("Starting testing Zookeeper. Port=[random], Directory=[{}]", zkDir);
        this.server = new TestingServer(-1, zkDir);
        this.server.start();
        LOGGER.info("Starting testing Zookeeper client. ConnectionString=[{}]", this.getServer().getConnectString());
        this.client = CuratorFrameworkFactory.newClient(this.getServer().getConnectString(),
                new RetryUntilElapsed(30000, 1000));
        this.client.start();
    }

    @Override
    public void close() {
        if (this.client != null) {
            LOGGER.info("Closing testing Zookeeper client...");
            client.close();
            this.client = null;
        }
        if (this.server != null) {
            try {
                LOGGER.info("Closing testing Zookeeper...");
                this.server.close();
                this.server = null;
            } catch (IOException e) {
                LOGGER.debug("Error closing testing Zookeeper", e);
            }
        }
    }

    public TestingServer getServer() {
        return server;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public static void main(String[] args) {
        try (TestingServer server = new TestingServer(Integer.parseInt(args[0]))) {
            server.start();
            Thread.currentThread().join();
        } catch (Exception e) {
            LOGGER.debug("Error running TestingServer", e);
        }
    }

}
