package kafkaexactlyonce;

import java.io.IOException;
import org.apache.curator.test.TestingServer;

public class ZookeeperRule {
    private TestingServer server;
    public void start() throws Exception {
        server = new TestingServer();
	server.start();
    }

    public void stop() throws IOException {
        server.stop();
    }
}
