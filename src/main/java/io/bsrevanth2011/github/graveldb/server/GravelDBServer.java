package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.log.Log;
import io.bsrevanth2011.github.graveldb.log.PersistentLog;
import io.bsrevanth2011.github.graveldb.util.ContextAwareThreadPoolExecutor;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class GravelDBServer {

    private static final Logger logger = LoggerFactory.getLogger(GravelDBConsensusService.class.getName());

    private final Server server;
    /**
     * Create a GravelDB server using serverBuilder as a base and features as data.
     */
    public GravelDBServer(String instanceId, int port, List<? extends Channel> channels, Map<String, String> dbConf) {

        logger.info("Instantiated server with instance id := " + instanceId);
        Log log = new PersistentLog();

        int threadPoolSize = channels.size();

        this.server = ServerBuilder
                .forPort(port)
                .executor(ContextAwareThreadPoolExecutor
                        .newWithContext(threadPoolSize, threadPoolSize, Integer.MAX_VALUE, TimeUnit.MILLISECONDS))
                .addService(new GravelDBConsensusService(instanceId, channels))
                .build();

        logger.info("Started server on port := " + port);
    }

    public void init() throws IOException {
        this.server.start();
    }
}