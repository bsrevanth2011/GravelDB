package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.server.util.ContextAwareThreadPoolExecutor;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class GravelDBServer {

    private static final Logger logger = LoggerFactory.getLogger(GravelDBConsensusService.class.getName());

    private final Server server;
    /**
     * Create a GravelDB server using serverBuilder as a base and features as data.
     */
    public GravelDBServer(String instanceId, int port, List<? extends Channel> targets) {

        logger.info("Instantiated server with instance id := " + instanceId);

        State state = new State(
                instanceId,
                new Log(),
                targets,
                0,
                null,
                0,
                0,
                GravelDBConsensusService.ServerState.FOLLOWER
        );

        this.server = ServerBuilder
                .forPort(port)
                .executor(ContextAwareThreadPoolExecutor.newWithContext(5, 5, 10_000, TimeUnit.MILLISECONDS))
                .addService(new GravelDBConsensusService(state))
                .build();
    }

    public void init() throws IOException {
        this.server.start();
    }

}