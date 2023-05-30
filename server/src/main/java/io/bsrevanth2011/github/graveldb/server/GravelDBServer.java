package io.bsrevanth2011.github.graveldb.server;

import io.bsrevanth2011.github.graveldb.util.ContextAwareThreadPoolExecutor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class GravelDBServer {

    private static final Logger logger = LoggerFactory.getLogger(GravelDBServer.class);
    private static final int THREADPOOL_SIZE = 100;

    private final Server server;
    /**
     * Create a GravelDB server using serverBuilder as a base and features as data.
     */
    public GravelDBServer(int instanceId, int port, ServerStubConfig[] stubConfigs) throws RocksDBException, IOException {

        RaftServer server = new RaftServer(instanceId, stubConfigs);
        DBClient client = new DBClient(server);

        this.server = ServerBuilder
                .forPort(port)
                .executor(ContextAwareThreadPoolExecutor
                        .newWithContext(THREADPOOL_SIZE, THREADPOOL_SIZE, Integer.MAX_VALUE, TimeUnit.MILLISECONDS))
                .addService(server)
                .addService(client)
                .build();

        logger.info("Starting server on port := " + port);
    }

    public void init() throws IOException {
        this.server.start();
    }
}