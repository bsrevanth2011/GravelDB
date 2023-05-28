package io.bsrevanth2011.github.graveldb.server;

public class GravelDBServerConfiguration {

    public static final int ELECTION_TIMEOUT_MILLIS = 25_000;
    public static final int HEARTBEAT_INTERVAL_MILLIS = 2_000;
    public static final int MAX_TASK_QUEUE_SIZE = 50_000;
    public static final String CONTEXT_AWARE_THREAD_POOL_NAME_FORMAT = "grpc-exec-%d";
    public static final int MAX_ENTRIES_IN_SINGLE_LOG_REQUEST = 100;
}
