package io.bsrevanth2011.github.graveldb.server;

public class GravelDBServerConfiguration {

    public static final int ELECTION_TIMEOUT_MILLIS = 30_000;
    public static final int HEARTBEAT_INTERVAL_MILLIS = 4_000;
    public static final int STATE_MACHINE_SYNC_FIXED_RATE_MILLIS = 4000;
    public static final int MAX_TASK_QUEUE_SIZE = 50_000;
    public static final String CONTEXT_AWARE_THREAD_POOL_NAME_FORMAT = "grpc-exec-%d";
    public static final int MAX_ENTRIES_IN_SINGLE_LOG_REQUEST = 100;
    public static final int MAX_REQUEST_TIMEOUT = 5_000;
}
