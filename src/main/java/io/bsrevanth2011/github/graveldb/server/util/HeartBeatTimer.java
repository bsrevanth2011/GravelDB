package io.bsrevanth2011.github.graveldb.server.util;

import io.bsrevanth2011.github.graveldb.server.GravelDBServerConfiguration;
import io.bsrevanth2011.github.graveldb.server.GravelDBServerUtil;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HeartBeatTimer {
    private ScheduledThreadPoolExecutor executor;
    private final Runnable callback;
    private Future<?> scheduledFuture;

    public HeartBeatTimer(Runnable callback) {
        this.callback = callback;
    }

    public void start() {
        scheduledFuture = getScheduledThreadPoolExecutor()
                .scheduleAtFixedRate(callback,
                        0,
                        GravelDBServerConfiguration.HEARTBEAT_INTERVAL_MILLIS,
                        TimeUnit.MILLISECONDS);
    }

    private ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
        if (executor == null) {
            executor = new ScheduledThreadPoolExecutor(1);
        }

        return executor;
    }

    public void cancel() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

}
