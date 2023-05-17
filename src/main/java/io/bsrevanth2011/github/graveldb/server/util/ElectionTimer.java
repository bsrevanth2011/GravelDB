package io.bsrevanth2011.github.graveldb.server.util;

import io.bsrevanth2011.github.graveldb.server.GravelDBServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ElectionTimer {

    private static final Logger logger = LoggerFactory.getLogger(ElectionTimer.class);
    private ScheduledThreadPoolExecutor executor;
    private final Runnable callback;
    private Future<?> scheduledFuture;

    public ElectionTimer(Runnable callback) {
        this.callback = callback;
    }

    public static ElectionTimer createStarted(Runnable runnable) {
        ElectionTimer electionTimer = new ElectionTimer(runnable);
        electionTimer.start();
        return electionTimer;
    }
    public void start() {
        long delay = generateRandomDelay();
        logger.info("Started election timer with delay of := " + delay);
        scheduledFuture = getScheduledThreadPoolExecutor().schedule(callback, delay, TimeUnit.MILLISECONDS);
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

    long generateRandomDelay() {
        // generate a random delay between T and 2T
        return GravelDBServerConfiguration.ELECTION_TIMEOUT_MILLIS
                + new Random().nextInt(0, GravelDBServerConfiguration.ELECTION_TIMEOUT_MILLIS / 1000) * 1000L;
    }

    public void restart() {
        cancel();
        start();
    }
}
