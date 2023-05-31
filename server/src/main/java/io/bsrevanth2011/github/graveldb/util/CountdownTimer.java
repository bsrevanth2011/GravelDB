package io.bsrevanth2011.github.graveldb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class CountdownTimer {

    private static final Logger logger = LoggerFactory.getLogger(CountdownTimer.class);
    private ScheduledThreadPoolExecutor executor;
    private Future<?> scheduledFuture;

    @SuppressWarnings("UnusedReturnValue")
    public long startCountdown(Runnable handler, long delay) {
        stopIfStarted();
        scheduledFuture = getScheduledThreadPoolExecutor()
                .schedule(handler, delay, TimeUnit.MILLISECONDS);
        logger.debug("Started countdown timer with a " +
                "trigger set to go off after a fixed delay of {} milliseconds from now", delay);
        return delay;
    }

    public void schedule(Runnable handler, long fixedRate) {
        stopIfStarted();
        scheduledFuture = getScheduledThreadPoolExecutor()
                .scheduleWithFixedDelay(handler, 0, fixedRate, TimeUnit.MILLISECONDS);
        logger.debug("Started scheduled timer with a " +
                "trigger set to go off every {} milliseconds", fixedRate);
    }

    private ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
        if (executor == null) {
            executor = new ScheduledThreadPoolExecutor(1);
        }
        return executor;
    }

    public void stopIfStarted() {
        if (scheduledFuture != null) {
            logger.debug("Terminating countdown timer");
            scheduledFuture.cancel(true);
        }
    }
}
