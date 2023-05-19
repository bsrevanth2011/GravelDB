package io.bsrevanth2011.github.graveldb.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bsrevanth2011.github.graveldb.server.GravelDBServerConfiguration;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.*;

public class ContextAwareThreadPoolExecutor extends ThreadPoolExecutor {

    private final Map<String, String> context;

    /**
     * Pool where task threads take fixed MDC from the thread that creates the pool.
     */
    public static ContextAwareThreadPoolExecutor newWithContext(int corePoolSize,
                                                                int maximumPoolSize,
                                                                long keepAliveTime,
                                                                TimeUnit unit) {
        return new ContextAwareThreadPoolExecutor(MDC.getCopyOfContextMap(),
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit);
    }

    private ContextAwareThreadPoolExecutor(Map<String, String> context,
                                           int corePoolSize,
                                           int maximumPoolSize,
                                           long keepAliveTime,
                                           TimeUnit unit) {
        super(corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                new ArrayBlockingQueue<>(GravelDBServerConfiguration.MAX_TASK_QUEUE_SIZE),
                new ThreadFactoryBuilder()
                        .setNameFormat(GravelDBServerConfiguration.CONTEXT_AWARE_THREAD_POOL_NAME_FORMAT)
                        .build());
        this.context = context;
    }

    /**
     * All executions will have MDC injected. {@code ThreadPoolExecutor}'s submission methods ({@code submit()} etc.)
     * all delegate to this.
     */
    @Override
    public void execute(@Nonnull Runnable command) {
        super.execute(wrap(command));
    }

    public Runnable wrap(final Runnable runnable) {
        return () -> {
            Map<String, String> previous = MDC.getCopyOfContextMap();
            if (context == null) {
                MDC.clear();
            } else {
                MDC.setContextMap(context);
            }
            try {
                runnable.run();
            } finally {
                if (previous == null) {
                    MDC.clear();
                } else {
                    MDC.setContextMap(previous);
                }
            }
        };
    }
}
