// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.processor;

import de.bytefish.pgbulkinsert.pgsql.processor.handler.IBulkWriteHandler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class BulkProcessor<TEntity> implements AutoCloseable {

    private final ScheduledThreadPoolExecutor scheduler;
    private final ScheduledFuture<?> scheduledFuture;

    private volatile boolean closed = false;

    private final IBulkWriteHandler<TEntity> handler;

    private final int bulkSize;

    private List<TEntity> batchedEntities;

    public BulkProcessor(IBulkWriteHandler<TEntity> handler, int bulkSize) {
        this(handler, bulkSize, null);
    }

    public BulkProcessor(IBulkWriteHandler<TEntity> handler, int bulkSize, Duration flushInterval) {

        this.handler = handler;
        this.bulkSize = bulkSize;

        // Start with an empty List of batched entities:
        this.batchedEntities = new ArrayList<>();

        if(flushInterval != null) {
            // Create a Scheduler for the time-based Flush Interval:
            this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
            this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Flush(), flushInterval.toMillis(), flushInterval.toMillis(), TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    public synchronized BulkProcessor<TEntity> add(TEntity entity) {
        batchedEntities.add(entity);
        executeIfNeccessary();
        return this;
    }

    @Override
    public void close() throws Exception {
        // If the Processor has already been closed, do not proceed:
        if (closed) {
            return;
        }
        closed = true;

        // Quit the Scheduled FlushInterval Future:
        if (this.scheduledFuture != null) {
            cancel(this.scheduledFuture);
            this.scheduler.shutdown();
        }

        // Are there any entities left to write?
        if (batchedEntities.size() > 0) {
            execute();
        }
    }

    private void executeIfNeccessary() {
        if(batchedEntities.size() >= bulkSize) {
            execute();
        }
    }

    // (currently) needs to be executed under a lock
    private void execute() {
        // Assign to a new List:
        final List<TEntity> entities = batchedEntities;
        // We can restart batching entities:
        batchedEntities = new ArrayList<>();
        // Write the previously batched entities to PostgreSQL:
        write(entities);
    }

    private void write(List<TEntity> entities) {
        try {
            handler.write(entities);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean cancel(Future<?> future) {
        if (future != null) {
            return future.cancel(false);
        }
        return false;
    }

    class Flush implements Runnable {

        @Override
        public void run() {
            synchronized (BulkProcessor.this) {
                if (closed) {
                    return;
                }
                if (batchedEntities.size() == 0) {
                    return;
                }
                execute();
            }

        }
    }
}