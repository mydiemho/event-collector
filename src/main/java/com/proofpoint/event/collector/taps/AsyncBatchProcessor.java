/*
 * Copyright 2011-2014 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.proofpoint.event.collector.taps;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.proofpoint.event.collector.EventCollectorStats;
import com.proofpoint.event.collector.EventCollectorStats.Status;
import com.proofpoint.event.collector.batch.EventBatch;

import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.proofpoint.event.collector.EventCollectorStats.Status.DROPPED;

public class AsyncBatchProcessor implements BatchProcessor
{
    private final BatchHandler handler;
    private final List<EventBatch> batchQueue;

    private final int maxOutstandingEvents;
    private final int maxQueueSize;
    private final String eventType;
    private final String flowId;
    private int outstandingEventCount;
    private Object outstandingEventsGuard = new Object();

    private final EventCollectorStats eventCollectorStats;

    public AsyncBatchProcessor(String eventType, String flowId, BatchProcessorConfig config, EventCollectorStats eventCollectorStats, BatchHandler handler)
    {
        this.eventType = checkNotNull(eventType, "eventType is null");
        this.flowId = checkNotNull(flowId, "flowId is null");
        this.handler = checkNotNull(handler, "handler is null");

        checkNotNull(config, "config is null");
        this.maxQueueSize = config.getQueueSize();
        this.batchQueue = new LinkedList<>();
        this.maxOutstandingEvents = config.getMaxOutstandingEvents();

        this.eventCollectorStats = checkNotNull(eventCollectorStats, "eventCollectorStats is null");
    }

    @Override
    public void put(EventBatch eventBatch)
    {
        checkNotNull(eventBatch, "eventBatch is null");

        synchronized (outstandingEventsGuard) {

            if (batchQueue.size() >= maxQueueSize) {
                // batchQueue is full: drop current batch
                recordMetrics(DROPPED, eventBatch.size());
            }

            batchQueue.add(eventBatch);
        }

        processPendingBatches();
    }

    private void processPendingBatches()
    {
        while (true) {
            EventBatch batch;
            synchronized (outstandingEventsGuard) {
                if (outstandingEventCount >= maxOutstandingEvents || batchQueue.isEmpty()) {
                    return;
                }
                batch = batchQueue.remove(0);
                outstandingEventCount += batch.size();
            }

            processBatch(batch);
        }
    }

    private void processBatch(final EventBatch finalBatch)
    {
        ListenableFuture<Status> future = handler.processBatch(finalBatch);
        addCallback(future, new FutureCallback<Status>()
        {
            @Override
            public void onSuccess(@Nullable Status status)
            {
                recordMetrics(status, finalBatch.size());
                onCompleted();
            }

            @Override
            public void onFailure(Throwable t)
            {
                // record nothing ?
                onCompleted();
            }

            private void onCompleted()
            {
                synchronized (outstandingEventsGuard) {
                    outstandingEventCount -= finalBatch.size();
                }

                processPendingBatches();
            }
        });

    }

    private void recordMetrics(Status status, int eventCount)
    {
        eventCollectorStats.outboundEvents(eventType, flowId, status).add(eventCount);
    }
}