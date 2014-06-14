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
package com.proofpoint.event.collector.batch;

import com.google.common.annotations.VisibleForTesting;
import com.proofpoint.event.collector.Event;
import com.proofpoint.event.collector.EventWriter;
import com.proofpoint.units.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class EventBatcher
{
    private final int maxBatchSize;
    private final Duration maxProcessingDelay;
    private final String type;
    private final Set<? extends  EventWriter> writers;
    private final ScheduledExecutorService executorService;

    private final ArrayList<Event> events;
    private final Object batchGuard = new Object();
    private ScheduledFuture<?> refreshFuture;
    private Runnable refreshJob = null;

    public EventBatcher(String type,
            int maxBatchSize,
            Duration maxProcessingDelay,
            Set<? extends EventWriter> writers,
            ScheduledExecutorService executorService)
    {
        this.type = checkNotNull(type, "type is null");
        this.maxBatchSize = maxBatchSize;
        this.maxProcessingDelay = checkNotNull(maxProcessingDelay, "maxProcessingDelay is null");
        this.writers = checkNotNull(writers, "writers are null");
        this.executorService = checkNotNull(executorService, "executorService is null");
        this.events = new ArrayList<>(maxBatchSize);
    }

    public void add(Event event)
    {
        checkArgument(event.getType().equals(type), "cannot accept event of type %s, event must be of type %s", event.getType(), type);

        EventBatch eventBatch = null;
        synchronized (batchGuard) {
            events.add(event);

            if (events.size() >= maxBatchSize) {
                eventBatch = createBatch();
            }
            else if (refreshFuture == null) {
                refreshJob = new Runnable()
                {
                    @Override
                    public void run()
                    {
                        EventBatch eventBatch = null;
                        synchronized (batchGuard) {
                            if (this == refreshJob) {
                                eventBatch = createBatch();
                            }
                        }
                        if (eventBatch != null) {
                            flush(eventBatch);
                        }
                    }
                };
                refreshFuture = executorService.schedule(refreshJob, maxProcessingDelay.toMillis(), TimeUnit.MILLISECONDS);
            }
        }

        if (eventBatch != null) {
            flush(eventBatch);
        }
    }

    private EventBatch createBatch()
    {
        EventBatch eventBatch = new EventBatch(type, events);
        events.clear();
        if (refreshFuture != null) {
            // If the maxumim batch size is 1, we'll never create future to timeout and send
            // batches. We must check this to avoid an NPE.
            refreshFuture.cancel(false);
            refreshFuture = null;
            refreshJob = null;
        }
        return eventBatch;
    }

    private void flush(EventBatch eventBatch)
    {
        for (EventWriter writer : writers) {
            try {
                writer.write(eventBatch);
            }
            catch (IOException e) {

            }
        }
    }

    @VisibleForTesting
    ArrayList<Event> getEvents()
    {
        return events;
    }
}
