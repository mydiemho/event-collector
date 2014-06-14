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
import com.google.inject.assistedinject.Assisted;
import com.proofpoint.event.collector.Event;
import com.proofpoint.event.collector.EventCollectorStats;
import com.proofpoint.event.collector.batch.EventBatch;
import com.proofpoint.event.collector.taps.BatchProcessor.BatchHandler;
import com.proofpoint.http.client.HttpClient.HttpResponseFuture;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.StatusResponseHandler.StatusResponse;
import com.proofpoint.http.client.balancing.BalancingHttpClient;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.proofpoint.event.collector.EventCollectorStats.Status.DELIVERED;
import static com.proofpoint.event.collector.EventCollectorStats.Status.LOST;
import static com.proofpoint.event.collector.EventCollectorStats.Status.REJECTED;
import static com.proofpoint.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.proofpoint.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.proofpoint.json.JsonCodec.listJsonCodec;

class HttpFlow implements Flow
{
    private static final Logger log = Logger.get(HttpFlow.class);
    private static final JsonCodec<List<Event>> EVENT_LIST_JSON_CODEC = listJsonCodec(Event.class);
    private final String eventType;
    private final String flowId;
    private final EventCollectorStats eventCollectorStats;
    private final BalancingHttpClient httpClient;
    private final BatchProcessor<EventBatch> batchProcessor;
    private final int maxOutstandingEvents = 10_000;
    private int outstandingEventsCount;
    private Object outstandingEventsGuard = new Object();

    @Inject
    HttpFlow(@Assisted("eventType") String eventType, @Assisted("flowId") String flowId, @Assisted BalancingHttpClient httpClient,
            BatchProcessorFactory batchProcessorFactory, EventCollectorStats eventCollectorStats)
    {
        this.eventType = checkNotNull(eventType, "eventType is null");
        this.flowId = checkNotNull(flowId, "flowId is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");

        this.batchProcessor = checkNotNull(batchProcessorFactory, "batchProcessorFactory is null").createBatchProcessor(createBatchProcessorName(eventType, flowId), new BatchHandler<EventBatch>()
        {
            @Override
            public void processBatch(EventBatch eventBatch)
            {
                HttpFlow.this.processBatch(eventBatch);
            }

            @Override
            public void notifyEntriesDropped(int count)
            {
                onRecordsLost(count);
            }
        });
        this.eventCollectorStats = checkNotNull(eventCollectorStats, "eventCollectorStats is null");
    }

    @Override
    public void enqueue(Event event)
    {
    }

    @Override
    public void enqueue(EventBatch eventBatch)
    {
        this.batchProcessor.put(eventBatch);
    }

    private void processBatch(EventBatch eventBatch)
    {
        final int batchSize = eventBatch.size();
        synchronized (outstandingEventsGuard) {
            while (outstandingEventsCount > maxOutstandingEvents) {
                try {
                    outstandingEventsGuard.wait();
                }
                catch (InterruptedException e) {
                    log.warn(e, "Weeeee!!!");
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            outstandingEventsCount += batchSize;
        }

        Request request = Request.builder()
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(EVENT_LIST_JSON_CODEC, eventBatch.getEvents()))
                .build();

        HttpResponseFuture<StatusResponse> responseFuture = httpClient.executeAsync(request, createStatusResponseHandler());
        addCallback(responseFuture, new FutureCallback<StatusResponse>()
        {
            @Override
            public void onSuccess(@Nullable StatusResponse result)
            {
                onCompleted();
                onRecordsDelivered(batchSize);
            }

            @Override
            public void onFailure(Throwable t)
            {
                onCompleted();
                onRecordsRejected(batchSize);

            }

            private void onCompleted()
            {
                synchronized (outstandingEventsGuard) {
                    boolean blocked = outstandingEventsCount >= maxOutstandingEvents;
                    outstandingEventsCount -= batchSize;
                    if (blocked && outstandingEventsCount < maxOutstandingEvents) {
                        outstandingEventsGuard.notifyAll();
                    }
                }
            }
        });
    }

    private void onRecordsDelivered(int eventCount)
    {
        eventCollectorStats.outboundEvents(eventType, flowId, DELIVERED).add(eventCount);
    }

    private void onRecordsLost(int eventCount)
    {
        eventCollectorStats.outboundEvents(eventType, flowId, LOST).add(eventCount);
    }

    private void onRecordsRejected(int eventCount)
    {
        eventCollectorStats.outboundEvents(eventType, flowId, REJECTED).add(eventCount);
    }

    private static String createBatchProcessorName(String eventType, String flowId)
    {
        return String.format("%s{%s}", eventType, flowId);
    }
}
