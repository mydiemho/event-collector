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
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.assistedinject.Assisted;
import com.proofpoint.event.collector.Event;
import com.proofpoint.event.collector.EventCollectorStats;
import com.proofpoint.event.collector.EventCollectorStats.Status;
import com.proofpoint.event.collector.batch.EventBatch;
import com.proofpoint.event.collector.taps.BatchProcessor.BatchHandler;
import com.proofpoint.http.client.HttpClient.HttpResponseFuture;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.StatusResponseHandler.StatusResponse;
import com.proofpoint.http.client.balancing.BalancingHttpClient;
import com.proofpoint.json.JsonCodec;

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
import static javax.ws.rs.core.Response.Status.fromStatusCode;

class HttpFlow implements Flow
{
    private static final JsonCodec<List<Event>> EVENT_LIST_JSON_CODEC = listJsonCodec(Event.class);
    private final BalancingHttpClient httpClient;
    private final BatchProcessor batchProcessor;

    @Inject
    HttpFlow(@Assisted("eventType") String eventType, @Assisted("flowId") String flowId, @Assisted BalancingHttpClient httpClient,
            BatchProcessorFactory batchProcessorFactory, EventCollectorStats eventCollectorStats)
    {

        checkNotNull(eventType, "eventType is null");
        checkNotNull(flowId, "flowId is null");
        checkNotNull(eventCollectorStats, "eventCollectorStats is null");

        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.batchProcessor = checkNotNull(batchProcessorFactory, "batchProcessorFactory is null").
                createBatchProcessor(eventType, flowId, eventCollectorStats, new BatchHandler()
                {
                    @Override
                    public SettableFuture<Status> processBatch(EventBatch eventBatch)
                    {
                        return HttpFlow.this.processBatch(eventBatch);
                    }
                });
    }

    @Override
    public void enqueue(EventBatch eventBatch)
    {
        this.batchProcessor.put(eventBatch);
    }

    @Override
    public void enqueue(Event event)
    {
        // do nothing
    }

    private SettableFuture<Status> processBatch(EventBatch eventBatch)
    {
        Request request = Request.builder()
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(EVENT_LIST_JSON_CODEC, eventBatch.getEvents()))
                .build();

        HttpResponseFuture<StatusResponse> responseFuture = httpClient.executeAsync(request, createStatusResponseHandler());
        final SettableFuture<Status> settableFuture = SettableFuture.create();

        addCallback(responseFuture, new FutureCallback<StatusResponse>()
        {
            @Override
            public void onSuccess(@Nullable StatusResponse result)
            {
                switch (fromStatusCode(result.getStatusCode()).getFamily()) {
                    case CLIENT_ERROR:
                        settableFuture.set(REJECTED);
                    case SERVER_ERROR:
                        settableFuture.set(LOST);
                    default:
                        settableFuture.set(DELIVERED);
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                // do nothing
            }
        });

        return settableFuture;
    }
}
