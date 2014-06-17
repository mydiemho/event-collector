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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import com.proofpoint.event.collector.Event;
import com.proofpoint.event.collector.EventCollectorStats;
import com.proofpoint.event.collector.EventCollectorStats.Status;
import com.proofpoint.event.collector.batch.EventBatch;
import com.proofpoint.event.collector.taps.BatchProcessor.BatchHandler;
import com.proofpoint.reporting.testing.TestingReportCollectionFactory;
import com.proofpoint.stats.CounterStat;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.proofpoint.event.collector.EventCollectorStats.Status.DELIVERED;
import static com.proofpoint.event.collector.EventCollectorStats.Status.DROPPED;
import static com.proofpoint.event.collector.EventCollectorStats.Status.LOST;
import static com.proofpoint.event.collector.EventCollectorStats.Status.REJECTED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static java.util.UUID.randomUUID;

public class TestAsyncBatchProcessor
{
    private static final String ARBITRARY_EVENT_TYPE = "foo";
    private static final String ARBITRARY_FLOW_ID = "flowTest";

    private static final EventBatch ARBITRARY_BATCH = new EventBatch(ARBITRARY_EVENT_TYPE, createEvents(ARBITRARY_EVENT_TYPE, 3));
    private static final EventBatch ARBITRARY_BATCH_A = ARBITRARY_BATCH;
    private static final EventBatch ARBITRARY_BATCH_B = new EventBatch(ARBITRARY_EVENT_TYPE, createEvents(ARBITRARY_EVENT_TYPE, 4));
    private static final EventBatch ARBITRARY_BATCH_C = new EventBatch(ARBITRARY_EVENT_TYPE, createEvents(ARBITRARY_EVENT_TYPE, 5));
    private BatchHandler handler;
    private EventCollectorStats eventCollectorStats;
    private TestingReportCollectionFactory testingReportCollectionFactory;

    @BeforeMethod
    public void setup()
    {
        handler = mock(BatchHandler.class);
        testingReportCollectionFactory = new TestingReportCollectionFactory();
        eventCollectorStats = testingReportCollectionFactory.createReportCollection(EventCollectorStats.class);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventType is null")
    public void testConstructorNullEventType()
    {
        new AsyncBatchProcessor(null, ARBITRARY_FLOW_ID, new BatchProcessorConfig(), eventCollectorStats, handler);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "flowId is null")
    public void testConstructorNullFlowId()
    {
        new AsyncBatchProcessor(ARBITRARY_EVENT_TYPE, null, new BatchProcessorConfig(), eventCollectorStats, handler);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new AsyncBatchProcessor(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, null, eventCollectorStats, handler);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventCollectorStats is null")
    public void testConstructorNullEventCollectorStats()
    {
        new AsyncBatchProcessor(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, new BatchProcessorConfig(), null, handler);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "handler is null")
    public void testConstructorNullHandler()
    {
        new AsyncBatchProcessor(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, new BatchProcessorConfig(), eventCollectorStats, null);
    }

    @Test
    public void testEnqueue()
            throws Exception
    {
        BlockingBatchHandler blockingHandler = new BlockingBatchHandler();
        BatchProcessor processor = new AsyncBatchProcessor(
                ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, new BatchProcessorConfig().setQueueSize(100).setMaxOutstandingEvents(10), eventCollectorStats, blockingHandler
        );

        processor.put(ARBITRARY_BATCH_A);
        processor.put(ARBITRARY_BATCH_B);
    }

    @Test
    public void testFullQueue()
            throws Exception
    {
        BlockingBatchHandler blockingHandler = new BlockingBatchHandler();
        BatchProcessor processor = new AsyncBatchProcessor(
                ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, new BatchProcessorConfig().setQueueSize(1).setMaxOutstandingEvents(3), eventCollectorStats, blockingHandler);

        // queue has 0 batch, outstandingCount = 0
        processor.put(ARBITRARY_BATCH_A);

        // 1 batch was pulled off for processing, queue is empty, outstandingCount = 3
        processor.put(ARBITRARY_BATCH_B);

        // queue now has 1 batch, is full, outstandingCount = 3
        processor.put(ARBITRARY_BATCH_C);

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, DROPPED);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        CounterStat counterStat = reportCollection.outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, DROPPED);
        verify(counterStat).add(ARBITRARY_BATCH_C.size());
        verifyNoMoreInteractions(counterStat);
    }

    @Test
    public void testBlockOnOutstandingEventsLimit()
            throws Exception
    {
        BlockingBatchHandler blockingHandler = new BlockingBatchHandler();
        BatchProcessor processor = new AsyncBatchProcessor(
                ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, new BatchProcessorConfig().setQueueSize(5).setMaxOutstandingEvents(3), eventCollectorStats, blockingHandler);

        // queue has 0 batch, outstandingCount = 0
        processor.put(ARBITRARY_BATCH_A);
        assertEquals(blockingHandler.getProcessCount(), 1);

        // batchA was pulled off for processing, queue is now empty, outstandingCount = 3
        processor.put(ARBITRARY_BATCH_B);
        assertEquals(blockingHandler.getProcessCount(), 1);

        // queue now has 1 batch, is full, outstandingCount = 3
        processor.put(ARBITRARY_BATCH_C);

        blockingHandler.setDelivered();             // outstandingCount = 0
        assertEquals(blockingHandler.getProcessCount(), 2);
    }

    @Test
    public void testProcessRecordCorrectMetrics()
            throws Exception
    {
        BlockingBatchHandler blockingHandler = new BlockingBatchHandler();
        BatchProcessor processor = new AsyncBatchProcessor(
                ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, new BatchProcessorConfig().setQueueSize(5), eventCollectorStats, blockingHandler);

        // queue has 0 batch, outstandingCount = 0
        processor.put(ARBITRARY_BATCH_A);
        blockingHandler.setDelivered();

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, DELIVERED);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        CounterStat counterStat = reportCollection.outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, DELIVERED);
        verify(counterStat).add(ARBITRARY_BATCH_A.size());
        verifyNoMoreInteractions(counterStat);

        processor.put(ARBITRARY_BATCH_B);
        blockingHandler.setRejected();

        verify(argumentVerifier).outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, REJECTED);
        verifyNoMoreInteractions(argumentVerifier);

        counterStat = reportCollection.outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, REJECTED);
        verify(counterStat).add(ARBITRARY_BATCH_B.size());
        verifyNoMoreInteractions(counterStat);

        // queue now contains BatchB, is full, outstandingCount = 3
        processor.put(ARBITRARY_BATCH_C);
        blockingHandler.setLost();

        verify(argumentVerifier).outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, LOST);
        verifyNoMoreInteractions(argumentVerifier);

        counterStat = reportCollection.outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, LOST);
        verify(counterStat).add(ARBITRARY_BATCH_C.size());
        verifyNoMoreInteractions(counterStat);
    }

    private static Event createEvent(String type)
    {
        return new Event(type, randomUUID().toString(), "host", DateTime.now(), ImmutableMap.<String, Object>of());
    }

    private static List<Event> createEvents(String type, int count)
    {
        ImmutableList.Builder<Event> eventBuilder = ImmutableList.builder();
        for (int i = 0; i < count; ++i) {
            eventBuilder.add(createEvent(type));
        }
        return eventBuilder.build();
    }

    // block when outstanding events exceed limit
    private static class BlockingBatchHandler implements BatchHandler
    {
        private int processCount;
        private SettableFuture<Status> settableFuture;

        public BlockingBatchHandler()
        {
        }

        @Override
        public SettableFuture<Status> processBatch(EventBatch eventBatch)
        {
            settableFuture = SettableFuture.create();
            processCount += 1;
            return settableFuture;
        }

        private void setDelivered()
        {
            settableFuture.set(DELIVERED);
        }

        private void setRejected()
        {
            settableFuture.set(REJECTED);
        }

        private void setLost()
        {
            settableFuture.set(LOST);
        }

        private int getProcessCount()
        {
            return processCount;
        }
    }
}
