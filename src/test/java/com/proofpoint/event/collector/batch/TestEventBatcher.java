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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.event.collector.Event;
import com.proofpoint.event.collector.EventWriter;
import com.proofpoint.testing.SerialScheduledExecutorService;
import com.proofpoint.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static java.util.UUID.randomUUID;

public class TestEventBatcher
{
    private static final int MAX_BATCH_SIZE = 5;
    private static final Duration MAX_PROCESSING_DELAY = new Duration(5, TimeUnit.SECONDS);
    private static final String ARBITRARY_EVENT_TYPE_A = "foo";
    private static final String ARBITRARY_EVENT_TYPE_B = "bar";
    private static final Event ARBITRARY_EVENT = createEvent(ARBITRARY_EVENT_TYPE_A);
    private static final Event[] ARBITRARY_LIST_OF_EVENTS = createEvents(ARBITRARY_EVENT_TYPE_A, MAX_BATCH_SIZE);
    private static final TestEventWriter ARBITRARY_WRITER = new TestEventWriter();

    private EventBatcher eventBatcher;
    private SerialScheduledExecutorService executorService;

    @BeforeMethod
    public void setup()
    {
        executorService = new SerialScheduledExecutorService();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "type is null")
    public void testConstructorNullEventType()
    {
        new EventBatcher(null, MAX_BATCH_SIZE, MAX_PROCESSING_DELAY, ImmutableSet.of(ARBITRARY_WRITER), executorService);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "maxProcessingDelay is null")
    public void testConstructorNullMaxProcessingDelay()
    {
        new EventBatcher(ARBITRARY_EVENT_TYPE_A, MAX_BATCH_SIZE, null, ImmutableSet.of(ARBITRARY_WRITER), executorService);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "writers are null")
    public void testConstructorNullEventWriters()
    {
        new EventBatcher(ARBITRARY_EVENT_TYPE_A, MAX_BATCH_SIZE, MAX_PROCESSING_DELAY, null, executorService);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "executorService is null")
    public void testConstructorNullExecutorService()
    {
        new EventBatcher(ARBITRARY_EVENT_TYPE_A, MAX_BATCH_SIZE, MAX_PROCESSING_DELAY, ImmutableSet.of(ARBITRARY_WRITER), null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAddingEventOfWrongType()
    {
        eventBatcher = new EventBatcher(ARBITRARY_EVENT_TYPE_A, MAX_BATCH_SIZE, MAX_PROCESSING_DELAY, ImmutableSet.of(ARBITRARY_WRITER), executorService);
        eventBatcher.add(createEvent(ARBITRARY_EVENT_TYPE_B));
    }

    @Test
    public void testFullBatchAreFlushed()
    {
        TestEventWriter eventWriter = new TestEventWriter();
        eventBatcher = new EventBatcher(ARBITRARY_EVENT_TYPE_A, MAX_BATCH_SIZE, MAX_PROCESSING_DELAY, ImmutableSet.of(eventWriter), executorService);
        eventWriter.verifyWrite(0, null);

        eventBatcher.add(ARBITRARY_LIST_OF_EVENTS[0]);

        for (int i = 1; i < MAX_BATCH_SIZE; i++) {
            eventBatcher.add(ARBITRARY_LIST_OF_EVENTS[i]);
        }

        eventWriter.verifyWrite(1, new EventBatch(ARBITRARY_EVENT_TYPE_A, Arrays.asList(ARBITRARY_LIST_OF_EVENTS)));
    }

    @Test
    public void testNothingHappenedIfBatchIsEmpty()
    {
        TestEventWriter eventWriter = new TestEventWriter();
        eventBatcher = new EventBatcher(ARBITRARY_EVENT_TYPE_A, MAX_BATCH_SIZE, MAX_PROCESSING_DELAY, ImmutableSet.of(eventWriter), executorService);
        eventWriter.verifyWrite(0, null);

        executorService.elapseTime(
                MAX_PROCESSING_DELAY.toMillis(),
                TimeUnit.MILLISECONDS);

        eventWriter.verifyWrite(0, null);
    }

    @Test
    public void testProcessingDurationElapsedFlushNonEmptyBatchEvenIfBatchIsNotFull()
    {
        TestEventWriter eventWriter = new TestEventWriter();
        eventBatcher = new EventBatcher(ARBITRARY_EVENT_TYPE_A, MAX_BATCH_SIZE, MAX_PROCESSING_DELAY, ImmutableSet.of(eventWriter), executorService);
        eventWriter.verifyWrite(0, null);

        // t = 0, add 1 event
        eventBatcher.add(ARBITRARY_EVENT);

        // move forward to t = 5
        executorService.elapseTime(
                MAX_PROCESSING_DELAY.toMillis(),
                TimeUnit.MILLISECONDS);

        // verify that a batch is written
        eventWriter.verifyWrite(1, new EventBatch(ARBITRARY_EVENT_TYPE_A, ImmutableList.of(ARBITRARY_EVENT)));
    }

    @Test
    public void testProcessingTimerIsResetAfterAFlush()
    {
        TestEventWriter eventWriter = new TestEventWriter();
        eventBatcher = new EventBatcher(ARBITRARY_EVENT_TYPE_A, MAX_BATCH_SIZE, MAX_PROCESSING_DELAY, ImmutableSet.of(eventWriter), executorService);
        eventWriter.verifyWrite(0, null);

        // t = 0, add 1 event
        eventBatcher.add(ARBITRARY_LIST_OF_EVENTS[0]);

        // t = 4, add 4 more events to fill up batch
        executorService.elapseTime(
                4,
                TimeUnit.SECONDS);

        for (int i = 1; i < MAX_BATCH_SIZE; i++) {
            eventBatcher.add(ARBITRARY_LIST_OF_EVENTS[i]);
        }

        // verify that a batch is written
        eventWriter.verifyWrite(1, new EventBatch(ARBITRARY_EVENT_TYPE_A, Arrays.asList(ARBITRARY_LIST_OF_EVENTS)));

        // t = 4, add 1 event, timer is now reset to flush at t = 9
        eventBatcher.add(ARBITRARY_EVENT);

        // t = 5, verify that nothing happened
        executorService.elapseTime(
                1,
                TimeUnit.SECONDS);
        eventWriter.verifyWrite(1, null);

        // t = 9, verify that a second batch is written
        executorService.elapseTime(
                4,
                TimeUnit.SECONDS);

        eventWriter.verifyWrite(2, new EventBatch(ARBITRARY_EVENT_TYPE_A, ImmutableList.of(ARBITRARY_EVENT)));

    }

    private static Event createEvent(String type)
    {
        return new Event(type, randomUUID().toString(), "host", DateTime.now(), ImmutableMap.<String, Object>of());
    }

    private static Event[] createEvents(String type, int count)
    {
        Event[] results = new Event[count];
        for (int i = 0; i < count; ++i) {
            results[i] = createEvent(type);
        }
        return results;
    }

    private static class TestEventWriter implements EventWriter {

        private EventBatch eventBatch;
        private int writeCount;

        public TestEventWriter()
        {
            writeCount = 0;
        }

        @Override
        public void write(Event event)
                throws IOException
        {
        }

        @Override
        public void write(EventBatch eventBatch)
                throws IOException
        {
            this.eventBatch = eventBatch;
            this.writeCount = writeCount + 1;
        }

        public void verifyWrite(int expectedWriteCount, EventBatch expectedEventBatch)
        {
            assertEquals(writeCount, expectedWriteCount);
            assertEquals(eventBatch, expectedEventBatch);

            reset();
        }

        private void reset()
        {
            this.eventBatch = null;
        }
    }
}
