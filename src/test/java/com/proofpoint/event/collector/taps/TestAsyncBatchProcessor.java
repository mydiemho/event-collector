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
import com.proofpoint.event.collector.Event;
import com.proofpoint.event.collector.batch.EventBatch;
import com.proofpoint.event.collector.taps.BatchProcessor.BatchHandler;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAsyncBatchProcessor
{
    private static final EventBatch ARBITRARY_BATCH = new EventBatch("foo", ImmutableList.of(event("foo")));
    private static final EventBatch ARBITRARY_BATCH_A = ARBITRARY_BATCH;
    private static final EventBatch ARBITRARY_BATCH_B = new EventBatch("bar", ImmutableList.of(event("bar")));
    private BatchHandler<EventBatch> handler;

    @BeforeMethod
    public void setup()
    {
        handler = mock(BatchHandler.class);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "name is null")
    public void testConstructorNullName()
    {
        new AsyncBatchProcessor(null, handler, new BatchProcessorConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "handler is null")
    public void testConstructorNullHandler()
    {
        new AsyncBatchProcessor("name", null, new BatchProcessorConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new AsyncBatchProcessor("name", handler, null);
    }

    @Test
    public void testEnqueue()
            throws Exception
    {
        BatchProcessor<EventBatch> processor = new AsyncBatchProcessor<>(
                "foo", handler, new BatchProcessorConfig().setQueueSize(100)
        );
        processor.start();

        processor.put(ARBITRARY_BATCH);
        processor.put(ARBITRARY_BATCH);
        processor.put(ARBITRARY_BATCH);
    }

    @Test
    public void testFullQueue()
            throws Exception
    {
        BlockingBatchHandler blockingHandler = blockingHandler();
        BatchProcessor<EventBatch> processor = new AsyncBatchProcessor<>(
                "foo", blockingHandler, new BatchProcessorConfig().setQueueSize(1)
        );

        processor.start();

        blockingHandler.lock();
        try {
            // This will be processed, and its processing will block the handler
            processor.put(ARBITRARY_BATCH);
            assertEquals(blockingHandler.getDroppedEntries(), 0);

            // Wait for the handler to pick up the item from the queue
            assertTrue(blockingHandler.waitForProcessor(10));

            // This will remain in the queue because the processing
            // thread has not yet been resumed
            processor.put(ARBITRARY_BATCH);
            assertEquals(blockingHandler.getDroppedEntries(), 0);

            // The queue is now full, this message will be dropped.
            processor.put(ARBITRARY_BATCH);
            assertEquals(blockingHandler.getDroppedEntries(), 1);
        }
        finally {
            blockingHandler.resumeProcessor();
            blockingHandler.unlock();
        }
    }

    @Test
    public void testContinueOnHandlerException()
            throws InterruptedException
    {
        BlockingBatchHandler blockingHandler = blockingHandlerThatThrowsException(new RuntimeException());
        BatchProcessor<EventBatch> processor = new AsyncBatchProcessor<>(
                "foo", blockingHandler, new BatchProcessorConfig().setQueueSize(100)
        );

        processor.start();

        blockingHandler.lock();
        try {
            processor.put(ARBITRARY_BATCH_A);

            assertTrue(blockingHandler.waitForProcessor(10));
            assertEquals(blockingHandler.getCallsToProcessBatch(), 1);
        }
        finally {
            blockingHandler.resumeProcessor();
            blockingHandler.unlock();
        }

        // When the processor unblocks, an exception (in its thread) will be thrown.
        // This should not affect the processor.
        blockingHandler.lock();
        try {
            processor.put(ARBITRARY_BATCH_B);
            assertTrue(blockingHandler.waitForProcessor(10));
            assertEquals(blockingHandler.getCallsToProcessBatch(), 2);
        }
        finally {
            blockingHandler.resumeProcessor();
            blockingHandler.unlock();
        }
    }

    @Test
    public void testStopsWhenStopCalled()
            throws InterruptedException
    {
        BlockingBatchHandler blockingHandler = blockingHandler();
        BatchProcessor<EventBatch> processor = new AsyncBatchProcessor<>(
                "foo", blockingHandler, new BatchProcessorConfig().setQueueSize(100)
        );

        processor.start();

        blockingHandler.lock();
        try {
            processor.put(ARBITRARY_BATCH_A);

            assertTrue(blockingHandler.waitForProcessor(10));

            // The processor hasn't been resumed. Stop it!
            processor.stop();
        }
        finally {
            blockingHandler.unlock();
        }

        try {
            processor.put(ARBITRARY_BATCH_B);
            fail();
        }
        catch (IllegalStateException ex) {
            assertEquals(ex.getMessage(), "Processor is not running");
        }
    }

    @Test
    public void testIgnoresExceptionsInHandler()
            throws InterruptedException
    {
        BlockingBatchHandler blockingHandler = blockingHandlerThatThrowsException(new NullPointerException());
        BatchProcessor<EventBatch> processor = new AsyncBatchProcessor<>(
                "foo", blockingHandler, new BatchProcessorConfig().setQueueSize(100)
        );
        processor.start();

        blockingHandler.lock();
        try {
            processor.put(ARBITRARY_BATCH_A);
            assertTrue(blockingHandler.waitForProcessor(10));
            blockingHandler.resumeProcessor();
        }
        finally {
            blockingHandler.unlock();
        }
        assertEquals(blockingHandler.getCallsToProcessBatch(), 1);

        blockingHandler.lock();
        try {
            processor.put(ARBITRARY_BATCH_B);
            assertTrue(blockingHandler.waitForProcessor(10));
            blockingHandler.resumeProcessor();
        }
        finally {
            blockingHandler.unlock();
        }
        assertEquals(blockingHandler.getCallsToProcessBatch(), 2);
    }

    private static Event event(String type)
    {
        return new Event(type, UUID.randomUUID().toString(), "localhost", DateTime.now(), Collections.<String, Object>emptyMap());
    }

    private static BlockingBatchHandler blockingHandler()
    {
        return new BlockingBatchHandler(new Runnable()
        {
            @Override
            public void run()
            {
            }
        });
    }

    private static BlockingBatchHandler blockingHandlerThatThrowsException(final RuntimeException exception)
    {
        return new BlockingBatchHandler(new Runnable()
        {
            @Override
            public void run()
            {
                throw exception;
            }
        });
    }

    private static class BlockingBatchHandler implements BatchHandler<EventBatch>
    {
        private final Lock lock = new ReentrantLock();
        private final Condition externalCondition = lock.newCondition();
        private final Condition internalCondition = lock.newCondition();
        private final Runnable onProcess;
        private long droppedEntries = 0;
        private long callsToProcessBatch = 0;

        public BlockingBatchHandler(Runnable onProcess)
        {
            this.onProcess = onProcess;
        }

        @Override
        public void processBatch(EventBatch entry)
        {
            // Wait for the right time to run
            lock.lock();
            callsToProcessBatch += 1;
            try {
                // Signal that we've started running
                externalCondition.signal();
                try {
                    // Block
                    internalCondition.await();
                }
                catch (InterruptedException ignored) {
                }
                onProcess.run();
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public void notifyEntriesDropped(int count)
        {
            droppedEntries += count;
        }

        public void lock()
        {
            lock.lock();
        }

        public void unlock()
        {
            lock.unlock();
        }

        public boolean waitForProcessor(long seconds)
                throws InterruptedException
        {
            return externalCondition.await(seconds, TimeUnit.SECONDS);
        }

        public void resumeProcessor()
        {
            internalCondition.signal();
        }

        public long getDroppedEntries()
        {
            return droppedEntries;
        }

        public long getCallsToProcessBatch()
        {
            return callsToProcessBatch;
        }
    }
}
