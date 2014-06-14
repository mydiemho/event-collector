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
package com.proofpoint.event.collector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.event.collector.batch.BatcherConfig;
import com.proofpoint.event.collector.batch.EventBatch;
import com.proofpoint.reporting.testing.TestingReportCollectionFactory;
import com.proofpoint.testing.SerialScheduledExecutorService;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static com.proofpoint.event.collector.EventCollectorStats.EventStatus.UNSUPPORTED;
import static com.proofpoint.event.collector.EventCollectorStats.EventStatus.VALID;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestEventResource
{
    private static final String ARBITRARY_EVENT_TYPE = "fooType";
    private static final String ARBITRARY_EVENT_TYPE_A = ARBITRARY_EVENT_TYPE;
    private static final String ARBITRARY_EVENT_TYPE_B = "barType";
    private static final ImmutableMap<String,String> ARBITRARY_DATA = ImmutableMap.of("foo", "bar", "hello", "world");

    private InMemoryEventWriter writer;
    private EventCollectorStats eventCollectorStats;
    private TestingReportCollectionFactory testingReportCollectionFactory;
    private ScheduledExecutorService executorService;

    @BeforeMethod
    public void setup()
    {
        writer = new InMemoryEventWriter();
        testingReportCollectionFactory = new TestingReportCollectionFactory();
        eventCollectorStats = testingReportCollectionFactory.createReportCollection(EventCollectorStats.class);
        executorService = new SerialScheduledExecutorService();
    }

    @Test
    public void testPost()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes(ARBITRARY_EVENT_TYPE), new BatcherConfig().setMaxBatchSize(1), eventCollectorStats, executorService);
        resource.start();

        Event event = new Event(ARBITRARY_EVENT_TYPE, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event);
        EventBatch eventBatch = new EventBatch(ARBITRARY_EVENT_TYPE, events);
        Response response = resource.post(events);

        assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
        assertNull(response.getEntity());
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

        assertEquals(writer.getEventBatch(ARBITRARY_EVENT_TYPE), eventBatch);

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).incomingEvents(ARBITRARY_EVENT_TYPE, VALID);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        verify(reportCollection.incomingEvents(ARBITRARY_EVENT_TYPE, VALID)).add(1);
        verifyNoMoreInteractions(reportCollection.incomingEvents(ARBITRARY_EVENT_TYPE, VALID));
    }

    @Test
    public void testPostUnsupportedType()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes(ARBITRARY_EVENT_TYPE_A), new BatcherConfig().setMaxBatchSize(2), eventCollectorStats, executorService);
        resource.start();

        Event event1 = new Event(ARBITRARY_EVENT_TYPE_A, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event event2 = new Event(ARBITRARY_EVENT_TYPE_A, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event badEvent = new Event(ARBITRARY_EVENT_TYPE_B, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event1, event2, badEvent);
        Response response = resource.post(events);

        assertEquals(writer.getEventBatch(ARBITRARY_EVENT_TYPE_A), new EventBatch(ARBITRARY_EVENT_TYPE_A, ImmutableList.of(event1, event2)));
        assertNull(writer.getEventBatch(ARBITRARY_EVENT_TYPE_B));

        assertEquals(response.getStatus(), Status.BAD_REQUEST.getStatusCode());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity().toString().startsWith("Unsupported event type(s): "));
        assertTrue(response.getEntity().toString().contains(ARBITRARY_EVENT_TYPE_B));

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier, times(2)).incomingEvents(ARBITRARY_EVENT_TYPE_A, VALID);
        verify(argumentVerifier).incomingEvents(ARBITRARY_EVENT_TYPE_B, UNSUPPORTED);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        verify(reportCollection.incomingEvents(ARBITRARY_EVENT_TYPE_A, VALID), times(2)).add(1);
        verify(reportCollection.incomingEvents(ARBITRARY_EVENT_TYPE_B, UNSUPPORTED)).add(1);
        verifyNoMoreInteractions(reportCollection.incomingEvents(ARBITRARY_EVENT_TYPE, VALID));
        verifyNoMoreInteractions(reportCollection.incomingEvents(ARBITRARY_EVENT_TYPE_B, UNSUPPORTED));
    }

    @Test
    public void testAcceptAllEvents()
            throws IOException
    {
        String eventTypeA = UUID.randomUUID().toString();
        String eventTypeB = UUID.randomUUID().toString();

        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes(ImmutableList.of(eventTypeA, eventTypeB)), new BatcherConfig().setMaxBatchSize(1), eventCollectorStats, executorService);
        resource.start();

        Event eventWithTypeA = new Event(eventTypeA, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event eventWithTypeB = new Event(eventTypeB, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(eventWithTypeA, eventWithTypeB);
        Response response = resource.post(events);

        assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
        assertNull(response.getEntity());
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

        EventBatch eventBatchOfTypeA = new EventBatch(eventTypeA, ImmutableList.of(eventWithTypeA));
        EventBatch eventBatchOfTypeB = new EventBatch(eventTypeB, ImmutableList.of(eventWithTypeB));
        assertEquals(writer.getEventBatch(eventTypeA), eventBatchOfTypeA);
        assertEquals(writer.getEventBatch(eventTypeB), eventBatchOfTypeB);

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).incomingEvents(eventTypeA, VALID);
        verify(argumentVerifier).incomingEvents(eventTypeB, VALID);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        verify(reportCollection.incomingEvents(eventTypeA, VALID)).add(1);
        verify(reportCollection.incomingEvents(eventTypeB, VALID)).add(1);
        verifyNoMoreInteractions(reportCollection.incomingEvents(eventTypeA, VALID));
        verifyNoMoreInteractions(reportCollection.incomingEvents(eventTypeB, VALID));
    }
}