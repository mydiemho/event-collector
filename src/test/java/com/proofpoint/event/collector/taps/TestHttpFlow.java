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

import com.proofpoint.event.collector.EventCollectorStats;
import com.proofpoint.http.client.balancing.BalancingHttpClient;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class TestHttpFlow
{
    private BalancingHttpClient httpClient;
    private BatchProcessorFactory batchProcessorFactory;
    private EventCollectorStats eventCollectorStats;

    @BeforeTest
    public void setup()
    {
        httpClient = mock(BalancingHttpClient.class);
        batchProcessorFactory = mock(BatchProcessorFactory.class);
        eventCollectorStats = mock(EventCollectorStats.class);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventType is null")
    public void testConstructorWithNullEventType()
    {
        new HttpFlow(null, "flowId", httpClient, batchProcessorFactory, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "flowId is null")
    public void testConstructorWithNullFlowId()
    {
        new HttpFlow("eventType", null, httpClient, batchProcessorFactory, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClient is null")
    public void testConstructorWithNullHttpClient()
    {
        new HttpFlow("eventType", "flowId", null, batchProcessorFactory, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "batchProcessorFactory is null")
    public void testConstructorWithNullBatchProcessorFactory()
    {
        new HttpFlow("eventType", "flowId", httpClient, null, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventCollectorStats is null")
    public void testConstructorWithNullEventCollectorStats()
    {
        new HttpFlow("eventType", "flowId", httpClient, batchProcessorFactory, null);
    }
}
