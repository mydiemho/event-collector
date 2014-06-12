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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import com.proofpoint.discovery.client.DiscoveryLookupClient;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceDescriptors;
import com.proofpoint.discovery.client.ServiceSelectorConfig;
import com.proofpoint.discovery.client.ServiceState;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.balancing.BalancingHttpClient;
import com.proofpoint.http.client.balancing.BalancingHttpClientConfig;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.reporting.ReportCollectionFactory;
import com.proofpoint.reporting.testing.TestingReportCollectionFactory;
import com.proofpoint.testing.SerialScheduledExecutorService;
import com.proofpoint.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestDiscoveryBasedActiveFlows
{
    private static final String EVENT_TAP_SERVICE = "eventTap";
    private static final String EVENT_TYPE_KEY = "eventType";
    private static final String POOL_KEY = "pool";
    private static final String FLOWID_KEY = "flowId";
    private static final String HTTP_KEY = "http";
    private static final String NODEID_KEY = "nodeId";
    private static final String LOCATION_KEY = "location";
    private static final String ETAG_KEY = "eTag";
    private static final String EVENT_TYPE_FOO = "Foo";
    private ServiceSelectorConfig selectorConfig;
    private NodeInfo nodeInfo;
    private DiscoveryLookupClient lookupClient;
    private SerialScheduledExecutorService executor;
    private HttpClient httpClient;
    private BalancingHttpClientConfig httpClientConfig;
    private ReportCollectionFactory reportCollectionFactory;
    private FlowFactory flowFactory;
    private DiscoveryBasedActiveFlowsConfig config;

    @BeforeMethod
    public void setup()
    {
        selectorConfig = mock(ServiceSelectorConfig.class);
        nodeInfo = mock(NodeInfo.class);
        lookupClient = mock(DiscoveryLookupClient.class);
        executor = new SerialScheduledExecutorService();
        httpClient = mock(HttpClient.class);
        httpClientConfig = new BalancingHttpClientConfig();
        reportCollectionFactory = new TestingReportCollectionFactory();
        flowFactory = mock(FlowFactory.class);
        config = new DiscoveryBasedActiveFlowsConfig();

        when(selectorConfig.getPool()).thenReturn("pool");
    }

    @Test
    public void testConstructorSuccess()
    {
        new DiscoveryBasedActiveFlows(selectorConfig, nodeInfo, lookupClient, executor, httpClient, httpClientConfig, reportCollectionFactory, flowFactory, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "selectorConfig is null")
    public void testConstructorWithSelectorConfigNull()
    {
        new DiscoveryBasedActiveFlows(null, nodeInfo, lookupClient, executor, httpClient, httpClientConfig, reportCollectionFactory, flowFactory, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "nodeInfo is null")
    public void testConstructorWithNodeInfoNull()
    {
        when(selectorConfig.getPool()).thenReturn(null);
        new DiscoveryBasedActiveFlows(selectorConfig, null, lookupClient, executor, httpClient, httpClientConfig, reportCollectionFactory, flowFactory, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "lookupClient is null")
    public void testConstructorWithLookupClientNull()
    {
        new DiscoveryBasedActiveFlows(selectorConfig, nodeInfo, null, executor, httpClient, httpClientConfig, reportCollectionFactory, flowFactory, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "executor is null")
    public void testConstructorWithExecutorNull()
    {
        new DiscoveryBasedActiveFlows(selectorConfig, nodeInfo, lookupClient, null, httpClient, httpClientConfig, reportCollectionFactory, flowFactory, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClient is null")
    public void testConstructorWithHttpClientNull()
    {
        new DiscoveryBasedActiveFlows(selectorConfig, nodeInfo, lookupClient, executor, null, httpClientConfig, reportCollectionFactory, flowFactory, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClientConfig is null")
    public void testConstructorWithHttpClientConfigNull()
    {
        new DiscoveryBasedActiveFlows(selectorConfig, nodeInfo, lookupClient, executor, httpClient, null, reportCollectionFactory, flowFactory, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "reportCollectionFactory is null")
    public void testConstructorWithReportCollectionFactoryNull()
    {
        new DiscoveryBasedActiveFlows(selectorConfig, nodeInfo, lookupClient, executor, httpClient, httpClientConfig, null, flowFactory, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "flowFactory is null")
    public void testConstructorWithFlowFactoryNull()
    {
        new DiscoveryBasedActiveFlows(selectorConfig, nodeInfo, lookupClient, executor, httpClient, httpClientConfig, reportCollectionFactory, null, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorWithConfigNull()
    {
        new DiscoveryBasedActiveFlows(selectorConfig, nodeInfo, lookupClient, executor, httpClient, httpClientConfig, reportCollectionFactory, flowFactory, null);
    }

    @Test
    public void testGetForType()
    {
        SettableFuture<ServiceDescriptors> serviceDescriptorsFuture = SettableFuture.create();
        serviceDescriptorsFuture.set(
                new ServiceDescriptors(EVENT_TAP_SERVICE, POOL_KEY,
                        ImmutableList.of(serviceDescriptor(EVENT_TYPE_FOO, "flowId-A"), serviceDescriptor(EVENT_TYPE_FOO, "flowId-B")), new Duration(5, TimeUnit.SECONDS), ETAG_KEY));
        when(lookupClient.getServices(eq(EVENT_TAP_SERVICE), eq(POOL_KEY))).thenReturn(serviceDescriptorsFuture);
        Flow flowA = setupFlow(EVENT_TYPE_FOO, "flowId-A");
        Flow flowB = setupFlow(EVENT_TYPE_FOO, "flowId-B");
        DiscoveryBasedActiveFlows activeFlows = new DiscoveryBasedActiveFlows(selectorConfig, nodeInfo, lookupClient, executor, httpClient, httpClientConfig, reportCollectionFactory, flowFactory, config);
        activeFlows.start();
        assertEquals(activeFlows.getForType(EVENT_TYPE_FOO), ImmutableSet.of(flowA, flowB));
    }

    private ServiceDescriptor serviceDescriptor(String eventType, String flowId)
    {
        return new ServiceDescriptor(UUID.randomUUID(), NODEID_KEY, EVENT_TAP_SERVICE, POOL_KEY, LOCATION_KEY, ServiceState.RUNNING,
                ImmutableMap.<String, String>builder()
                .put(EVENT_TYPE_KEY, eventType)
                .put(FLOWID_KEY, flowId)
                .put(HTTP_KEY, "http://foo.bar.com:8080/v1/event")
                .build());
    }

    private Flow setupFlow(String eventType, String flowId)
    {
        Flow flow = mock(HttpFlow.class);
        when(flowFactory.createHttpFlow(eq(eventType), eq(flowId), any(BalancingHttpClient.class))).thenReturn(flow);
        return flow;
    }
}
