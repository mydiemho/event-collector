package com.proofpoint.event.collector.taps;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
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

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEquals;

public class TestDiscoveryBasedActiveFlows
{
    private ServiceSelectorConfig selectorConfig;
    private NodeInfo nodeInfo;
    private DiscoveryLookupClient lookupClient;
    private ScheduledExecutorService executor;
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
        executor = mock(ScheduledExecutorService.class);
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
        SerialScheduledExecutorService executor = new SerialScheduledExecutorService();
        SettableFuture<ServiceDescriptors> serviceDescriptorsFuture = SettableFuture.create();
        serviceDescriptorsFuture.set(
                new ServiceDescriptors("eventTap", "pool", 
                        ImmutableList.of(new ServiceDescriptor(UUID.randomUUID(), "nodeId", "eventTap", "pool", "location", ServiceState.RUNNING,
                                        ImmutableMap.<String, String>builder()
                                                .put("eventType", "Foo")
                                                .put("flowId", "flowId-A")
                                                .put("http", "http://foo.bar.com:8080/v1/event")
                                                .build())), new Duration(5, TimeUnit.SECONDS), "eTag"));
        when(lookupClient.getServices(eq("eventTap"), eq("pool"))).thenReturn(serviceDescriptorsFuture);
        Flow flowForFoo = mock(HttpFlow.class);
        when(flowFactory.createHttpFlow(eq("Foo"), eq("flowId-A"), any(BalancingHttpClient.class))).thenReturn(flowForFoo);
        DiscoveryBasedActiveFlows activeFlows = new DiscoveryBasedActiveFlows(selectorConfig, nodeInfo, lookupClient, executor, httpClient, httpClientConfig, reportCollectionFactory, flowFactory, config);
        activeFlows.start();
        executor.elapseTime(5, TimeUnit.SECONDS);
        Collection<Flow> flows = activeFlows.getForType("Foo");
        assertEquals(flows, ImmutableSet.of(flowForFoo));
    }
}
