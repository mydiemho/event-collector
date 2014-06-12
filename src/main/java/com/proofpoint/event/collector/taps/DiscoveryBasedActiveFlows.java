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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.proofpoint.discovery.client.DiscoveryLookupClient;
import com.proofpoint.discovery.client.ForDiscoveryClient;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceDescriptorsListener;
import com.proofpoint.discovery.client.ServiceDescriptorsUpdater;
import com.proofpoint.discovery.client.ServiceSelectorConfig;
import com.proofpoint.discovery.client.ServiceType;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.balancing.BalancingHttpClient;
import com.proofpoint.http.client.balancing.BalancingHttpClientConfig;
import com.proofpoint.http.client.balancing.HttpServiceBalancerImpl;
import com.proofpoint.http.client.balancing.HttpServiceBalancerStats;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.reporting.ReportCollectionFactory;
import org.weakref.jmx.ObjectNameBuilder;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

class DiscoveryBasedActiveFlows implements ActiveFlows
{
    static final String SERVICE_TYPE = "eventTap";
    private static final String STATS_NAME = new ObjectNameBuilder(HttpServiceBalancerStats.class.getPackage().getName())
            .withProperty("serviceType", SERVICE_TYPE)
            .build();

    private final AtomicReference<Multimap<String, Flow>> flows = new AtomicReference<>();
    private final ServiceDescriptorsUpdater serviceDescriptorsUpdater;
    private final String pool;
    private final BalancingHttpClientConfig config;
    private final HttpServiceBalancerStats httpServiceBalancerStats;
    private final HttpClient httpClient;
    private final FlowFactory flowFactory;
    private final boolean allowHttpClients;
    private Map<FlowKey, FlowInfo> flowInfo;

    @Inject
    DiscoveryBasedActiveFlows(
            @ServiceType(SERVICE_TYPE) ServiceSelectorConfig selectorConfig,
            NodeInfo nodeInfo,
            DiscoveryLookupClient lookupClient,
            @ForDiscoveryClient ScheduledExecutorService executor,
            @ForDiscoveryBasedActiveFlows HttpClient httpClient,
            @ForDiscoveryBasedActiveFlows BalancingHttpClientConfig httpClientConfig,
            ReportCollectionFactory reportCollectionFactory,
            FlowFactory flowFactory,
            DiscoveryBasedActiveFlowsConfig config)
    {
        this.pool = firstNonNull(checkNotNull(selectorConfig, "selectorConfig is null").getPool(), checkNotNull(nodeInfo, "nodeInfo is null").getPool());
        this.serviceDescriptorsUpdater = new ServiceDescriptorsUpdater(new ServiceDescriptorsListener()
        {
            @Override
            public void updateServiceDescriptors(Iterable<ServiceDescriptor> serviceDescriptors)
            {
                DiscoveryBasedActiveFlows.this.updateServiceDescriptors(serviceDescriptors);
            }
        }, SERVICE_TYPE, selectorConfig, nodeInfo, checkNotNull(lookupClient, "lookupClient is null"), checkNotNull(executor, "executor is null"));
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.config = checkNotNull(httpClientConfig, "httpClientConfig is null");
        this.httpServiceBalancerStats = checkNotNull(reportCollectionFactory, "reportCollectionFactory is null").createReportCollection(HttpServiceBalancerStats.class, STATS_NAME);
        this.flowFactory = checkNotNull(flowFactory, "flowFactory is null");
        this.allowHttpClients = checkNotNull(config, "config is null").isAllowHttpConsumers();
        this.flowInfo = ImmutableMap.of();
    }

    @PostConstruct
    void start()
    {
        serviceDescriptorsUpdater.start();
    }

    @Override
    public Collection<Flow> getForType(String type)
    {
        return flows.get().get(type);
    }

    private void updateServiceDescriptors(Iterable<ServiceDescriptor> serviceDescriptors)
    {
        Multimap<FlowKey, URI> activeFlows = extractActiveFlows(serviceDescriptors);
        ImmutableMultimap.Builder<String, Flow> flowBuilder = ImmutableMultimap.builder();
        ImmutableMap.Builder<FlowKey, FlowInfo> flowInfoBuilder = ImmutableMap.builder();

        synchronized (this) {
            for (Map.Entry<FlowKey, Collection<URI>> entry : activeFlows.asMap().entrySet()) {
                FlowKey key = entry.getKey();
                FlowInfo flowInfo = this.flowInfo.get(key);
                if (flowInfo == null) {
                    flowInfo = createNewFlow(key);
                }

                flowInfo.balancer.updateHttpUris(ImmutableSet.copyOf(entry.getValue()));
                flowInfoBuilder.put(key, flowInfo);
                flowBuilder.put(key.getEventType(), flowInfo.flow);
            }
            this.flowInfo = flowInfoBuilder.build();
            this.flows.set(flowBuilder.build());
        }
    }

    private FlowInfo createNewFlow(FlowKey key)
    {
        HttpServiceBalancerImpl balancer = new HttpServiceBalancerImpl(format("type=[%s], pool=[%s], eventType=[%s], flowId=[%s]", SERVICE_TYPE, pool, key.getEventType(), key.getFlowId()), httpServiceBalancerStats);
        BalancingHttpClient httpClient = new BalancingHttpClient(balancer, this.httpClient, config);
        return new FlowInfo(flowFactory.createHttpFlow(key.getEventType(), key.getFlowId(), httpClient), balancer);
    }

    private Multimap<FlowKey, URI> extractActiveFlows(Iterable<ServiceDescriptor> serviceDescriptors)
    {
        ImmutableMultimap.Builder<FlowKey, URI> builder = ImmutableMultimap.builder();
        for (ServiceDescriptor serviceDescriptor : serviceDescriptors) {
            String eventType = serviceDescriptor.getProperties().get("eventType");
            String flowId = serviceDescriptor.getProperties().get("flowId");
            if (Strings.isNullOrEmpty(eventType) || Strings.isNullOrEmpty(flowId)) {
                continue;
            }

            FlowKey key = new FlowKey(eventType, flowId);
            String https = serviceDescriptor.getProperties().get("https");
            if (https != null) {
                try {
                    builder.put(key, new URI(https));
                    continue;
                }
                catch (URISyntaxException ignored) {
                }
            }

            if (allowHttpClients) {
                String http = serviceDescriptor.getProperties().get("http");
                if (http != null) {
                    try {
                        builder.put(key, new URI(http));
                    }
                    catch (URISyntaxException ignored) {
                    }
                }
            }
        }

        return builder.build();
    }

    private static final class FlowInfo
    {
        final Flow flow;
        final HttpServiceBalancerImpl balancer;

        private FlowInfo(Flow flow, HttpServiceBalancerImpl balancer)
        {
            this.flow = checkNotNull(flow, "flow is null");
            this.balancer = checkNotNull(balancer, "balancer is null");
        }
    }
}
