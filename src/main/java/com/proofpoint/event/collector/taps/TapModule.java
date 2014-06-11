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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.PrivateBinder;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.proofpoint.event.collector.EventWriter;
import com.proofpoint.http.client.balancing.BalancingHttpClientConfig;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.proofpoint.configuration.ConfigurationModule.bindConfig;
import static com.proofpoint.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.proofpoint.http.client.HttpClientBinder.httpClientPrivateBinder;

public class TapModule implements Module
{
    @Override
    public void configure(Binder binder)
    {
        PrivateBinder privateBinder = binder.newPrivateBinder();

        discoveryBinder(binder).bindSelector(DiscoveryBasedActiveFlows.SERVICE_TYPE);
        bindConfig(privateBinder).to(DiscoveryBasedActiveFlowsConfig.class);
        privateBinder.bind(ActiveFlows.class).to(DiscoveryBasedActiveFlows.class);
        privateBinder.bind(DiscoveryBasedActiveFlows.class).in(SINGLETON);

        httpClientPrivateBinder(privateBinder, binder).bindHttpClient("event-taps", ForDiscoveryBasedActiveFlows.class);
        bindConfig(privateBinder).annotatedWith(ForDiscoveryBasedActiveFlows.class).prefixedWith("event-taps").to(BalancingHttpClientConfig.class);

        bindConfig(privateBinder).to(BatchProcessorConfig.class);
        privateBinder.bind(BatchProcessorFactory.class).to(BatchProcessorFactoryImpl.class);
        privateBinder.bind(BatchProcessorFactoryImpl.class).in(SINGLETON);

        privateBinder.install(new FactoryModuleBuilder()
                .implement(Flow.class, HttpFlow.class)
                .build(FlowFactory.class));

        privateBinder.bind(EventTapWriter.class).in(SINGLETON);
        privateBinder.expose(EventTapWriter.class);

        newSetBinder(binder, EventWriter.class).addBinding().to(EventTapWriter.class).in(SINGLETON);
    }
}
