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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.proofpoint.bootstrap.Bootstrap;
import com.proofpoint.discovery.client.testing.TestingDiscoveryModule;
import com.proofpoint.event.collector.EventCollectorStats;
import com.proofpoint.node.testing.TestingNodeModule;
import com.proofpoint.reporting.ReportingModule;
import org.testng.annotations.Test;
import org.weakref.jmx.ObjectNameBuilder;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import static com.proofpoint.reporting.ReportBinder.reportBinder;
import static org.mockito.Mockito.mock;

public class TestTapModule
{
    @Test
    public void testTapModule() throws Exception
    {
        ImmutableMap<String, String> config = ImmutableMap.<String, String>builder()
                .build();

        Bootstrap app = Bootstrap.bootstrapApplication("event-collector")
                .doNotInitializeLogging()
                .withModules(
                        new TapModule(),
                        new TestingDiscoveryModule(),
                        new TestingNodeModule(),
                        new ReportingModule(),
                        new MBeanModule(),
                        new Module()
                        {
                            @Override
                            public void configure(Binder binder)
                            {
                                binder.bind(MBeanServer.class).toInstance(mock(MBeanServer.class));
                                reportBinder(binder).bindReportCollection(EventCollectorStats.class).as(new ObjectNameBuilder(EventCollectorStats.class.getPackage().getName()).withProperty("type", "EventCollector").build());
                            }
                        });

        Injector injector = app
                .setRequiredConfigurationProperties(config)
                .initialize();
    }
}
