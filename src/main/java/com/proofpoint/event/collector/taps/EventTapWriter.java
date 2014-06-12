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

import com.proofpoint.event.collector.Event;
import com.proofpoint.event.collector.EventWriter;

import javax.inject.Inject;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventTapWriter implements EventWriter
{
    private final ActiveFlows activeFlows;

    @Inject
    EventTapWriter(ActiveFlows activeFlows)
    {
        this.activeFlows = checkNotNull(activeFlows, "activeFlows is null");
    }

    @Override
    public void write(Event event)
            throws IOException
    {
        for (Flow flow : activeFlows.getForType(event.getType())) {
            flow.enqueue(event);
        }
    }
}
