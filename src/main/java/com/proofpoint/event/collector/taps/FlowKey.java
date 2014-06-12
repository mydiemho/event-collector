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

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

class FlowKey
{
    private final String eventType;
    private final String flowId;

    FlowKey(String eventType, String flowId)
    {
        this.eventType = checkNotNull(eventType, "eventType is null");
        this.flowId = checkNotNull(flowId, "flowId is null");
    }

    String getFlowId()
    {
        return flowId;
    }

    String getEventType()
    {
        return eventType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(eventType, flowId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final FlowKey other = (FlowKey) obj;
        return Objects.equal(this.eventType, other.eventType) && Objects.equal(this.flowId, other.flowId);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("eventType", eventType)
                .add("flowId", flowId)
                .toString();
    }
}
