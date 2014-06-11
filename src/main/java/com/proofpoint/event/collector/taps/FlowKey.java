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
