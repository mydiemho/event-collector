package com.proofpoint.event.collector.taps;

import java.util.Collection;

interface ActiveFlows
{
    Collection<Flow> getForType(String type);
}
