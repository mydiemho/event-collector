package com.proofpoint.event.collector.taps;

import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;

public class DiscoveryBasedActiveFlowsConfig
{
    private boolean allowHttpConsumers = true;

    public boolean isAllowHttpConsumers()
    {
        return allowHttpConsumers;
    }

    @Config("collector.event-tap.allow-http-consumers")
    @ConfigDescription("Permit events to be sent to http-only consumers.")
    public DiscoveryBasedActiveFlowsConfig setAllowHttpConsumers(boolean allowHttpConsumers)
    {
        this.allowHttpConsumers = allowHttpConsumers;
        return this;
    }
}
