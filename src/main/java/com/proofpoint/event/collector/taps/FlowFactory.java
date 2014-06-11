package com.proofpoint.event.collector.taps;

import com.google.inject.assistedinject.Assisted;
import com.proofpoint.http.client.balancing.BalancingHttpClient;

interface FlowFactory
{
    Flow createHttpFlow(@Assisted("eventType") String eventType, @Assisted("flowId") String flowId,
            @Assisted BalancingHttpClient httpClient);
}
