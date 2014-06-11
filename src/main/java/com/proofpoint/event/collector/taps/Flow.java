package com.proofpoint.event.collector.taps;

import com.proofpoint.event.collector.Event;

interface Flow
{
    void enqueue(Event event);
}
