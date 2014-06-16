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

import com.proofpoint.event.collector.EventCollectorStats;
import com.proofpoint.event.collector.taps.BatchProcessor.BatchHandler;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class BatchProcessorFactoryImpl implements BatchProcessorFactory
{
    private final BatchProcessorConfig config;

    @Inject
    public BatchProcessorFactoryImpl(BatchProcessorConfig config)
    {
        this.config = checkNotNull(config, "config is null");
    }

    @Override
    public BatchProcessor createBatchProcessor(String eventType, String flowId, EventCollectorStats eventCollectorStats, BatchHandler batchHandler)
    {
        return new AsyncBatchProcessor(eventType, flowId, config, eventCollectorStats, batchHandler);
    }
}
