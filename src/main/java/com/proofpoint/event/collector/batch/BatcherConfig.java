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
package com.proofpoint.event.collector.batch;

import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;
import com.proofpoint.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class BatcherConfig
{
    private int maxBatchSize = 1000;
    private Duration maxProcessingDelay = new Duration(2, TimeUnit.SECONDS);

    @Min(1)
    public int getMaxBatchSize()
    {
        return maxBatchSize;
    }

    @Config("collector.batcher.max-batch-size")
    @ConfigDescription("The maximum number of events to include in a single batch posted to a given event tap.")
    public BatcherConfig setMaxBatchSize(int maxBatchSize)
    {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    @NotNull
    public Duration getMaxProcessingDelay()
    {
        return maxProcessingDelay;
    }

    @Config("collector.batcher.max-processing-delay")
    @ConfigDescription("The interval between batching incoming events.")
    public BatcherConfig setMaxProcessingDelay(Duration maxProcessingDelay)
    {
        this.maxProcessingDelay = maxProcessingDelay;
        return this;
    }
}
