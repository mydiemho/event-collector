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

import com.google.common.collect.ImmutableMap;
import com.proofpoint.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static com.proofpoint.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.proofpoint.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.proofpoint.configuration.testing.ConfigAssertions.recordDefaults;
import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;
import static com.proofpoint.testing.ValidationAssertions.assertValidates;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestBatcherConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BatcherConfig.class)
                .setMaxBatchSize(1000)
                .setMaxProcessingDelay(new Duration(2, SECONDS)));

    }

    @Test
    public void testExplicitPropertyMappings()
    {
        assertFullMapping(
                new ImmutableMap.Builder<String, String>()
                        .put("collector.batcher.max-batch-size", "17")
                        .put("collector.batcher.max-processing-delay", "5s")
                        .build(),
                new BatcherConfig()
                        .setMaxBatchSize(17)
                        .setMaxProcessingDelay(new Duration(5, SECONDS)));
    }

    @Test
    public void testValidation()
    {
        BatcherConfig batcherConfig = new BatcherConfig()
                .setMaxBatchSize(0)
                .setMaxProcessingDelay(null);

        assertFailsValidation(batcherConfig, "maxBatchSize", "must be greater than or equal to 1", Min.class);
        assertFailsValidation(batcherConfig, "maxProcessingDelay", "may not be null", NotNull.class);

        assertValidates(new BatcherConfig().setMaxBatchSize(7).setMaxProcessingDelay(new Duration(6, SECONDS)));
    }
}
