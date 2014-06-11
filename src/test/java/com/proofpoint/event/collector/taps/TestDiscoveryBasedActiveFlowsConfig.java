package com.proofpoint.event.collector.taps;

import com.google.common.collect.ImmutableMap;
import com.proofpoint.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static com.proofpoint.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.proofpoint.configuration.testing.ConfigAssertions.recordDefaults;

public class TestDiscoveryBasedActiveFlowsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DiscoveryBasedActiveFlowsConfig.class)
                .setAllowHttpConsumers(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("collector.event-tap.allow-http-consumers", "false")
                .build();

        DiscoveryBasedActiveFlowsConfig expected = new DiscoveryBasedActiveFlowsConfig()
                .setAllowHttpConsumers(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
