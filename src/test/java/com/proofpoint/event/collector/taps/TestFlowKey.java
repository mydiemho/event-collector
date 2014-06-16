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

import org.testng.annotations.Test;

import static com.proofpoint.testing.EquivalenceTester.equivalenceTester;

public class TestFlowKey
{
    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventType is null")
    public void testConstructorWithNullEventType()
    {
        new FlowKey(null, "flowId");
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "flowId is null")
    public void testConstructorWithNullFlowId()
    {
        new FlowKey("eventType", null);
    }

    @Test
    public void testEquivalence()
    {
        equivalenceTester().addEquivalentGroup(new FlowKey("Foo", "flowIdA"), new FlowKey("Foo", "flowIdA"))
                .addEquivalentGroup(new FlowKey("Bar", "flowIdA"))
                .addEquivalentGroup(new FlowKey("Bar", "flowIdB"));
    }
}
