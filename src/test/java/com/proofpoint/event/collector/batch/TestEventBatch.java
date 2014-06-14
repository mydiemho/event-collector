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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.proofpoint.event.collector.Event;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static com.proofpoint.testing.EquivalenceTester.equivalenceTester;
import static java.util.UUID.randomUUID;

public class TestEventBatch
{
    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "type is null")
    public void testConstructorNullType()
    {
        new EventBatch(null, ImmutableList.<Event>of());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "batch is null")
    public void testConstructorNullBatch()
    {
        new EventBatch("foo", null);
    }

    @Test
    public void testEquivalence()
            throws Exception
    {
        String typeA = "a";
        String typeB = "b";
        Event event1OfTypeA = createEvent(typeA);
        Event event2OfTypeA = createEvent(typeA);

        equivalenceTester()
                .addEquivalentGroup(new EventBatch(typeA, ImmutableList.of(event1OfTypeA, event2OfTypeA)), new EventBatch(typeA, ImmutableList.of(event1OfTypeA, event2OfTypeA)))
                .addEquivalentGroup(new EventBatch(typeA, ImmutableList.of(event1OfTypeA)))
                .addEquivalentGroup(new EventBatch(typeB, ImmutableList.of(event1OfTypeA, event2OfTypeA)))
                .check();
    }

    private static Event createEvent(String type)
    {
        return new Event(type, randomUUID().toString(), "host", DateTime.now(), ImmutableMap.<String, Object>of());
    }
}
