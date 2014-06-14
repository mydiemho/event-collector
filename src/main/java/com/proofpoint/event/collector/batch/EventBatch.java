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
import com.proofpoint.event.collector.Event;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventBatch implements AutoCloseable
{
    private final List<Event> batch;

    private String type;

    public EventBatch(String type, List<Event> batch)
    {
        this.type = checkNotNull(type, "type is null");
        this.batch = ImmutableList.copyOf(checkNotNull(batch, "batch is null"));
    }

    public int size()
    {
        return batch.size();
    }

    public List<Event> getEvents()
    {
        return ImmutableList.copyOf(batch);
    }

    public String getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(batch, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final EventBatch other = (EventBatch) obj;
        return Objects.equals(this.batch, other.batch) && Objects.equals(this.type, other.type);
    }

    @Override
    public String toString()
    {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("batch", batch)
                .add("type", type)
                .toString();
    }

    @Override
    public void close()
    {
        // TODO: placeholder for extra features
    }
}