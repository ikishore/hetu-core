/*
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
package io.prestosql.plugin.pmemory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PMemoryPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final long tableId;
    private final int bucketCount;
    private final boolean forUpdateOrDelete;

    public PMemoryPartitioningHandle(long tableId, int bucketCount)
    {
        this(tableId, bucketCount, false);
    }

    @JsonCreator
    public PMemoryPartitioningHandle(
            @JsonProperty("tableId") long tableId,
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("forUpdate") boolean forUpdateOrDelete)
    {
        this.tableId = tableId;
        this.bucketCount = bucketCount;
        this.forUpdateOrDelete = forUpdateOrDelete;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public boolean isForUpdateOrDelete()
    {
        return forUpdateOrDelete;
    }

    @Override
    public String toString()
    {
        return format("buckets=%s", bucketCount);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PMemoryPartitioningHandle that = (PMemoryPartitioningHandle) o;
        return bucketCount == that.bucketCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketCount);
    }
}
