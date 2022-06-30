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
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class PMemoryTableHandle
        implements ConnectorTableHandle
{
    private final long id;
    private final String tableName;
    private final OptionalLong limit;
    private final OptionalDouble sampleRatio;
    private final TupleDomain<ColumnHandle> predicate;
    private final Optional<PMemoryBucketHandle> bucketHandle;

    public PMemoryTableHandle(long id, String name, Optional<PMemoryBucketHandle> bucketHandle)
    {
        this(id, name, OptionalLong.empty(), OptionalDouble.empty(), TupleDomain.all(), bucketHandle);
    }

    @JsonCreator
    public PMemoryTableHandle(
            @JsonProperty("id") long id,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("sampleRatio") OptionalDouble sampleRatio,
            @JsonProperty("predicate") TupleDomain<ColumnHandle> predicate,
            @JsonProperty("bucketHandle") Optional<PMemoryBucketHandle> bucketHandle)
    {
        this.id = id;
        this.tableName = tableName;
        this.limit = requireNonNull(limit, "limit is null");
        this.sampleRatio = requireNonNull(sampleRatio, "sampleRatio is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public long getId()
    {
        return id;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public OptionalDouble getSampleRatio()
    {
        return sampleRatio;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public Optional<PMemoryBucketHandle> getBucketHandle()
    {
        return bucketHandle;
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
        PMemoryTableHandle that = (PMemoryTableHandle) o;
        return id == that.id &&
                limit.equals(that.limit) &&
                sampleRatio.equals(that.sampleRatio);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, limit, sampleRatio);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(id);
        limit.ifPresent(value -> builder.append("(limit:" + value + ")"));
        sampleRatio.ifPresent(value -> builder.append("(sampleRatio:" + value + ")"));
        bucketHandle.ifPresent(value -> builder.append("(bucketHandle:" + value + ")"));
        return builder.toString();
    }
}
