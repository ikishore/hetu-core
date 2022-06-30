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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PMemoryBucketHandle
{
    //bucketing columns
    private final List<PMemoryColumnHandle> columns;
    // Number of buckets in the table, as specified in table metadata
    private final int tableBucketCount;

    @JsonCreator
    public PMemoryBucketHandle(
            @JsonProperty("columns") List<PMemoryColumnHandle> columns,
            @JsonProperty("tableBucketCount") int tableBucketCount)
    {
        this.columns = requireNonNull(columns, "columns is null");
        this.tableBucketCount = tableBucketCount;
    }

    @JsonProperty
    public List<PMemoryColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public int getTableBucketCount()
    {
        return tableBucketCount;
    }
}
