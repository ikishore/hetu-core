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
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class PMemoryDataFragment
{
    private static final JsonCodec<PMemoryDataFragment> MEMORY_DATA_FRAGMENT_CODEC = jsonCodec(PMemoryDataFragment.class);

    private final HostAddress hostAddress;
    //store the metadata for each bucket separately in (bucketId, rows)pairs
    //if there is no bucketing, there is one entry with bucketId=-1
    private final Map<Integer, Long> rows;

    @JsonCreator
    public PMemoryDataFragment(
            @JsonProperty("hostAndPort") HostAddress hostAddress,
            @JsonProperty("rows") Map<Integer, Long> rows)
    {
        this.hostAddress = requireNonNull(hostAddress, "hostAddress is null");
        //checkArgument(rows >= 0, "Rows number can not be negative");
        this.rows = ImmutableMap.copyOf(rows);
    }

    @JsonProperty
    public HostAddress getHostAndPort()
    {
        return hostAddress;
    }

    @JsonProperty
    public Map<Integer, Long> getRows()
    {
        return rows;
    }

    public Slice toSlice()
    {
        return Slices.wrappedBuffer(MEMORY_DATA_FRAGMENT_CODEC.toJsonBytes(this));
    }

    public static PMemoryDataFragment fromSlice(Slice fragment)
    {
        return MEMORY_DATA_FRAGMENT_CODEC.fromJson(fragment.getBytes());
    }

    //merge fragments for each bucket
    public static PMemoryDataFragment merge(PMemoryDataFragment a, PMemoryDataFragment b)
    {
        checkArgument(a.getHostAndPort().equals(b.getHostAndPort()), "Can not merge fragments from different hosts");

        Map<Integer, Long> mergedRows = new HashMap<>();

        for (Map.Entry<Integer, Long> entry : a.getRows().entrySet()) {
            if (!mergedRows.containsKey(entry.getKey())) {
                mergedRows.put(entry.getKey(), 0L);
            }

            mergedRows.put(entry.getKey(), mergedRows.get(entry.getKey()) + entry.getValue());
        }

        for (Map.Entry<Integer, Long> entry : b.getRows().entrySet()) {
            if (!mergedRows.containsKey(entry.getKey())) {
                mergedRows.put(entry.getKey(), 0L);
            }

            mergedRows.put(entry.getKey(), mergedRows.get(entry.getKey()) + entry.getValue());
        }

        return new PMemoryDataFragment(a.getHostAndPort(), mergedRows);
    }
}
