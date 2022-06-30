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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;

public final class PMemorySplitManager
        implements ConnectorSplitManager
{
    private final int splitsPerNode;
    private final PMemoryMetadata metadata;

    @Inject
    public PMemorySplitManager(PMemoryConfig config, PMemoryMetadata metadata)
    {
        this.splitsPerNode = config.getSplitsPerNode();
        this.metadata = metadata;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle handle, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        PMemoryTableHandle table = (PMemoryTableHandle) handle;

        List<PMemoryDataFragment> dataFragments = metadata.getDataFragments(table.getId());

        int totalRows = 0;

        ImmutableList.Builder<PMemorySplit> splits = ImmutableList.builder();

        for (PMemoryDataFragment dataFragment : dataFragments) {
            Map<Integer, Long> rowsPerBucket = dataFragment.getRows();

            //do this calculation to avoid having too many splits
            int splitsPerBucket = (rowsPerBucket.size() > 0) ? (splitsPerNode + rowsPerBucket.size() - 1)/rowsPerBucket.size() : splitsPerNode;

            for (Map.Entry<Integer, Long> entry : rowsPerBucket.entrySet()) {
                Integer bucket = entry.getKey();
                long rows = entry.getValue();
                totalRows += rows;

                if (table.getLimit().isPresent() && totalRows > table.getLimit().getAsLong()) {
                    rows -= totalRows - table.getLimit().getAsLong();
                    splits.add(new PMemorySplit(table.getId(), 0, 1, dataFragment.getHostAndPort(), rows, OptionalLong.of(rows), OptionalInt.empty()));
                    break;
                }

                //create the splits for the partition
                for (int i = 0; i < splitsPerBucket; i++) {
                    splits.add(new PMemorySplit(table.getId(), i, splitsPerBucket, dataFragment.getHostAndPort(), rows, OptionalLong.empty(), (bucket >= 0) ? OptionalInt.of(bucket) : OptionalInt.empty()));
                }
            }
        }
        return new PMemorySplitSource(splits.build());
    }
}
