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

import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeUtils;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class PMemoryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final PMemoryPagesStore pagesStore;

    @Inject
    public PMemoryPageSourceProvider(PMemoryPagesStore pagesStore, TypeManager typeManager, PMemoryMetadata memoryMetadata)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns)
    {
        return createPageSource(transaction, session, split, table, columns, Optional.empty());
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            Optional<DynamicFilterSupplier> dynamicFilterSupplier)
    {
        List<PMemorySplit> memorySplits = ((PMemorySplitWrapper) split).getSplits();

        if (memorySplits.size() != 1) {
            throw new UnsupportedOperationException("not implemented yet");
        }

        long tableId = memorySplits.get(0).getTable();
        int partNumber = memorySplits.get(0).getPartNumber();
        int totalParts = memorySplits.get(0).getTotalPartsPerWorker();
        long expectedRows = memorySplits.get(0).getExpectedRows();
        PMemoryTableHandle memoryTable = (PMemoryTableHandle) table;
        OptionalDouble sampleRatio = memoryTable.getSampleRatio();

        //add bucket number information

        List<Integer> columnIndexes = columns.stream()
                                             .map(PMemoryColumnHandle.class::cast)
                                             .map(PMemoryColumnHandle::getColumnIndex).collect(toList());
        List<Page> pages = pagesStore.getPages(
                tableId,
                partNumber,
                totalParts,
                columnIndexes,
                expectedRows,
                memorySplits.get(0).getBucketNumber(),
                memorySplits.get(0).getLimit(),
                sampleRatio);
        return new FixedPageSource(pages.stream()
                                        .map(page -> applyFilter(page, dynamicFilterSupplier, columns))
                                        .collect(toList()));
    }

    private Page applyFilter(Page page, Optional<DynamicFilterSupplier> dynamicFilters, List<ColumnHandle> columns)
    {
        if (!dynamicFilters.isPresent()) {
            return page;
        }
        int[] positions = new int[page.getPositionCount()];
        int length = 0;
        for (int i = 0; i < page.getPositionCount(); ++i) {
            boolean match = true;
            for (Map.Entry<ColumnHandle, DynamicFilter> entry : dynamicFilters.get().getDynamicFilters().entrySet()) {
                PMemoryColumnHandle columnHandle = (PMemoryColumnHandle) entry.getKey();
                DynamicFilter dynamicFilter = entry.getValue();
                Object value = TypeUtils.readNativeValue(columnHandle.getType(), page.getBlock(columns.indexOf(columnHandle)), i);
                if (!dynamicFilter.contains(value)) {
                    match = false;
                }
            }
            if (match) {
                positions[length++] = i;
            }
        }
        return page.getPositions(positions, 0, length);
    }
}
