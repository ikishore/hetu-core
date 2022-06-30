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
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static io.prestosql.plugin.pmemory.PMemoryErrorCode.MEMORY_LIMIT_EXCEEDED;
import static io.prestosql.plugin.pmemory.PMemoryErrorCode.MISSING_DATA;
import static java.lang.String.format;

@ThreadSafe
public class PMemoryPagesStore
{
    private final long maxBytes;

    @GuardedBy("this")
    private long currentBytes;

    private final Map<Long, TableData> tables = new HashMap<>();

    @Inject
    public PMemoryPagesStore(PMemoryConfig config)
    {
        this.maxBytes = config.getMaxDataPerNode().toBytes();
    }

    public synchronized void initialize(long tableId)
    {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData());
        }
    }

    //add page / non-bucketed
    public synchronized void add(Long tableId, Page page)
    {
        add(tableId, page, OptionalInt.empty());
    }

    public synchronized void add(Long tableId, Page page, OptionalInt bucketNumber)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        page.compact();

        long newSize = currentBytes + page.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new PrestoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        TableData tableData = tables.get(tableId);
        //specify bucket to add to
        tableData.add(page, bucketNumber);
    }

    public synchronized List<Page> getPages(
            Long tableId,
            int partNumber,
            int totalParts,
            List<Integer> columnIndexes,
            long expectedRows,
            OptionalInt bucketNumber,
            OptionalLong limit,
            OptionalDouble sampleRatio)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        TableData tableData = tables.get(tableId);
        if (tableData.getRows() < expectedRows) {
            throw new PrestoException(MISSING_DATA,
                    format("Expected to find [%s] rows on a worker, but found [%s].", expectedRows, tableData.getRows()));
        }

        //restrict the pages that are returned, before partitioning between workers
        List<Page> eligibleTableData = tableData.getPages(bucketNumber);

        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();

        boolean done = false;
        long totalRows = 0;
        for (int i = partNumber; i < eligibleTableData.size() && !done; i += totalParts) {
            if (sampleRatio.isPresent() && ThreadLocalRandom.current().nextDouble() >= sampleRatio.getAsDouble()) {
                continue;
            }

            Page page = eligibleTableData.get(i);
            totalRows += page.getPositionCount();
            if (limit.isPresent() && totalRows > limit.getAsLong()) {
                page = page.getRegion(0, (int) (page.getPositionCount() - (totalRows - limit.getAsLong())));
                done = true;
            }
            partitionedPages.add(getColumns(page, columnIndexes));
        }

        return partitionedPages.build();
    }

    public synchronized boolean contains(Long tableId)
    {
        return tables.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when PMemoryPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which PMemoryTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we can not determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, TableData>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, TableData> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                for (Page removedPage : tablePagesEntry.getValue().getPages()) {
                    currentBytes -= removedPage.getRetainedSizeInBytes();
                }
                tableDataIterator.remove();
            }
        }
    }

    private static Page getColumns(Page page, List<Integer> columnIndexes)
    {
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = page.getBlock(columnIndexes.get(i));
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }

    private static final class TableData
    {
        //store pages for each bucket separately (only one entry if not bucketed
        private final Map<Integer, List<Page>> pages = new HashMap<>();
        private long rows;

        public void add(Page page, OptionalInt bucketNumber)
        {
            Integer bucketNumberFinal = (bucketNumber.isPresent()) ? bucketNumber.getAsInt() : -1;

            if (!pages.containsKey(bucketNumberFinal)) {
                pages.put(bucketNumberFinal, new ArrayList<>());
            }

            pages.get(bucketNumberFinal).add(page);
            rows += page.getPositionCount();
        }

        private List<Page> getPages(OptionalInt bucketNumber)
        {
            if (!bucketNumber.isPresent()) {
                return getPages();
            }

            return pages.get(bucketNumber.getAsInt());
        }

        private List<Page> getPages()
        {
            List<Page> allPages = new ArrayList<>();

            for (Map.Entry<Integer, List<Page>> entry : pages.entrySet()) {
                allPages.addAll(entry.getValue());
            }

            return allPages;
        }

        private long getRows()
        {
            return rows;
        }
    }
}
