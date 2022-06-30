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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.prestosql.spi.*;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.IntArrayBlockBuilder;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

public class PMemoryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final PMemoryPagesStore pagesStore;
    private final PMemoryInternalNode currentNode;
    private final PageIndexerFactory pageIndexerFactory;

    @Inject
    public PMemoryPageSinkProvider(PMemoryPagesStore pagesStore, NodeManager nodeManager, PageIndexerFactory pageIndexerFactory)
    {
        this(pagesStore, new PMemoryInternalNode(requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getNodeIdentifier(),
                requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getHostAndPort(),
                requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getVersion(),
                requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().isCoordinator()), pageIndexerFactory);
    }

    @VisibleForTesting
    public PMemoryPageSinkProvider(PMemoryPagesStore pagesStore, PMemoryInternalNode currentNode, PageIndexerFactory pageIndexerFactory)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.currentNode = requireNonNull(currentNode, "currentHostAddress is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        PMemoryOutputTableHandle memoryOutputTableHandle = (PMemoryOutputTableHandle) outputTableHandle;
        long tableId = memoryOutputTableHandle.getTable();
        checkState(memoryOutputTableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(memoryOutputTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId);

        return new PMemoryPageSink(pagesStore, pageIndexerFactory, currentNode, tableId, memoryOutputTableHandle.getInputColumns(), memoryOutputTableHandle.getColumnTypes(), memoryOutputTableHandle.getBucketProperty());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        PMemoryInsertTableHandle memoryInsertTableHandle = (PMemoryInsertTableHandle) insertTableHandle;
        long tableId = memoryInsertTableHandle.getTable();
        checkState(memoryInsertTableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(memoryInsertTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId);
        return new PMemoryPageSink(pagesStore, pageIndexerFactory, currentNode, tableId, memoryInsertTableHandle.getInputColumns(), memoryInsertTableHandle.getColumnTypes(), memoryInsertTableHandle.getBucketProperty());
    }

    private static class PMemoryPageSink
            implements ConnectorPageSink
    {
        private final PMemoryPagesStore pagesStore;
        private final PMemoryInternalNode currentNode;
        private final long tableId;

        private Map<Integer, Long> addedRows;

        private final int[] bucketColumns;
        private final PMemoryBucketFunction bucketFunction;
        private final int[] dataColumnInputIndex; // ordinal of columns (not counting sample weight column)


        private final Optional<PMemoryBucketProperty> bucketProperty;

        private final PMemoryWriterPagePartitioner pagePartitioner;

        public PMemoryPageSink(
                PMemoryPagesStore pagesStore,
                PageIndexerFactory pageIndexerFactory,
                PMemoryInternalNode currentNode,
                long tableId,
                List<PMemoryColumnHandle> inputColumns,
                List<Type> columnTypes,
                Optional<PMemoryBucketProperty> bucketProperty)
        {
            this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
            this.currentNode = requireNonNull(currentNode, "currentHostAddress is null");
            this.tableId = tableId;

            this.addedRows = new HashMap<>();

            Map<String, Integer> dataColumnNameToIdMap = new HashMap<>();
            ImmutableList.Builder<Integer> dataColumnsInputIndex = ImmutableList.builder();
            // sample weight column is passed separately, so index must be calculated without this column
            int inputIndex;
            for (inputIndex = 0; inputIndex < inputColumns.size(); inputIndex++) {
                PMemoryColumnHandle column = inputColumns.get(inputIndex);
                dataColumnsInputIndex.add(inputIndex);
                dataColumnNameToIdMap.put(column.getName(), inputIndex);
            }

            this.bucketProperty = bucketProperty;

            this.dataColumnInputIndex = Ints.toArray(dataColumnsInputIndex.build());

            if (bucketProperty.isPresent()) {
                int bucketCount = bucketProperty.get().getBucketCount();
                bucketColumns = bucketProperty.get().getBucketedBy().stream()
                        .mapToInt(dataColumnNameToIdMap::get)
                        .toArray();
                bucketFunction = new PMemoryBucketFunction(bucketCount);
            }
            else {
                bucketColumns = null;
                bucketFunction = null;
            }

            this.pagePartitioner = new PMemoryWriterPagePartitioner(
                    pageIndexerFactory,
                    inputColumns,
                    columnTypes,
                    bucketProperty.isPresent());
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            //no bucketing, simply append to bucket -1
            if (!bucketProperty.isPresent()) {
                pagesStore.add(tableId, page);

                if (!addedRows.containsKey(-1)) {
                    addedRows.put(-1, 0L);
                }

                addedRows.put(-1, addedRows.get(-1) + page.getPositionCount());
            } else {
                //partition and write to multiple buckets
                writePage(page);
            }

            return NOT_BLOCKED;
        }

        private void writePage(Page page)
        {
            //for each row, get a writer index (bucket id)
            int[] writerIndexes = getWriterIndexes(page);

            // position count for each writer
            int[] sizes = new int[bucketProperty.get().getBucketCount()];
            for (int index : writerIndexes) {
                if (index >= bucketProperty.get().getBucketCount()) {
                    throw new UnsupportedOperationException("index >= bucketCount" + index);
                }
                sizes[index]++;
            }

            // record which positions are used by which writer
            int[][] writerPositions = new int[bucketProperty.get().getBucketCount()][];
            int[] counts = new int[bucketProperty.get().getBucketCount()];

            for (int position = 0; position < page.getPositionCount(); position++) {
                int index = writerIndexes[position];

                int count = counts[index];
                if (count == 0) {
                    writerPositions[index] = new int[sizes[index]];
                }
                writerPositions[index][count] = position;
                counts[index] = count + 1;
            }

            // invoke the writers
            Page dataPage = getDataPage(page);
            for (int index = 0; index < writerPositions.length; index++) {
                int[] positions = writerPositions[index];
                if (positions == null) {
                    continue;
                }

                // If write is partitioned across multiple writers, filter page using dictionary blocks
                Page pageForWriter = dataPage;
                if (positions.length != dataPage.getPositionCount()) {
                    verify(positions.length == counts[index]);
                    pageForWriter = pageForWriter.getPositions(positions, 0, positions.length);
                }
                //add to bucket with id index
                pagesStore.add(tableId, pageForWriter, OptionalInt.of(index));

                if (!addedRows.containsKey(index)) {
                    addedRows.put(index, 0L);
                }

                addedRows.put(index, addedRows.get(index) + pageForWriter.getPositionCount());
            }
        }

        private int[] getWriterIndexes(Page page)
        {
            int[] bucketBlock = buildBucketBlock(page);
            return bucketBlock;
        }

        private Page getDataPage(Page page)
        {
            Block[] blocks = new Block[dataColumnInputIndex.length];

            for (int i = 0; i < dataColumnInputIndex.length; i++) {
                int dataColumn = dataColumnInputIndex[i];
                blocks[i] = page.getBlock(dataColumn);
            }
            return new Page(page.getPositionCount(), blocks);
        }

        private int[] buildBucketBlock(Page page)
        {
            int[] bucketColumn = new int[page.getPositionCount()];
            Page bucketColumnsPage = extractColumns(page, bucketColumns);
            for (int position = 0; position < page.getPositionCount(); position++) {
                int bucket = bucketFunction.getBucket(bucketColumnsPage, position);
                bucketColumn[position] = bucket;
            }
            return bucketColumn;
        }

        private static Page extractColumns(Page page, int[] columns)
        {
            Block[] blocks = new Block[columns.length];
            for (int i = 0; i < columns.length; i++) {
                int dataColumn = columns[i];
                blocks[i] = page.getBlock(dataColumn);
            }
            return new Page(page.getPositionCount(), blocks);
        }

        private static class PMemoryWriterPagePartitioner
        {
            private final PageIndexer pageIndexer;

            public PMemoryWriterPagePartitioner(
                    PageIndexerFactory pageIndexerFactory,
                    List<PMemoryColumnHandle> inputColumns,
                    List<Type> types,
                    boolean bucketed)
            {
                requireNonNull(inputColumns, "inputColumns is null");
                requireNonNull(types, "types is null");
                requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

                List<Type> partitionColumnTypes = new ArrayList<>();

                if (bucketed) {
                    partitionColumnTypes.add(INTEGER);
                }

                this.pageIndexer = pageIndexerFactory.createPageIndexer(partitionColumnTypes);
            }

            public int[] partitionPage(Block bucketBlock)
            {
                Page partitionColumns = new Page(bucketBlock.getPositionCount(), new Block[0]);
                if (bucketBlock != null) {
                    Block[] blocks = new Block[1];
                    blocks[0] = bucketBlock;
                    partitionColumns = new Page(partitionColumns.getPositionCount(), blocks);
                }
                return pageIndexer.indexPage(partitionColumns);
            }

            public int getMaxIndex()
            {
                return pageIndexer.getMaxIndex();
            }
        }


        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            return completedFuture(ImmutableList.of(new PMemoryDataFragment(currentNode.getHostAndPort(), addedRows).toSlice()));
        }

        @Override
        public void abort()
        {
        }
    }
}
