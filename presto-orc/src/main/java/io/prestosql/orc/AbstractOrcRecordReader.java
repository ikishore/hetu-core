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
package io.prestosql.orc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.Closer;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.hetu.core.common.algorithm.SequenceUtils;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.MetadataReader;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.metadata.PostScript.HiveWriterVersion;
import io.prestosql.orc.metadata.StripeInformation;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.StripeStatistics;
import io.prestosql.orc.reader.AbstractColumnReader;
import io.prestosql.orc.reader.CachingColumnReader;
import io.prestosql.orc.reader.DataCachingSelectiveColumnReader;
import io.prestosql.orc.reader.ResultCachingSelectiveColumnReader;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.StreamSourceMeta;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexLookUpException;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.FixedWidthType;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static io.prestosql.orc.OrcReader.BATCH_SIZE_GROWTH_FACTOR;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static io.prestosql.orc.OrcWriteValidation.WriteChecksumBuilder.createWriteChecksumBuilder;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

abstract class AbstractOrcRecordReader<T extends AbstractColumnReader>
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AbstractOrcRecordReader.class).instanceSize();
    private static final Logger log = Logger.get(AbstractOrcRecordReader.class);
    private final OrcDataSource orcDataSource;

    private T[] columnReaders;
    protected long[] currentBytesPerCell;
    protected long[] maxBytesPerCell;
    protected long maxCombinedBytesPerRow;

    private final long totalRowCount;
    private final long splitLength;
    protected final long maxBlockBytes;
    protected long currentPosition;
    protected long currentStripePosition;
    protected int[] matchingRowsInBatchArray;
    protected int currentBatchSize;
    protected int nextBatchSize;
    protected int maxBatchSize = MAX_BATCH_SIZE;
    protected boolean currentStripeFinished;

    protected boolean pageMetadataEnabled;

    protected final List<StripeInformation> stripes;
    private final StripeReader stripeReader;
    protected int currentStripe = -1;
    private AggregatedMemoryContext currentStripeSystemMemoryContext;

    private final long fileRowCount;
    private final List<Long> stripeFilePositions;
    private long filePosition;

    private Iterator<RowGroup> rowGroups = ImmutableList.<RowGroup>of().iterator();
    private int currentRowGroup = -1;
    private long currentGroupRowCount;
    private int nextRowInGroup;

    protected final Map<String, Slice> userMetadata;

    private final AggregatedMemoryContext systemMemoryUsage;

    protected final OrcBlockFactory blockFactory;

    private final Optional<OrcWriteValidation> writeValidation;
    protected final Optional<OrcWriteValidation.WriteChecksumBuilder> writeChecksumBuilder;
    protected final Optional<OrcWriteValidation.StatisticsValidation> rowGroupStatisticsValidation;
    protected final Optional<OrcWriteValidation.StatisticsValidation> stripeStatisticsValidation;
    protected final Optional<OrcWriteValidation.StatisticsValidation> fileStatisticsValidation;

    Map<StripeInformation, PeekingIterator<Integer>> stripeMatchingRows = new HashMap<>();

    public AbstractOrcRecordReader(
            List<OrcColumn> readColumns,
            List<Type> readTypes,
            OrcPredicate predicate,
            long numberOfRows,
            List<StripeInformation> fileStripes,
            Optional<ColumnMetadata<ColumnStatistics>> fileStats,
            List<Optional<StripeStatistics>> stripeStats,
            OrcDataSource inputOrcDataSource,
            long splitOffset,
            long splitLength,
            ColumnMetadata<OrcType> orcTypes,
            Optional<OrcDecompressor> decompressor,
            int rowsInRowGroup,
            DateTimeZone legacyFileTimeZone,
            HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            Map<String, Slice> userMetadata,
            AggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize,
            Function<Exception, RuntimeException> exceptionTransform,
            Optional<List<IndexMetadata>> indexes,
            Map<String, Domain> domains,
            OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties,
            Map<String, List<Domain>> orDomains,
            boolean pageMetadataEnabled)
            throws OrcCorruptionException
    {
        requireNonNull(readColumns, "readColumns is null");
        checkArgument(readColumns.stream().distinct().count() == readColumns.size(), "readColumns contains duplicate entries");
        requireNonNull(readTypes, "readTypes is null");
        checkArgument(readColumns.size() == readTypes.size(), "readColumns and readTypes must have the same size");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(fileStripes, "fileStripes is null");
        requireNonNull(stripeStats, "stripeStats is null");
        requireNonNull(inputOrcDataSource, "orcDataSource is null");
        requireNonNull(orcTypes, "types is null");
        requireNonNull(decompressor, "decompressor is null");
        requireNonNull(userMetadata, "userMetadata is null");
        requireNonNull(systemMemoryUsage, "systemMemoryUsage is null");
        requireNonNull(exceptionTransform, "exceptionTransform is null");

        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        this.writeChecksumBuilder = writeValidation.map(validation -> createWriteChecksumBuilder(orcTypes, readTypes));
        this.rowGroupStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(orcTypes, readTypes));
        this.stripeStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(orcTypes, readTypes));
        this.fileStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(orcTypes, readTypes));
        this.systemMemoryUsage = systemMemoryUsage.newAggregatedMemoryContext();
        this.blockFactory = new OrcBlockFactory(exceptionTransform, true);

        this.maxBlockBytes = requireNonNull(maxBlockSize, "maxBlockSize is null").toBytes();

        this.pageMetadataEnabled = pageMetadataEnabled;

        // it is possible that old versions of orc use 0 to mean there are no row groups
        checkArgument(rowsInRowGroup > 0, "rowsInRowGroup must be greater than zero");
        checkArgument(orDomains != null, "orDomain map cannot be null");

        // sort stripes by file position
        List<StripeInfo> stripeInfos = new ArrayList<>();
        for (int i = 0; i < fileStripes.size(); i++) {
            Optional<StripeStatistics> stats = Optional.empty();
            // ignore all stripe stats if too few or too many
            if (stripeStats.size() == fileStripes.size()) {
                stats = stripeStats.get(i);
            }
            stripeInfos.add(new StripeInfo(fileStripes.get(i), stats));
        }
        stripeInfos.sort(comparingLong(info -> info.getStripe().getOffset()));

        // assumptions made about the index:
        // 1. they are all bitmap indexes
        // 2. the index split offset corresponds to the stripe offset

        // each stripe could have an index for multiple columns
        Map<Long, List<IndexMetadata>> stripeOffsetToIndex = new HashMap<>();
        if (indexes.isPresent() && !indexes.get().isEmpty()
                // check there is only one type of index
                && indexes.get().stream().map(i -> i.getIndex().getId()).collect(Collectors.toSet()).size() == 1) {
            for (IndexMetadata i : indexes.get()) {
                long offset = i.getSplitStart();

                stripeOffsetToIndex.putIfAbsent(offset, new LinkedList<>());

                List<IndexMetadata> stripeIndexes = stripeOffsetToIndex.get(offset);
                stripeIndexes.add(i);
            }
        }

        /* Stripe Filtering:
         *      Stripes in the given spilt are checked using additional metadata like Statistics, Index; whichever
         *  available and applied to eliminate reading and matching of the stripes which do not contain the data.
         *  Stats: minmax, null counts etc are applied here to check if any of the predicate conditions match
         *  Heuristic Index: bitmap,bloom are applied to check and confirm if the stripe has matching records
         *      as per the predicates; if not then eliminated.
         *
         *  Additionally, if BITMAP Index is present; the rows within a given rowGroup matching the Conjunct domains
         *  are selected and are guaranteed to match the predicate.
         */
        long localTotalRowCount = 0;
        long localFileRowCount = 0;
        ImmutableList.Builder<StripeInformation> localStripes = ImmutableList.builder();
        Map<StripeInformation, List<IndexMetadata>> stripeIndexes = new HashMap<>();
        ImmutableList.Builder<Long> localStripeFilePositions = ImmutableList.builder();
        if (!fileStats.isPresent() || predicate.matches(numberOfRows, fileStats.get())) {
            // select stripes that start within the specified split
            for (int i = 0; i < stripeInfos.size(); i++) {
                StripeInfo info = stripeInfos.get(i);
                StripeInformation stripe = info.getStripe();
                if (splitContainsStripe(splitOffset, splitLength, stripe)
                        && isStripeIncluded(stripe, info.getStats(), predicate)
                        && !filterStripeUsingIndex(stripe, stripeOffsetToIndex, domains, orDomains)) {
                    localStripes.add(stripe);
                    localStripeFilePositions.add(localFileRowCount);
                    localTotalRowCount += stripe.getNumberOfRows();
                }
                localFileRowCount += stripe.getNumberOfRows();
            }
        }
        this.totalRowCount = localTotalRowCount;
        this.stripes = localStripes.build();
        this.stripeFilePositions = localStripeFilePositions.build();

        OrcDataSource localOrcDataSource = inputOrcDataSource;
        localOrcDataSource = wrapWithCacheIfTinyStripes(localOrcDataSource, this.stripes, maxMergeDistance, tinyStripeThreshold);
        this.orcDataSource = localOrcDataSource;
        this.splitLength = splitLength;

        this.fileRowCount = stripeInfos.stream()
                .map(StripeInfo::getStripe)
                .mapToLong(StripeInformation::getNumberOfRows)
                .sum();

        this.userMetadata = ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));

        this.currentStripeSystemMemoryContext = this.systemMemoryUsage.newAggregatedMemoryContext();
        // The streamReadersSystemMemoryContext covers the StreamReader local buffer sizes, plus leaf node columnReaders'
        // instance sizes who use local buffers. SliceDirectStreamReader's instance size is not counted, because it
        // doesn't have a local buffer. All non-leaf level columnReaders' (e.g. MapStreamReader, LongStreamReader,
        // ListStreamReader and StructStreamReader) instance sizes were not counted, because calling setBytes() in
        // their constructors is confusing.
        AggregatedMemoryContext streamReadersSystemMemoryContext = this.systemMemoryUsage.newAggregatedMemoryContext();
        stripeReader = new StripeReader(
                localOrcDataSource,
                legacyFileTimeZone.toTimeZone().toZoneId(),
                decompressor,
                orcTypes,
                ImmutableSet.copyOf(readColumns),
                rowsInRowGroup,
                predicate,
                hiveWriterVersion,
                metadataReader,
                writeValidation,
                orcCacheStore,
                orcCacheProperties);

        OptionalInt fixedWidthRowSize = getFixedWidthRowSize(readTypes);
        if (fixedWidthRowSize.isPresent() && fixedWidthRowSize.getAsInt() != 0) {
            nextBatchSize = adjustMaxBatchSize(fixedWidthRowSize.getAsInt());
        }
        else {
            nextBatchSize = initialBatchSize;
        }
    }

    private boolean filterStripeUsingIndex(StripeInformation stripe, Map<Long, List<IndexMetadata>> stripeOffsetToIndex,
            Map<String, Domain> and, Map<String, List<Domain>> or)
    {
        if (stripeOffsetToIndex.isEmpty()) {
            return false;
        }

        List<IndexMetadata> stripeIndex = stripeOffsetToIndex.get(Long.valueOf(stripe.getOffset()));
        Map<Index, Domain> andDomainMap = new HashMap<>();
        Map<Index, Domain> orDomainMap = new HashMap<>();

        for (Map.Entry<String, Domain> domainEntry : and.entrySet()) {
            String columnName = domainEntry.getKey();
            Domain columnDomain = domainEntry.getValue();

            // if the index exists, there should only be one index for this column within this stripe
            List<IndexMetadata> indexMetadata = stripeIndex.stream().filter(p -> p.getColumns()[0].equalsIgnoreCase(columnName)).collect(Collectors.toList());
            if (indexMetadata.isEmpty() || indexMetadata.size() > 1) {
                continue;
            }

            Index index = indexMetadata.get(0).getIndex();
            andDomainMap.put(index, columnDomain);
        }

        for (Map.Entry<String, List<Domain>> domainEntry : or.entrySet()) {
            String columnName = domainEntry.getKey();
            List<Domain> columnDomain = domainEntry.getValue();

            // if the index exists, there should only be one index for this column within this stripe
            List<IndexMetadata> indexMetadata = stripeIndex.stream().filter(p -> p.getColumns()[0].equalsIgnoreCase(columnName)).collect(Collectors.toList());
            if (indexMetadata.isEmpty() || indexMetadata.size() > 1) {
                continue;
            }

            Index index = indexMetadata.get(0).getIndex();
            orDomainMap.put(index, columnDomain.get(0));
        }

        if (!andDomainMap.isEmpty()) {
            List<Iterator<Integer>> matchings = new ArrayList<>(andDomainMap.size());
            for (Map.Entry<Index, Domain> e : andDomainMap.entrySet()) {
                try {
                    Iterator<Integer> lookUpRes = e.getKey().lookUp(e.getValue());
                    if (lookUpRes != null) {
                        matchings.add(lookUpRes);
                    }
                    else if (!e.getKey().matches(e.getValue())) {
                        return true;
                    }
                }
                catch (UnsupportedOperationException | IndexLookUpException uoe2) {
                    return false;
                }
            }
            if (!matchings.isEmpty()) {
                Iterator<Integer> thisStripeMatchingRows = SequenceUtils.intersect(matchings);
                PeekingIterator<Integer> peekingIterator = Iterators.peekingIterator(thisStripeMatchingRows);
                this.stripeMatchingRows.put(stripe, peekingIterator);
            }
            return false;
        }
        if (!orDomainMap.isEmpty()) {
            for (Map.Entry<Index, Domain> e : orDomainMap.entrySet()) {
                try {
                    Iterator<Integer> thisStripeMatchingRows = e.getKey().lookUp(e.getValue());
                    if (thisStripeMatchingRows != null) {
                        if (thisStripeMatchingRows.hasNext()) {
                            /* any one matched; then include the stripe */
                            return false;
                        }
                    }
                    else if (e.getKey().matches(e.getValue())) {
                        return false;
                    }
                }
                catch (UnsupportedOperationException | IndexLookUpException uoe2) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    private static OptionalInt getFixedWidthRowSize(List<Type> columnTypes)
    {
        int totalFixedWidth = 0;
        for (Type type : columnTypes) {
            if (type instanceof FixedWidthType) {
                // add 1 byte for null flag
                totalFixedWidth += ((FixedWidthType) type).getFixedSize() + 1;
            }
            else {
                return OptionalInt.empty();
            }
        }

        return OptionalInt.of(totalFixedWidth);
    }

    protected void blockLoaded(int columnIndex, Block block)
    {
        if (block.getPositionCount() <= 0) {
            return;
        }

        currentBytesPerCell[columnIndex] += block.getSizeInBytes() / currentBatchSize;
        if (maxBytesPerCell[columnIndex] < currentBytesPerCell[columnIndex]) {
            long delta = currentBytesPerCell[columnIndex] - maxBytesPerCell[columnIndex];
            maxCombinedBytesPerRow += delta;
            maxBytesPerCell[columnIndex] = currentBytesPerCell[columnIndex];
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / maxCombinedBytesPerRow)));
        }
    }

    /**
     * Returns the sum of the largest cells in size from each column
     */
    public long getMaxCombinedBytesPerRow()
    {
        return maxCombinedBytesPerRow;
    }

    protected void updateMaxCombinedBytesPerRow(int columnIndex, Block block) // TODO: Is it required
    {
        if (block.getPositionCount() > 0) {
            long bytesPerCell = block.getSizeInBytes() / block.getPositionCount();
            if (maxBytesPerCell[columnIndex] < bytesPerCell) {
                maxCombinedBytesPerRow = maxCombinedBytesPerRow - maxBytesPerCell[columnIndex] + bytesPerCell;
                maxBytesPerCell[columnIndex] = bytesPerCell;
                adjustMaxBatchSize(maxCombinedBytesPerRow);
            }
        }
    }

    protected T[] getColumnReaders()
    {
        return columnReaders;
    }

    private static boolean splitContainsStripe(long splitOffset, long splitLength, StripeInformation stripe)
    {
        long splitEndOffset = splitOffset + splitLength;
        return splitOffset <= stripe.getOffset() && stripe.getOffset() < splitEndOffset;
    }

    private static boolean isStripeIncluded(
            StripeInformation stripe,
            Optional<StripeStatistics> stripeStats,
            OrcPredicate predicate)
    {
        // if there are no stats, include the column
        return stripeStats
                .map(StripeStatistics::getColumnStatistics)
                .map(columnStats -> predicate.matches(stripe.getNumberOfRows(), columnStats))
                .orElse(true);
    }

    @VisibleForTesting
    static OrcDataSource wrapWithCacheIfTinyStripes(OrcDataSource dataSource, List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize tinyStripeThreshold)
    {
        if (dataSource instanceof CachingOrcDataSource) {
            return dataSource;
        }
        for (StripeInformation stripe : stripes) {
            if (stripe.getTotalLength() > tinyStripeThreshold.toBytes()) {
                return dataSource;
            }
        }
        return new CachingOrcDataSource(dataSource, LinearProbeRangeFinder.createTinyStripesRangeFinder(stripes, maxMergeDistance, tinyStripeThreshold));
    }

    /**
     * Return the row position relative to the start of the file.
     */
    public long getFilePosition()
    {
        return filePosition;
    }

    /**
     * Returns the total number of rows in the file. This count includes rows
     * for stripes that were completely excluded due to stripe statistics.
     */
    public long getFileRowCount()
    {
        return fileRowCount;
    }

    /**
     * Return the row position within the stripes being read by this reader.
     * This position will include rows that were never read due to row groups
     * that are excluded due to row group statistics. Thus, it will advance
     * faster than the number of rows actually read.
     */
    public long getReaderPosition()
    {
        return currentPosition;
    }

    /**
     * Returns the total number of rows that can possibly be read by this reader.
     * This count may be fewer than the number of rows in the file if some
     * stripes were excluded due to stripe statistics, but may be more than
     * the number of rows read if some row groups are excluded due to statistics.
     */
    public long getReaderRowCount()
    {
        return totalRowCount;
    }

    public long getSplitLength()
    {
        return splitLength;
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(orcDataSource);
            for (AbstractColumnReader column : columnReaders) {
                if (column != null) {
                    closer.register(column::close);
                }
            }
        }

        if (writeChecksumBuilder.isPresent()) {
            OrcWriteValidation.WriteChecksum actualChecksum = writeChecksumBuilder.get().build();
            validateWrite(validation -> validation.getChecksum().getTotalRowCount() == actualChecksum.getTotalRowCount(), "Invalid row count");
            List<Long> columnHashes = actualChecksum.getColumnHashes();
            for (int i = 0; i < columnHashes.size(); i++) {
                int columnIndex = i;
                validateWrite(validation -> validation.getChecksum().getColumnHashes().get(columnIndex).equals(columnHashes.get(columnIndex)),
                        "Invalid checksum for column %s", columnIndex);
            }
            validateWrite(validation -> validation.getChecksum().getStripeHash() == actualChecksum.getStripeHash(), "Invalid stripes checksum");
        }
        if (fileStatisticsValidation.isPresent()) {
            Optional<ColumnMetadata<ColumnStatistics>> columnStatistics = fileStatisticsValidation.get().build();
            writeValidation.get().validateFileStatistics(orcDataSource.getId(), columnStatistics);
        }
    }

    public Map<String, Slice> getUserMetadata()
    {
        return ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));
    }

    protected int getNextRowInGroup()
    {
        return nextRowInGroup;
    }

    protected void batchRead(int batchSize)
    {
        nextRowInGroup += batchSize;
    }

    protected int adjustMaxBatchSize(long averageRowBytes)
    {
        maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / averageRowBytes)));
        return maxBatchSize;
    }

    protected int prepareNextBatch()
            throws IOException
    {
        currentStripeFinished = false;
        // update position for current row group (advancing resets them)
        filePosition += currentBatchSize;
        currentPosition += currentBatchSize;

        // if next row is within the current group return
        if (nextRowInGroup >= currentGroupRowCount) {
            // attempt to advance to next row group
            if (!advanceToNextRowGroup()) {
                filePosition = fileRowCount;
                currentPosition = totalRowCount;
                return -1;
            }
        }

        // We will grow currentBatchSize by BATCH_SIZE_GROWTH_FACTOR starting from initialBatchSize to maxBatchSize or
        // the number of rows left in this rowgroup, whichever is smaller. maxBatchSize is adjusted according to the
        // block size for every batch and never exceed MAX_BATCH_SIZE. But when the number of rows in the last batch in
        // the current rowgroup is smaller than min(nextBatchSize, maxBatchSize), the nextBatchSize for next batch in
        // the new rowgroup should be grown based on min(nextBatchSize, maxBatchSize) but not by the number of rows in
        // the last batch, i.e. currentGroupRowCount - nextRowInGroup. For example, if the number of rows read for
        // single fixed width column are: 1, 16, 256, 1024, 1024,..., 1024, 256 and the 256 was because there is only
        // 256 rows left in this row group, then the nextBatchSize should be 1024 instead of 512. So we need to grow the
        // nextBatchSize before limiting the currentBatchSize by currentGroupRowCount - nextRowInGroup.
        currentBatchSize = toIntExact(min(nextBatchSize, maxBatchSize));
        nextBatchSize = min(currentBatchSize * BATCH_SIZE_GROWTH_FACTOR, MAX_BATCH_SIZE);
        currentBatchSize = toIntExact(min(currentBatchSize, currentGroupRowCount - nextRowInGroup));

        // row groups read finished, so going to next stripe
        if (!rowGroups.hasNext() && (nextRowInGroup + currentBatchSize >= currentGroupRowCount)) {
            currentStripeFinished = true;
        }

        return currentBatchSize;
    }

    protected Boolean isCurrentStripeFinished()
    {
        return currentStripeFinished;
    }

    private void advanceToNextStripe()
            throws IOException
    {
        currentStripeSystemMemoryContext.close();
        currentStripeSystemMemoryContext = systemMemoryUsage.newAggregatedMemoryContext();
        rowGroups = ImmutableList.<RowGroup>of().iterator();

        if (currentStripe >= 0) {
            if (stripeStatisticsValidation.isPresent()) {
                OrcWriteValidation.StatisticsValidation statisticsValidation = stripeStatisticsValidation.get();
                long offset = stripes.get(currentStripe).getOffset();
                writeValidation.get().validateStripeStatistics(orcDataSource.getId(), offset, statisticsValidation.build().get());
                statisticsValidation.reset();
            }
        }

        currentStripe++;
        if (currentStripe >= stripes.size()) {
            return;
        }

        if (currentStripe > 0) {
            currentStripePosition += stripes.get(currentStripe - 1).getNumberOfRows();
        }

        StripeInformation stripeInformation = stripes.get(currentStripe);
        validateWriteStripe(stripeInformation.getNumberOfRows());

        Stripe stripe = stripeReader.readStripe(stripeInformation, currentStripeSystemMemoryContext);
        if (stripe != null) {
            // Give readers access to dictionary streams
            InputStreamSources dictionaryStreamSources = stripe.getDictionaryStreamSources();
            ColumnMetadata<ColumnEncoding> columnEncodings = stripe.getColumnEncodings();
            for (AbstractColumnReader columnReader : columnReaders) {
                if (columnReader != null) {
                    ZoneId fileTimeZone = stripe.getFileTimeZone();
                    columnReader.startStripe(fileTimeZone,
                            dictionaryStreamSources, columnEncodings);
                }
            }

            rowGroups = stripe.getRowGroups().iterator();
        }
    }

    private void validateWrite(Predicate<OrcWriteValidation> test, String messageFormat, Object... args)
            throws OrcCorruptionException
    {
        if (writeValidation.isPresent() && !test.test(writeValidation.get())) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }

    private void validateWriteStripe(int rowCount)
    {
        writeChecksumBuilder.ifPresent(builder -> builder.addStripe(rowCount));
    }

    protected void validateWritePageChecksum(Page page)
    {
        if (writeChecksumBuilder.isPresent()) {
            writeChecksumBuilder.get().addPage(page);
            rowGroupStatisticsValidation.get().addPage(page);
            stripeStatisticsValidation.get().addPage(page);
            fileStatisticsValidation.get().addPage(page);
        }
    }

    protected void setColumnReadersParam(T[] columnReaders)
    {
        this.columnReaders = columnReaders;
        currentBytesPerCell = new long[columnReaders.length];
        maxBytesPerCell = new long[columnReaders.length];
    }

    private boolean advanceToNextRowGroup()
            throws IOException
    {
        nextRowInGroup = 0;

        if (currentRowGroup >= 0) {
            if (rowGroupStatisticsValidation.isPresent()) {
                OrcWriteValidation.StatisticsValidation statisticsValidation = rowGroupStatisticsValidation.get();
                long offset = stripes.get(currentStripe).getOffset();
                writeValidation.get().validateRowGroupStatistics(orcDataSource.getId(), offset, currentRowGroup, statisticsValidation.build().get());
                statisticsValidation.reset();
            }
        }
        while (!rowGroups.hasNext() && currentStripe < stripes.size()) {
            advanceToNextStripe();
            currentRowGroup = -1;
        }

        if (!rowGroups.hasNext()) {
            currentGroupRowCount = 0;
            return false;
        }

        currentRowGroup++;
        RowGroup localCurrentRowGroup = rowGroups.next();
        currentGroupRowCount = localCurrentRowGroup.getRowCount();
        if (localCurrentRowGroup.getMinAverageRowBytes() > 0) {
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / localCurrentRowGroup.getMinAverageRowBytes())));
        }

        currentPosition = currentStripePosition + localCurrentRowGroup.getRowOffset();
        filePosition = stripeFilePositions.get(currentStripe) + localCurrentRowGroup.getRowOffset();

        // give reader data streams from row group
        InputStreamSources rowGroupStreamSources = localCurrentRowGroup.getStreamSources();
        for (AbstractColumnReader columnReader : columnReaders) {
            if (columnReader != null) {
                if (columnReader instanceof CachingColumnReader
                        || columnReader instanceof ResultCachingSelectiveColumnReader
                        || columnReader instanceof DataCachingSelectiveColumnReader) {
                    StreamSourceMeta streamSourceMeta = new StreamSourceMeta();
                    streamSourceMeta.setDataSourceId(orcDataSource.getId());
                    streamSourceMeta.setLastModifiedTime(orcDataSource.getLastModifiedTime());
                    streamSourceMeta.setStripeOffset(stripes.get(currentStripe).getOffset());
                    streamSourceMeta.setRowGroupOffset(localCurrentRowGroup.getRowOffset());
                    streamSourceMeta.setRowCount(localCurrentRowGroup.getRowCount());
                    rowGroupStreamSources.setStreamSourceMeta(streamSourceMeta);
                }
                columnReader.startRowGroup(rowGroupStreamSources);
            }
        }

        return true;
    }

    /**
     * @return The size of memory retained by all the stream readers (local buffers + object overhead)
     */
    @VisibleForTesting
    long getStreamReaderRetainedSizeInBytes()
    {
        long totalRetainedSizeInBytes = 0;
        for (AbstractColumnReader columnReader : columnReaders) {
            if (columnReader != null) {
                totalRetainedSizeInBytes += columnReader.getRetainedSizeInBytes();
            }
        }
        return totalRetainedSizeInBytes;
    }

    /**
     * @return The size of memory retained by the current stripe (excludes object overheads)
     */
    @VisibleForTesting
    long getCurrentStripeRetainedSizeInBytes()
    {
        return currentStripeSystemMemoryContext.getBytes();
    }

    /**
     * @return The total size of memory retained by this OrcRecordReader
     */
    @VisibleForTesting
    long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getStreamReaderRetainedSizeInBytes() + getCurrentStripeRetainedSizeInBytes();
    }

    /**
     * @return The system memory reserved by this OrcRecordReader. It does not include non-leaf level StreamReaders'
     * instance sizes.
     */
    @VisibleForTesting
    long getSystemMemoryUsage()
    {
        return systemMemoryUsage.getBytes();
    }

    private static class StripeInfo
    {
        private final StripeInformation stripe;
        private final Optional<StripeStatistics> stats;

        public StripeInfo(StripeInformation stripe, Optional<StripeStatistics> stats)
        {
            this.stripe = requireNonNull(stripe, "stripe is null");
            this.stats = requireNonNull(stats, "metadata is null");
        }

        public StripeInformation getStripe()
        {
            return stripe;
        }

        public Optional<StripeStatistics> getStats()
        {
            return stats;
        }
    }

    static class LinearProbeRangeFinder
            implements CachingOrcDataSource.RegionFinder
    {
        private final List<DiskRange> diskRanges;
        private int index;

        private LinearProbeRangeFinder(List<DiskRange> diskRanges)
        {
            this.diskRanges = diskRanges;
        }

        @Override
        public DiskRange getRangeFor(long desiredOffset)
        {
            // Assumption: range are always read in order
            // Assumption: bytes that are not part of any range are never read
            while (index < diskRanges.size()) {
                DiskRange range = diskRanges.get(index);
                if (range.getEnd() > desiredOffset) {
                    checkArgument(range.getOffset() <= desiredOffset);
                    return range;
                }
                index++;
            }
            throw new IllegalArgumentException("Invalid desiredOffset " + desiredOffset);
        }

        public static LinearProbeRangeFinder createTinyStripesRangeFinder(List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize tinyStripeThreshold)
        {
            if (stripes.isEmpty()) {
                return new LinearProbeRangeFinder(ImmutableList.of());
            }

            List<DiskRange> scratchDiskRanges = stripes.stream()
                    .map(stripe -> new DiskRange(stripe.getOffset(), toIntExact(stripe.getTotalLength())))
                    .collect(Collectors.toList());
            List<DiskRange> localDiskRanges = mergeAdjacentDiskRanges(scratchDiskRanges, maxMergeDistance, tinyStripeThreshold);

            return new LinearProbeRangeFinder(localDiskRanges);
        }
    }
}
