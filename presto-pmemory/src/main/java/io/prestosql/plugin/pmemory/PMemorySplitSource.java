package io.prestosql.plugin.pmemory;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.collect.Maps.transformValues;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class PMemorySplitSource
    implements ConnectorSplitSource
{
    private final List<PMemorySplit> splits;
    private int offset;
    private final Map<Integer, Integer> offsetPerBatch;

    public PMemorySplitSource(Iterable<? extends PMemorySplit> splits)
    {
        this.offsetPerBatch = new HashMap<>();
        requireNonNull(splits, "splits is null");
        List<PMemorySplit> splitsList = new ArrayList<>();
        for (PMemorySplit split : splits) {
            splitsList.add(split);

            if (split.getBucketNumber().isPresent()) {
                offsetPerBatch.put(split.getBucketNumber().getAsInt(), 0);
            } else {
                offsetPerBatch.put(-1, 0);
            }
        }
        this.splits = Collections.unmodifiableList(splitsList);
    }

    @SuppressWarnings("ObjectEquality")
    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        OptionalInt bucketNumber = toBucketNumber(partitionHandle);

        //partitioned request, restrict to eligible splits
        if (bucketNumber.isPresent()) {
            List<PMemorySplit> eligibleSplits = splits
                    .stream()
                    .filter(split -> split.getBucketNumber().isPresent() && split.getBucketNumber().getAsInt() == bucketNumber.getAsInt())
                    .collect(Collectors.toList());

            int remainingSplits = eligibleSplits.size() - offsetPerBatch.get(bucketNumber.getAsInt());
            int size = Math.min(remainingSplits, maxSize);
            List<PMemorySplit> returnedSplits = eligibleSplits.subList(offsetPerBatch.get(bucketNumber.getAsInt()), offsetPerBatch.get(bucketNumber.getAsInt()) + size);
            offsetPerBatch.put(bucketNumber.getAsInt(), offsetPerBatch.get(bucketNumber.getAsInt()) + size);
            offset += size;

            List<ConnectorSplit> results = returnedSplits.stream()
                    .map(split -> PMemorySplitWrapper.wrap((PMemorySplit) split))
                    .collect(Collectors.toList());

            return completedFuture(new ConnectorSplitBatch(results, isFinished()));
        }

        //run for all splits
        int remainingSplits = splits.size() - offset;
        int size = Math.min(remainingSplits, maxSize);
        List<PMemorySplit> returnedSplits = splits.subList(offset, offset + size);
        offset += size;

        List<ConnectorSplit> results = returnedSplits.stream()
                .map(split -> PMemorySplitWrapper.wrap((PMemorySplit) split))
                .collect(Collectors.toList());

        return completedFuture(new ConnectorSplitBatch(results, isFinished()));
    }

    private static OptionalInt toBucketNumber(ConnectorPartitionHandle partitionHandle)
    {
        if (partitionHandle == NOT_PARTITIONED) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(((PMemoryPartitionHandle) partitionHandle).getBucket());
    }

    @Override
    public boolean isFinished()
    {
        return offset >= splits.size();
    }

    @Override
    public void close()
    {
    }
}
