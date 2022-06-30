package io.prestosql.plugin.pmemory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class PMemorySplitWrapper
    implements ConnectorSplit {
    private final List<PMemorySplit> splits;
    private final OptionalInt bucketNumber;

    @JsonCreator
    public PMemorySplitWrapper(
            @JsonProperty("splits") List<PMemorySplit> splits,
            @JsonProperty("bucketNumber") OptionalInt bucketNumber)
    {
        this.splits = requireNonNull(splits, "split lists is null");
        this.bucketNumber = bucketNumber;
    }

    /*@Override
    public String getFilePath()
    {
        return splits.stream().findFirst().orElseThrow(IllegalArgumentException::new).getFilePath();
    }

    @Override
    public long getStartIndex()
    {
        return splits.stream().findFirst().orElseThrow(IllegalArgumentException::new).getStartIndex();
    }

    @Override
    public long getEndIndex()
    {
        return splits.stream().findFirst().orElseThrow(IllegalArgumentException::new).getEndIndex();
    }

    @Override
    public long getLastModifiedTime()
    {
        return splits.stream().findFirst().orElseThrow(IllegalArgumentException::new).getLastModifiedTime();
    }

    @Override
    public boolean isCacheable()
    {
        return splits.stream().findFirst().orElseThrow(IllegalAccessError::new).isCacheable();
    }*/

    @JsonProperty
    public List<PMemorySplit> getSplits()
    {
        return splits;
    }

    @Override
    public List<ConnectorSplit> getUnwrappedSplits()
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @JsonProperty
    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return splits.stream()
                .flatMap(s -> s.getAddresses().stream())
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public Object getInfo()
    {
        if (splits.isEmpty()) {
            return ImmutableMap.of();
        }
        PMemorySplit split = splits.get(0);
        return ImmutableMap.builder()
                .put("hosts", getAddresses())
                .put("table", split.getTable())
                .build();
    }

    public static PMemorySplitWrapper wrap(PMemorySplit pMemorySplit)
    {
        return new PMemorySplitWrapper(ImmutableList.of(pMemorySplit), pMemorySplit.getBucketNumber());
    }

    public static PMemorySplitWrapper wrap(List<PMemorySplit> pMemorySplitList, OptionalInt bucketNumber)
    {
        return new PMemorySplitWrapper(pMemorySplitList, bucketNumber);
    }

    public static PMemorySplit getOnlyHiveSplit(ConnectorSplit connectorSplit)
    {
        return getOnlyElement(((PMemorySplitWrapper) connectorSplit).getSplits());
    }

    @Override
    public int getSplitCount()
    {
        return splits.size();
    }
}
