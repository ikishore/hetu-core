package io.prestosql.plugin.pmemory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

//metadata that describes bucketing of a table
public class PMemoryBucketProperty {
    //bucketing columns
    private final List<String> bucketedBy;
    //number of buckets
    private final int bucketCount;

    @JsonCreator
    public PMemoryBucketProperty(
            @JsonProperty("bucketedBy") List<String> bucketedBy,
            @JsonProperty("bucketCount") int bucketCount)
    {
        this.bucketedBy = ImmutableList.copyOf(requireNonNull(bucketedBy, "bucketedBy is null"));
        this.bucketCount = bucketCount;
    }

    @JsonProperty
    public List<String> getBucketedBy()
    {
        return bucketedBy;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
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
        PMemoryBucketProperty that = (PMemoryBucketProperty) o;
        return bucketCount == that.bucketCount &&
                Objects.equals(bucketedBy, that.bucketedBy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketedBy, bucketCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketedBy", bucketedBy)
                .add("bucketCount", bucketCount)
                .toString();
    }
}
