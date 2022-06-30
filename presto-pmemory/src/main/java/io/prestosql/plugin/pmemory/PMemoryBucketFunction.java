package io.prestosql.plugin.pmemory;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;

import java.util.List;
import java.util.stream.Collectors;

import io.prestosql.spi.Page;
import io.prestosql.spi.connector.BucketFunction;
import org.w3c.dom.TypeInfo;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

//PMemoryBucketFunction is the hash function used for pmemory related operations (such as bucketing)
//it combines all the attributes in a position into one hash value
//Before hashing, the target channels have been projected into a new Page
public class PMemoryBucketFunction
    implements BucketFunction {
    private final int bucketCount;

    public PMemoryBucketFunction(int bucketCount)
    {
        this(bucketCount, false);
    }

    public PMemoryBucketFunction(int bucketCount, boolean forUpdate)
    {
        this.bucketCount = bucketCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        int channelCount = page.getChannelCount();
        int result = 0;
        for (int i = 0; i < channelCount; i++) {
            int fieldHash = (!page.getBlock(i).isNull(position)) ? page.getBlock(i).get(position).hashCode() : 0;
            result = result * 31 + fieldHash;
        }
        return (result & Integer.MAX_VALUE) % bucketCount;

    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketCount", bucketCount)
                .toString();
    }
}
