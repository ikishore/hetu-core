package io.prestosql.plugin.pmemory;

import io.prestosql.spi.connector.ConnectorPartitionHandle;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class PMemoryPartitionHandle
    extends ConnectorPartitionHandle {
    private final int bucket;

    public PMemoryPartitionHandle(int bucket)
    {
        this.bucket = bucket;
    }

    public int getBucket()
    {
        return bucket;
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
        PMemoryPartitionHandle that = (PMemoryPartitionHandle) o;
        return bucket == that.bucket;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucket);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .toString();
    }
}
