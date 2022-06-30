package io.prestosql.plugin.pmemory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.prestosql.spi.session.PropertyMetadata.*;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

//extend pmemory to handle bucketing properties in create table as queries
public class PMemoryTableProperties {
    public static final String BUCKETED_BY_PROPERTY = "bucketed_by";
    public static final String BUCKET_COUNT_PROPERTY = "bucket_count";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public PMemoryTableProperties(TypeManager typeManager)
    {
        tableProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        BUCKETED_BY_PROPERTY,
                        "Bucketing columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                integerProperty(BUCKET_COUNT_PROPERTY, "Number of buckets", 0, false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<PMemoryBucketProperty> getBucketProperty(Map<String, Object> tableProperties)
    {
        List<String> bucketedBy = getBucketedBy(tableProperties);
        int bucketCount = (Integer) tableProperties.get(BUCKET_COUNT_PROPERTY);
        if ((bucketedBy.isEmpty()) && (bucketCount == 0)) {
            return Optional.empty();
        }
        if (bucketCount < 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s must be greater than zero", BUCKET_COUNT_PROPERTY));
        }
        if (bucketedBy.isEmpty() || bucketCount == 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s and %s must be specified together", BUCKETED_BY_PROPERTY, BUCKET_COUNT_PROPERTY));
        }
        return Optional.of(new PMemoryBucketProperty(bucketedBy, bucketCount));
    }


    @SuppressWarnings("unchecked")
    private static List<String> getBucketedBy(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(BUCKETED_BY_PROPERTY);
    }
}
