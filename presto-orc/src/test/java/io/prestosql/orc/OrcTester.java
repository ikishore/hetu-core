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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.prestosql.metadata.Metadata;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcUtil;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcConf;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterators.advance;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static io.prestosql.orc.OrcTester.Format.ORC_11;
import static io.prestosql.orc.OrcTester.Format.ORC_12;
import static io.prestosql.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.prestosql.orc.TestingOrcPredicate.createOrcPredicate;
import static io.prestosql.orc.metadata.CompressionKind.LZ4;
import static io.prestosql.orc.metadata.CompressionKind.NONE;
import static io.prestosql.orc.metadata.CompressionKind.SNAPPY;
import static io.prestosql.orc.metadata.CompressionKind.ZLIB;
import static io.prestosql.orc.metadata.CompressionKind.ZSTD;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.rescale;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.Varchars.truncateToLength;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;

public class OrcTester
{
    private static final Logger log = Logger.get(OrcTester.class);
    public static final DataSize MAX_BLOCK_SIZE = new DataSize(1, MEGABYTE);
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    private static final Metadata METADATA = createTestMetadataManager();

    public enum Format
    {
        ORC_12, ORC_11
    }

    private boolean structTestsEnabled;
    private boolean mapTestsEnabled;
    private boolean listTestsEnabled;
    private boolean complexStructuralTestsEnabled;
    private boolean structuralNullTestsEnabled;
    private boolean reverseTestsEnabled;
    private boolean nullTestsEnabled;
    private boolean missingStructFieldsTestsEnabled;
    private boolean skipBatchTestsEnabled;
    private boolean skipStripeTestsEnabled;
    private Set<Format> formats = ImmutableSet.of();
    private Set<CompressionKind> compressions = ImmutableSet.of();
    private boolean useSelectiveOrcReader;

    public static OrcTester quickOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.structTestsEnabled = true;
        orcTester.mapTestsEnabled = true;
        orcTester.listTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.missingStructFieldsTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.formats = ImmutableSet.of(ORC_12);
        orcTester.compressions = ImmutableSet.of(ZLIB);
        return orcTester;
    }

    public static OrcTester fullOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.structTestsEnabled = true;
        orcTester.mapTestsEnabled = true;
        orcTester.listTestsEnabled = true;
        orcTester.complexStructuralTestsEnabled = true;
        orcTester.structuralNullTestsEnabled = true;
        orcTester.reverseTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.missingStructFieldsTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.skipStripeTestsEnabled = true;
        orcTester.formats = ImmutableSet.copyOf(Format.values());
        orcTester.compressions = ImmutableSet.of(NONE, SNAPPY, ZLIB, LZ4, ZSTD);
        return orcTester;
    }

    public static OrcTester quickSelectiveOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.listTestsEnabled = true;
        orcTester.structTestsEnabled = false;
        orcTester.nullTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.formats = ImmutableSet.of(ORC_12, ORC_11);
        orcTester.compressions = ImmutableSet.of(ZLIB, ZSTD);
        orcTester.useSelectiveOrcReader = true;
        orcTester.listTestsEnabled = false;

        return orcTester;
    }

    public void testRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        testRoundTrip(type, readValues, ImmutableList.of());
    }

    public void testRoundTrip(Type type, List<?> readValues, List<Map<Integer, TupleDomainFilter>> filters)
            throws Exception
    {
        // just the values
        testRoundTripType(type, readValues, filters);

        // all nulls
        if (nullTestsEnabled) {
            assertRoundTrip(
                    type,
                    readValues.stream()
                            .map(value -> null)
                            .collect(toList()),
                            filters);
        }

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(type, readValues);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            testStructRoundTrip(
                    rowType(type, type, type),
                    readValues.stream()
                            .map(OrcTester::toHiveStruct)
                            .collect(toList()));
        }

        // values wrapped in map
        if (mapTestsEnabled && type.isComparable()) {
            testMapRoundTrip(type, readValues);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(type, readValues);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(
                    arrayType(type),
                    readValues.stream()
                            .map(OrcTester::toHiveList)
                            .collect(toList()));
        }
    }

    private void testStructRoundTrip(Type type, List<?> values)
            throws Exception
    {
        Type rowType = rowType(type, type, type);
        // values in simple struct
        testRoundTripType(
                rowType,
                values.stream()
                        .map(OrcTester::toHiveStruct)
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple struct
            testRoundTripType(
                    rowType,
                    insertNullEvery(5, values).stream()
                            .map(OrcTester::toHiveStruct)
                            .collect(toList()));

            // all null values in simple struct
            testRoundTripType(
                    rowType,
                    values.stream()
                            .map(value -> toHiveStruct(null))
                            .collect(toList()));
        }

        if (missingStructFieldsTestsEnabled) {
            Type readType = rowType(type, type, type, type, type, type);
            Type writeType = rowType(type, type, type);

            List<?> writeValues = values.stream()
                    .map(OrcTester::toHiveStruct)
                    .collect(toList());

            List<?> readValues = values.stream()
                    .map(OrcTester::toHiveStructWithNull)
                    .collect(toList());

            assertRoundTrip(writeType, readType, writeValues, readValues, ImmutableList.of());
        }
    }

    private void testMapRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        Type mapType = mapType(type, type);

        // maps can not have a null key, so select a value to use for the map key when the value is null
        Object readNullKeyValue = Iterables.getLast(readValues);

        // values in simple map
        testRoundTripType(
                mapType,
                readValues.stream()
                        .map(value -> toHiveMap(value, readNullKeyValue))
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple map
            testRoundTripType(
                    mapType,
                    insertNullEvery(5, readValues).stream()
                            .map(value -> toHiveMap(value, readNullKeyValue))
                            .collect(toList()));

            // all null values in simple map
            testRoundTripType(
                    mapType,
                    readValues.stream()
                            .map(value -> toHiveMap(null, readNullKeyValue))
                            .collect(toList()));
        }
    }

    private void testListRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        Type arrayType = arrayType(type);
        // values in simple list
        testRoundTripType(
                arrayType,
                readValues.stream()
                        .map(OrcTester::toHiveList)
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple list
            testRoundTripType(
                    arrayType,
                    insertNullEvery(5, readValues).stream()
                            .map(OrcTester::toHiveList)
                            .collect(toList()));

            // all null values in simple list
            testRoundTripType(
                    arrayType,
                    readValues.stream()
                            .map(value -> toHiveList(null))
                            .collect(toList()));
        }
    }

    private void testRoundTripType(Type type, List<?> readValues)
            throws Exception
    {
        testRoundTripType(type, readValues, ImmutableList.of());
    }

    private void testRoundTripType(Type type, List<?> readValues, List<Map<Integer, TupleDomainFilter>> filters)
            throws Exception
    {
        // forward order
        assertRoundTrip(type, readValues, filters);

        // reverse order
        if (reverseTestsEnabled) {
            assertRoundTrip(type, reverse(readValues), filters);
        }

        if (nullTestsEnabled) {
            // forward order with nulls
            assertRoundTrip(type, insertNullEvery(5, readValues), filters);

            // reverse order with nulls
            if (reverseTestsEnabled) {
                assertRoundTrip(type, insertNullEvery(5, reverse(readValues)), filters);
            }
        }
    }

    public void assertRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        assertRoundTrip(type, type, readValues, readValues, ImmutableList.of());
    }

    private void assertRoundTrip(Type type, List<?> readValues, List<Map<Integer, TupleDomainFilter>> filters)
            throws Exception
    {
        assertRoundTrip(type, type, readValues, readValues, filters);
    }

    private void assertRoundTrip(Type writeType, Type readType, List<?> writeValues, List<?> readValues, List<Map<Integer, TupleDomainFilter>> filters)
            throws Exception
    {
        OrcWriterStats stats = new OrcWriterStats();
        for (CompressionKind compression : compressions) {
            boolean hiveSupported = (compression != LZ4) && (compression != ZSTD);

            for (Format format : formats) {
                // write Hive, read Presto
                if (hiveSupported) {
                    try (TempFile tempFile = new TempFile()) {
                        writeOrcColumnHive(tempFile.getFile(), format, compression, writeType, writeValues.iterator());
                        assertFileContentsPresto(readType, tempFile, readValues, false, false, useSelectiveOrcReader, filters);
                    }
                }
            }

            // write Presto, read Hive and Presto
            try (TempFile tempFile = new TempFile()) {
                writeOrcColumnPresto(tempFile.getFile(), compression, writeType, writeValues.iterator(), stats);

                if (hiveSupported) {
                    assertFileContentsHive(readType, tempFile, readValues);
                }

                assertFileContentsPresto(readType, tempFile, readValues, false, false, useSelectiveOrcReader, filters);

                if (skipBatchTestsEnabled) {
                    assertFileContentsPresto(readType, tempFile, readValues, true, false, useSelectiveOrcReader, filters);
                }

                if (skipStripeTestsEnabled) {
                    assertFileContentsPresto(readType, tempFile, readValues, false, true, useSelectiveOrcReader, filters);
                }
            }
        }

        assertEquals(stats.getWriterSizeInBytes(), 0);
    }

    private static void assertFileContentsPresto(
            List<Type> types,
            TempFile tempFile,
            List<?> expectedValues,
            OrcPredicate orcPredicate,
            Optional<Map<Integer, TupleDomainFilter>> filters)
            throws IOException
    {
        try (OrcSelectiveRecordReader recordReader = createCustomOrcSelectiveRecordReader(tempFile, orcPredicate, types, MAX_BATCH_SIZE, filters.orElse(ImmutableMap.of()))) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);

            int rowsProcessed = 0;
            while (true) {
                Page page = recordReader.getNextPage();
                if (page == null) {
                    break;
                }

                if (page.getPositionCount() == 0) {
                    continue;
                }

                for (int i = 0; i < types.size(); i++) {
                    Type type = types.get(i);
                    Block block = page.getBlock(i);

                    List<Object> data = new ArrayList<>(block.getPositionCount());
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        data.add(type.getObjectValue(SESSION, block, position));
                    }

                    for (int j = 0; j < block.getPositionCount(); j++) {
                        assertColumnValueEquals(type, data.get(j), expectedValues.get(rowsProcessed + j));
                    }
                }

                rowsProcessed += page.getPositionCount();
            }

            assertEquals(rowsProcessed, expectedValues.size());
        }
    }

    static OrcSelectiveRecordReader createCustomOrcSelectiveRecordReader(
            TempFile tempFile,
            OrcPredicate predicate,
            List<Type> types,
            int initialBatchSize,
            Map<Integer, TupleDomainFilter> filters)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true, tempFile.getFile().lastModified());
        OrcReader orcReader = new OrcReader(orcDataSource, OrcFileTail.readFrom(orcDataSource, Optional.empty()), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));

        assertEquals(orcReader.getColumnNames(), makeColumnNames(types.size()));
        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);

        Map<Integer, Type> columnTypes = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableMap(Function.identity(), types::get));

        return orcReader.createSelectiveRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                orcReader.getRootColumn().getNestedColumns(),
                types,
                ImmutableList.of(0),
                columnTypes,
                filters,
                ImmutableMap.of(),
                predicate,
                0,
                orcDataSource.getSize(),
                HIVE_STORAGE_TIME_ZONE,
                newSimpleAggregatedMemoryContext(),
                initialBatchSize,
                RuntimeException::new,
                Optional.empty(),
                null,
                OrcCacheStore.CACHE_NOTHING,
                new OrcCacheProperties(),
                Optional.empty(),
                new HashMap<>(),
                null,
                false,
                ImmutableMap.of(),
                ImmutableMap.of(),
                new HashSet<>());
    }

    private static List<String> makeColumnNames(int columns)
    {
        return IntStream.range(0, columns)
                .mapToObj(i -> i == 0 ? "test" : "test" + (i + 1))
                .collect(toList());
    }

    private static List<?> filterRows(List<Type> types, List<?> values, Map<Integer, TupleDomainFilter> columnFilters)
    {
        List<Integer> passingRows = IntStream.range(0, values.size())
                .filter(row -> testRow(types, values, row, columnFilters))
                .boxed()
                .collect(toList());
        return IntStream.range(0, values.size())
                .mapToObj(column -> passingRows.stream().map(values::get).collect(toList())).collect(toList()).get(0);
    }

    //later we can extend for multiple columns
    private static boolean testRow(List<Type> types, List<?> values, int row, Map<Integer, TupleDomainFilter> columnFilters)
    {
        for (int column = 0; column < types.size(); column++) {
            TupleDomainFilter filter = columnFilters.get(column);
            if (filter == null) {
                continue;
            }

            Object value = values.get(row);
            if (value == null) {
                if (!filter.testNull()) {
                    return false;
                }
            }
            else {
                Type type = types.get(column);
                if (type == BOOLEAN) {
                    if (!filter.testBoolean((Boolean) value)) {
                        return false;
                    }
                }
                else if (type == BIGINT || type == INTEGER || type == SMALLINT) {
                    if (!filter.testLong(((Number) value).longValue())) {
                        return false;
                    }
                }
                else if (type == DATE) {
                    if (!filter.testLong(((SqlDate) value).getDays())) {
                        return false;
                    }
                }
                else if (type == TIMESTAMP) {
                    return filter.testLong(((SqlTimestamp) value).getMillis());
                }
                else if (type == VARCHAR) {
                    return filter.testBytes(((String) value).getBytes(), 0, ((String) value).length());
                }
                else if (type instanceof CharType) {
                    String charString = String.valueOf(value);
                    return filter.testBytes(charString.getBytes(StandardCharsets.UTF_8), 0, charString.length());
                }
                else if (type == VARBINARY) {
                    byte[] binary = ((SqlVarbinary) value).getBytes();
                    return filter.testBytes(binary, 0, binary.length);
                }
                else if (type instanceof DecimalType) {
                    DecimalType decimalType = (DecimalType) type;
                    BigDecimal bigDecimal = ((SqlDecimal) value).toBigDecimal();
                    if (decimalType.isShort()) {
                        return filter.testLong(bigDecimal.unscaledValue().longValue());
                    }
                }
                else if (type == DOUBLE) {
                    if (!filter.testDouble((double) value)) {
                        return false;
                    }
                }
                else {
                    fail("Unsupported type: " + type);
                }
            }
        }

        return true;
    }

    private static void assertFileContentsPresto(
            Type type,
            TempFile tempFile,
            List<?> expectedValues,
            boolean skipFirstBatch,
            boolean skipStripe,
            boolean useSelectiveOrcReader,
            List<Map<Integer, TupleDomainFilter>> filters)
            throws IOException
    {
        OrcPredicate orcPredicate = createOrcPredicate(type, expectedValues);
        if (useSelectiveOrcReader) {
            assertFileContentsPresto(ImmutableList.of(type), tempFile, expectedValues, orcPredicate, Optional.empty());

            for (Map<Integer, TupleDomainFilter> columnFilters : filters) {
                assertFileContentsPresto(ImmutableList.of(type), tempFile, filterRows(ImmutableList.of(type), expectedValues, columnFilters), orcPredicate, Optional.of(columnFilters));
            }

            return;
        }

        try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, createOrcPredicate(type, expectedValues), type, MAX_BATCH_SIZE)) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);

            boolean isFirst = true;
            int rowsProcessed = 0;
            Iterator<?> iterator = expectedValues.iterator();
            for (Page page = recordReader.nextPage(); page != null; page = recordReader.nextPage()) {
                int batchSize = page.getPositionCount();
                if (skipStripe && rowsProcessed < 10000) {
                    assertEquals(advance(iterator, batchSize), batchSize);
                }
                else if (skipFirstBatch && isFirst) {
                    assertEquals(advance(iterator, batchSize), batchSize);
                    isFirst = false;
                }
                else {
                    Block block = page.getBlock(0);

                    List<Object> data = new ArrayList<>(block.getPositionCount());
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        data.add(type.getObjectValue(SESSION, block, position));
                    }

                    for (int i = 0; i < batchSize; i++) {
                        assertTrue(iterator.hasNext());
                        Object expected = iterator.next();
                        Object actual = data.get(i);
                        assertColumnValueEquals(type, actual, expected);
                    }
                }
                assertEquals(recordReader.getReaderPosition(), rowsProcessed);
                assertEquals(recordReader.getFilePosition(), rowsProcessed);
                rowsProcessed += batchSize;
            }
            assertFalse(iterator.hasNext());
            assertNull(recordReader.nextPage());

            assertEquals(recordReader.getReaderPosition(), rowsProcessed);
            assertEquals(recordReader.getFilePosition(), rowsProcessed);
        }
    }

    private static void assertColumnValueEquals(Type type, Object actual, Object expected)
    {
        if (actual == null) {
            assertNull(expected);
            return;
        }
        String baseType = type.getTypeSignature().getBase();
        if (StandardTypes.ARRAY.equals(baseType)) {
            List<?> actualArray = (List<?>) actual;
            List<?> expectedArray = (List<?>) expected;
            assertEquals(actualArray.size(), expectedArray.size());

            Type elementType = type.getTypeParameters().get(0);
            for (int i = 0; i < actualArray.size(); i++) {
                Object actualElement = actualArray.get(i);
                Object expectedElement = expectedArray.get(i);
                assertColumnValueEquals(elementType, actualElement, expectedElement);
            }
        }
        else if (StandardTypes.MAP.equals(baseType)) {
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            Map<?, ?> expectedMap = (Map<?, ?>) expected;
            assertEquals(actualMap.size(), expectedMap.size());

            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            List<Entry<?, ?>> expectedEntries = new ArrayList<>(expectedMap.entrySet());
            for (Entry<?, ?> actualEntry : actualMap.entrySet()) {
                Iterator<Entry<?, ?>> iterator = expectedEntries.iterator();
                while (iterator.hasNext()) {
                    Entry<?, ?> expectedEntry = iterator.next();
                    try {
                        assertColumnValueEquals(keyType, actualEntry.getKey(), expectedEntry.getKey());
                        assertColumnValueEquals(valueType, actualEntry.getValue(), expectedEntry.getValue());
                        iterator.remove();
                    }
                    catch (AssertionError ignored) {
                        log.debug(ignored.getMessage());
                    }
                }
            }
            assertTrue(expectedEntries.isEmpty(), "Unmatched entries " + expectedEntries);
        }
        else if (StandardTypes.ROW.equals(baseType)) {
            List<Type> fieldTypes = type.getTypeParameters();

            List<?> actualRow = (List<?>) actual;
            List<?> expectedRow = (List<?>) expected;
            assertEquals(actualRow.size(), fieldTypes.size());
            assertEquals(actualRow.size(), expectedRow.size());

            for (int fieldId = 0; fieldId < actualRow.size(); fieldId++) {
                Type fieldType = fieldTypes.get(fieldId);
                Object actualElement = actualRow.get(fieldId);
                Object expectedElement = expectedRow.get(fieldId);
                assertColumnValueEquals(fieldType, actualElement, expectedElement);
            }
        }
        else if (type.equals(DOUBLE)) {
            Double actualDouble = (Double) actual;
            Double expectedDouble = (Double) expected;
            if (actualDouble.isNaN()) {
                assertTrue(expectedDouble.isNaN(), "expected double to be NaN");
            }
            else {
                assertEquals(actualDouble, expectedDouble, 0.001);
            }
        }
        else if (!Objects.equals(actual, expected)) {
            assertEquals(actual, expected);
        }
    }

    static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcPredicate predicate, Type type, int initialBatchSize)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true, tempFile.getFile().lastModified());
        OrcReader orcReader = new OrcReader(orcDataSource, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), MAX_BLOCK_SIZE);

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));
        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);

        return orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(type),
                predicate,
                HIVE_STORAGE_TIME_ZONE,
                newSimpleAggregatedMemoryContext(),
                initialBatchSize,
                RuntimeException::new);
    }

    static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcPredicate predicate, List<String> columns, List<Type> types, int initialBatchSize, OrcCacheStore orcCacheStore, OrcCacheProperties orcCacheProperties)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true, tempFile.getFile().lastModified());
        OrcReader orcReader = new OrcReader(orcDataSource, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE),
                MAX_BLOCK_SIZE);

        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);
        Map<String, OrcColumn> columnByName = orcReader.getRootColumn()
                .getNestedColumns()
                .stream().collect(Collectors.toMap(OrcColumn::getColumnName, Function.identity()));
        List<OrcColumn> readColumns = columns.stream()
                .filter(columnByName::containsKey)
                .map(columnByName::get)
                .collect(toList());
        return orcReader.createRecordReader(
                readColumns,
                types,
                predicate,
                HIVE_STORAGE_TIME_ZONE,
                newSimpleAggregatedMemoryContext(),
                initialBatchSize,
                RuntimeException::new,
                orcCacheStore,
                orcCacheProperties);
    }

    public static void writeOrcColumnPresto(File outputFile, CompressionKind compression, Type type, Iterator<?> values, OrcWriterStats stats)
            throws Exception
    {
        ImmutableMap.Builder<String, String> metadata = ImmutableMap.builder();
        metadata.put("columns", "test");
        metadata.put("columns.types", createSettableStructObjectInspector("test", type).getTypeName());

        OrcWriter writer = new OrcWriter(
                new OutputStreamOrcDataSink(new FileOutputStream(outputFile)),
                ImmutableList.of("test"),
                ImmutableList.of(type),
                compression,
                new OrcWriterOptions(),
                false,
                ImmutableMap.of(),
                true,
                BOTH,
                stats, Optional.empty(), Optional.empty());

        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1024);
        while (values.hasNext()) {
            Object value = values.next();
            writeValue(type, blockBuilder, value);
        }

        writer.write(new Page(blockBuilder.build()));
        writer.close();
        writer.validate(new FileOrcDataSource(outputFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true, outputFile.lastModified()));
    }

    private static void writeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            if (BOOLEAN.equals(type)) {
                type.writeBoolean(blockBuilder, (Boolean) value);
            }
            else if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || BIGINT.equals(type)) {
                type.writeLong(blockBuilder, ((Number) value).longValue());
            }
            else if (Decimals.isShortDecimal(type)) {
                type.writeLong(blockBuilder, ((SqlDecimal) value).toBigDecimal().unscaledValue().longValue());
            }
            else if (Decimals.isLongDecimal(type)) {
                type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(((SqlDecimal) value).toBigDecimal().unscaledValue()));
            }
            else if (DOUBLE.equals(type)) {
                type.writeDouble(blockBuilder, ((Number) value).doubleValue());
            }
            else if (REAL.equals(type)) {
                float floatValue = ((Number) value).floatValue();
                type.writeLong(blockBuilder, Float.floatToIntBits(floatValue));
            }
            else if (type instanceof VarcharType) {
                Slice slice = truncateToLength(utf8Slice((String) value), type);
                type.writeSlice(blockBuilder, slice);
            }
            else if (type instanceof CharType) {
                Slice slice = truncateToLengthAndTrimSpaces(utf8Slice((String) value), type);
                type.writeSlice(blockBuilder, slice);
            }
            else if (VARBINARY.equals(type)) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer(((SqlVarbinary) value).getBytes()));
            }
            else if (DATE.equals(type)) {
                long days = ((SqlDate) value).getDays();
                type.writeLong(blockBuilder, days);
            }
            else if (TIMESTAMP.equals(type)) {
                long millis = ((SqlTimestamp) value).getMillis();
                type.writeLong(blockBuilder, millis);
            }
            else {
                String baseType = type.getTypeSignature().getBase();
                if (StandardTypes.ARRAY.equals(baseType)) {
                    List<?> array = (List<?>) value;
                    Type elementType = type.getTypeParameters().get(0);
                    BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
                    for (Object elementValue : array) {
                        writeValue(elementType, arrayBlockBuilder, elementValue);
                    }
                    blockBuilder.closeEntry();
                }
                else if (StandardTypes.MAP.equals(baseType)) {
                    Map<?, ?> map = (Map<?, ?>) value;
                    Type keyType = type.getTypeParameters().get(0);
                    Type valueType = type.getTypeParameters().get(1);
                    BlockBuilder mapBlockBuilder = blockBuilder.beginBlockEntry();
                    for (Entry<?, ?> entry : map.entrySet()) {
                        writeValue(keyType, mapBlockBuilder, entry.getKey());
                        writeValue(valueType, mapBlockBuilder, entry.getValue());
                    }
                    blockBuilder.closeEntry();
                }
                else if (StandardTypes.ROW.equals(baseType)) {
                    List<?> array = (List<?>) value;
                    List<Type> fieldTypes = type.getTypeParameters();
                    BlockBuilder rowBlockBuilder = blockBuilder.beginBlockEntry();
                    for (int fieldId = 0; fieldId < fieldTypes.size(); fieldId++) {
                        Type fieldType = fieldTypes.get(fieldId);
                        writeValue(fieldType, rowBlockBuilder, array.get(fieldId));
                    }
                    blockBuilder.closeEntry();
                }
                else {
                    throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
        }
    }

    private static void assertFileContentsHive(
            Type type,
            TempFile tempFile,
            Iterable<?> expectedValues)
            throws Exception
    {
        assertFileContentsOrcHive(type, tempFile, expectedValues);
    }

    private static void assertFileContentsOrcHive(
            Type type,
            TempFile tempFile,
            Iterable<?> expectedValues)
            throws Exception
    {
        JobConf configuration = new JobConf(new Configuration(false));
        configuration.set(READ_COLUMN_IDS_CONF_STR, "0");
        configuration.setBoolean(READ_ALL_COLUMNS, false);

        Reader reader = OrcFile.createReader(
                new Path(tempFile.getFile().getAbsolutePath()),
                new ReaderOptions(configuration)
                        .useUTCTimestamp(true));
        RecordReader recordReader = reader.rows();

        StructObjectInspector rowInspector = (StructObjectInspector) reader.getObjectInspector();
        StructField field = rowInspector.getStructFieldRef("test");

        Iterator<?> iterator = expectedValues.iterator();
        Object rowData = null;
        while (recordReader.hasNext()) {
            rowData = recordReader.next(rowData);
            Object expectedValue = iterator.next();

            Object actualValue = rowInspector.getStructFieldData(rowData, field);
            actualValue = decodeRecordReaderValue(type, actualValue);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }
        assertFalse(iterator.hasNext());
    }

    private static Object decodeRecordReaderValue(Type type, Object inputActualValue)
    {
        Object actualValue = inputActualValue;
        if (actualValue instanceof BooleanWritable) {
            actualValue = ((BooleanWritable) actualValue).get();
        }
        else if (actualValue instanceof ByteWritable) {
            actualValue = ((ByteWritable) actualValue).get();
        }
        else if (actualValue instanceof BytesWritable) {
            actualValue = new SqlVarbinary(((BytesWritable) actualValue).copyBytes());
        }
        else if (actualValue instanceof DateWritableV2) {
            actualValue = new SqlDate(((DateWritableV2) actualValue).getDays());
        }
        else if (actualValue instanceof DoubleWritable) {
            actualValue = ((DoubleWritable) actualValue).get();
        }
        else if (actualValue instanceof FloatWritable) {
            actualValue = ((FloatWritable) actualValue).get();
        }
        else if (actualValue instanceof IntWritable) {
            actualValue = ((IntWritable) actualValue).get();
        }
        else if (actualValue instanceof HiveCharWritable) {
            actualValue = ((HiveCharWritable) actualValue).getPaddedValue().toString();
        }
        else if (actualValue instanceof LongWritable) {
            actualValue = ((LongWritable) actualValue).get();
        }
        else if (actualValue instanceof ShortWritable) {
            actualValue = ((ShortWritable) actualValue).get();
        }
        else if (actualValue instanceof HiveDecimalWritable) {
            DecimalType decimalType = (DecimalType) type;
            HiveDecimalWritable writable = (HiveDecimalWritable) actualValue;
            // writable messes with the scale so rescale the values to the Presto type
            BigInteger rescaledValue = rescale(writable.getHiveDecimal().unscaledValue(), writable.getScale(), decimalType.getScale());
            actualValue = new SqlDecimal(rescaledValue, decimalType.getPrecision(), decimalType.getScale());
        }
        else if (actualValue instanceof Text) {
            actualValue = actualValue.toString();
        }
        else if (actualValue instanceof TimestampWritableV2) {
            actualValue = sqlTimestampOf(((TimestampWritableV2) actualValue).getTimestamp().toEpochMilli());
        }
        else if (actualValue instanceof OrcStruct) {
            List<Object> fields = new ArrayList<>();
            OrcStruct structObject = (OrcStruct) actualValue;
            for (int fieldId = 0; fieldId < structObject.getNumFields(); fieldId++) {
                fields.add(OrcUtil.getFieldValue(structObject, fieldId));
            }
            actualValue = decodeRecordReaderStruct(type, fields);
        }
        else if (actualValue instanceof List) {
            actualValue = decodeRecordReaderList(type, ((List<?>) actualValue));
        }
        else if (actualValue instanceof Map) {
            actualValue = decodeRecordReaderMap(type, (Map<?, ?>) actualValue);
        }
        return actualValue;
    }

    private static List<Object> decodeRecordReaderList(Type type, List<?> list)
    {
        Type elementType = type.getTypeParameters().get(0);
        return list.stream()
                .map(element -> decodeRecordReaderValue(elementType, element))
                .collect(toList());
    }

    private static Object decodeRecordReaderMap(Type type, Map<?, ?> map)
    {
        Type keyType = type.getTypeParameters().get(0);
        Type valueType = type.getTypeParameters().get(1);
        Map<Object, Object> newMap = new HashMap<>();
        for (Entry<?, ?> entry : map.entrySet()) {
            newMap.put(decodeRecordReaderValue(keyType, entry.getKey()), decodeRecordReaderValue(valueType, entry.getValue()));
        }
        return newMap;
    }

    private static List<Object> decodeRecordReaderStruct(Type type, List<?> fields)
    {
        List<Type> fieldTypes = type.getTypeParameters();
        List<Object> newFields = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Type fieldType = fieldTypes.get(i);
            Object field = fields.get(i);
            newFields.add(decodeRecordReaderValue(fieldType, field));
        }

        for (int j = fields.size(); j < fieldTypes.size(); j++) {
            newFields.add(null);
        }

        return newFields;
    }

    public static DataSize writeOrcColumnHive(File outputFile, Format format, CompressionKind compression, Type type, Iterator<?> values)
            throws Exception
    {
        RecordWriter recordWriter = createOrcRecordWriter(outputFile, format, compression, type);
        return writeOrcFileColumnHive(outputFile, recordWriter, type, values);
    }

    public static DataSize writeOrcFileColumnHive(File outputFile, RecordWriter recordWriter, Type type, Iterator<?> values)
            throws Exception
    {
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", type);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
        Serializer serializer = new OrcSerde();

        while (values.hasNext()) {
            Object value = values.next();
            value = preprocessWriteValueHive(type, value);
            objectInspector.setStructFieldData(row, fields.get(0), value);

            Writable record = serializer.serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
        return succinctBytes(outputFile.length());
    }

    private static ObjectInspector getJavaObjectInspector(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        if (type.equals(BIGINT)) {
            return javaLongObjectInspector;
        }
        if (type.equals(INTEGER)) {
            return javaIntObjectInspector;
        }
        if (type.equals(SMALLINT)) {
            return javaShortObjectInspector;
        }
        if (type.equals(TINYINT)) {
            return javaByteObjectInspector;
        }
        if (type.equals(REAL)) {
            return javaFloatObjectInspector;
        }
        if (type.equals(DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        if (type instanceof VarcharType) {
            return javaStringObjectInspector;
        }
        if (type instanceof CharType) {
            int charLength = ((CharType) type).getLength();
            return new JavaHiveCharObjectInspector(getCharTypeInfo(charLength));
        }
        if (type instanceof VarbinaryType) {
            return javaByteArrayObjectInspector;
        }
        if (type.equals(DATE)) {
            return javaDateObjectInspector;
        }
        if (type.equals(TIMESTAMP)) {
            return javaTimestampObjectInspector;
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getPrimitiveJavaObjectInspector(new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()));
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
            return getStandardListObjectInspector(getJavaObjectInspector(type.getTypeParameters().get(0)));
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.MAP)) {
            ObjectInspector keyObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(0));
            ObjectInspector valueObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(1));
            return getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.ROW)) {
            return getStandardStructObjectInspector(
                    type.getTypeSignature().getParameters().stream()
                            .map(parameter -> parameter.getNamedTypeSignature().getName().get())
                            .collect(toList()),
                    type.getTypeParameters().stream()
                            .map(OrcTester::getJavaObjectInspector)
                            .collect(toList()));
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static Object preprocessWriteValueHive(Type type, Object value)
    {
        if (value == null) {
            return null;
        }

        if (type.equals(BOOLEAN)) {
            return value;
        }
        if (type.equals(TINYINT)) {
            return ((Number) value).byteValue();
        }
        if (type.equals(SMALLINT)) {
            return ((Number) value).shortValue();
        }
        if (type.equals(INTEGER)) {
            return ((Number) value).intValue();
        }
        if (type.equals(BIGINT)) {
            return ((Number) value).longValue();
        }
        if (type.equals(REAL)) {
            return ((Number) value).floatValue();
        }
        if (type.equals(DOUBLE)) {
            return ((Number) value).doubleValue();
        }
        if (type instanceof VarcharType) {
            return value;
        }
        if (type instanceof CharType) {
            return new HiveChar((String) value, ((CharType) type).getLength());
        }
        if (type.equals(VARBINARY)) {
            return ((SqlVarbinary) value).getBytes();
        }
        if (type.equals(DATE)) {
            return Date.ofEpochDay(((SqlDate) value).getDays());
        }
        if (type.equals(TIMESTAMP)) {
            return Timestamp.ofEpochMilli(((SqlTimestamp) value).getMillis());
        }
        if (type instanceof DecimalType) {
            return HiveDecimal.create(((SqlDecimal) value).toBigDecimal());
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
            Type elementType = type.getTypeParameters().get(0);
            return ((List<?>) value).stream()
                    .map(element -> preprocessWriteValueHive(elementType, element))
                    .collect(toList());
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.MAP)) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);
            Map<Object, Object> newMap = new HashMap<>();
            for (Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                newMap.put(preprocessWriteValueHive(keyType, entry.getKey()), preprocessWriteValueHive(valueType, entry.getValue()));
            }
            return newMap;
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.ROW)) {
            List<?> fieldValues = (List<?>) value;
            List<Type> fieldTypes = type.getTypeParameters();
            List<Object> newStruct = new ArrayList<>();
            for (int fieldId = 0; fieldId < fieldValues.size(); fieldId++) {
                newStruct.add(preprocessWriteValueHive(fieldTypes.get(fieldId), fieldValues.get(fieldId)));
            }
            return newStruct;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    static RecordWriter createOrcRecordWriter(File outputFile, Format format, CompressionKind compression, Type type)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        OrcConf.WRITE_FORMAT.setString(jobConf, format == ORC_12 ? "0.12" : "0.11");
        OrcConf.COMPRESS.setString(jobConf, compression.name());

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                createTableProperties("test", getJavaObjectInspector(type).getTypeName()),
                () -> {});
    }

    static RecordWriter createOrcRecordWriter(File outputFile, Format format, CompressionKind compression, List<String> names, List<String> bloomFilterColumns, List<Type> types)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        OrcConf.WRITE_FORMAT.setString(jobConf, format == ORC_12 ? "0.12" : "0.11");
        OrcConf.COMPRESS.setString(jobConf, compression.name());
        List<String> objectInspectors = types.stream()
                .map(OrcTester::getJavaObjectInspector)
                .map(ObjectInspector::getTypeName)
                .collect(toList());

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                createTableProperties(names, bloomFilterColumns, objectInspectors),
                () -> {});
    }

    static SettableStructObjectInspector createSettableStructObjectInspector(String name, Type type)
    {
        return getStandardStructObjectInspector(ImmutableList.of(name), ImmutableList.of(getJavaObjectInspector(type)));
    }

    static SettableStructObjectInspector createSettableStructObjectInspector(List<String> names, List<Type> types)
    {
        List<ObjectInspector> objectInspectors = types
                .stream()
                .map(OrcTester::getJavaObjectInspector)
                .collect(Collectors.toList());
        return getStandardStructObjectInspector(ImmutableList.copyOf(names), objectInspectors);
    }

    private static Properties createTableProperties(String name, String type)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", name);
        orderTableProperties.setProperty("columns.types", type);
        orderTableProperties.setProperty("orc.bloom.filter.columns", name);
        orderTableProperties.setProperty("orc.bloom.filter.fpp", "0.50");
        orderTableProperties.setProperty("orc.bloom.filter.write.version", "original");
        return orderTableProperties;
    }

    private static Properties createTableProperties(List<String> names, List<String> bloomFilterColumns, List<String> types)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", String.join(",", names));
        orderTableProperties.setProperty("columns.types", String.join(",", types));
        orderTableProperties.setProperty("orc.bloom.filter.columns", String.join(",", bloomFilterColumns));
        orderTableProperties.setProperty("orc.bloom.filter.fpp", "0.50");
        orderTableProperties.setProperty("orc.bloom.filter.write.version", "original");
        return orderTableProperties;
    }

    private static <T> List<T> reverse(List<T> iterable)
    {
        return Lists.reverse(ImmutableList.copyOf(iterable));
    }

    private static <T> List<T> insertNullEvery(int n, List<T> iterable)
    {
        return newArrayList(() -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                position++;
                if (position > n) {
                    position = 0;
                    return null;
                }

                if (!delegate.hasNext()) {
                    return endOfData();
                }

                return delegate.next();
            }
        });
    }

    private static List<Object> toHiveStruct(Object input)
    {
        return asList(input, input, input);
    }

    private static List<Object> toHiveStructWithNull(Object input)
    {
        return asList(input, input, input, null, null, null);
    }

    private static Map<Object, Object> toHiveMap(Object input, Object nullKeyValue)
    {
        Map<Object, Object> map = new HashMap<>();
        map.put(input != null ? input : nullKeyValue, input);
        return map;
    }

    private static List<Object> toHiveList(Object input)
    {
        return asList(input, input, input, input);
    }

    private static Type arrayType(Type elementType)
    {
        return METADATA.getFunctionAndTypeManager().getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
    }

    public static Type mapType(Type keyType, Type valueType)
    {
        return METADATA.getFunctionAndTypeManager().getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType.getTypeSignature()), TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    private static Type rowType(Type... fieldTypes)
    {
        ImmutableList.Builder<TypeSignatureParameter> typeSignatureParameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            String filedName = "field_" + i;
            Type fieldType = fieldTypes[i];
            typeSignatureParameters.add(TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName(filedName, false)), fieldType.getTypeSignature())));
        }
        return METADATA.getFunctionAndTypeManager().getParameterizedType(StandardTypes.ROW, typeSignatureParameters.build());
    }
}
