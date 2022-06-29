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
package io.prestosql.orc.reader;

import com.google.common.collect.ImmutableList;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.FloatInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.time.ZoneId;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static io.prestosql.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class FloatSelectiveColumnReader
        implements SelectiveColumnReader<Integer>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FloatSelectiveColumnReader.class).instanceSize();
    private static final Block NULL_BLOCK = REAL.createBlockBuilder(null, 1).appendNull().build();

    private final OrcColumn streamDescriptor;
    private final TupleDomainFilter filter;
    private final boolean nullsAllowed;
    private final boolean outputRequired;
    private final LocalMemoryContext systemMemoryContext;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private InputStreamSource<FloatInputStream> dataStreamSource = missingStreamSource(FloatInputStream.class);
    private BooleanInputStream presentStream;
    private FloatInputStream dataStream;

    private boolean rowGroupOpen;
    private int readOffset;
    private int[] values;
    private boolean[] nulls;
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean allNulls;

    public FloatSelectiveColumnReader(
            OrcColumn streamDescriptor,
            Optional<TupleDomainFilter> filter,
            boolean outputRequired,
            LocalMemoryContext systemMemoryContext)
    {
        requireNonNull(filter, "filter is null");
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = filter.orElse(null);
        this.outputRequired = outputRequired;
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.nullsAllowed = this.filter == null || this.filter.testNull();
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, ZoneId storageTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(FloatInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, FloatInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        rowGroupOpen = false;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(values) + sizeOf(nulls) + sizeOf(outputPositions);
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();
        rowGroupOpen = true;
    }

    @Override
    public int read(int offset, int[] positions, int positionCount, TupleDomainFilter filter)
            throws IOException
    {
        return readOr(offset, positions, positionCount,
                (this.filter == null) ? null : ImmutableList.of(this.filter),
                null);
    }

    @Override
    public int readOr(int offset, int[] positions, int positionCount, List<TupleDomainFilter> filters, BitSet accumulator)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;
        outputPositionCount = 0;

        ensureValuesCapacity(outputRequired, positionCount, nullsAllowed && presentStream != null, positions);

        if (filters != null) {
            if (outputPositions == null || outputPositions.length < positionCount) {
                outputPositions = new int[positionCount];
            }
        }
        else {
            outputPositions = positions;
        }

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        int streamPosition = 0;
        if (dataStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
            if (filters != null && filters.get(0).testNull() && accumulator != null) {
                accumulator.set(positions[0], streamPosition);
            }
        }
        else if (filters == null) {
            streamPosition = readNoFilter(positions, positionCount);
        }
        else if (accumulator == null) {
            streamPosition = readWithFilter(positions, positionCount, filters);
        }
        else {
            streamPosition = readWithOrFilter(positions, positionCount, filters, accumulator);
        }

        readOffset = offset + streamPosition;
        return outputPositionCount;
    }

    private void ensureValuesCapacity(boolean outputRequired, int capacity, boolean recordNulls, int[] positions)
    {
        if (outputRequired) {
            if (values == null || values.length < capacity) {
                values = new int[capacity];
            }

            if (recordNulls) {
                if (nulls == null || nulls.length < capacity) {
                    nulls = new boolean[capacity];
                }
            }
        }
    }

    private int readWithFilter(int[] positions, int positionCount, List<TupleDomainFilter> filters)
            throws IOException
    {
        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if (nullsAllowed) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                float value = dataStream.next();
                if (filters.get(0).testFloat(value)) {
                    if (outputRequired) {
                        values[outputPositionCount] = floatToRawIntBits(value);
                        if (nullsAllowed && presentStream != null) {
                            nulls[outputPositionCount] = false;
                        }
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            streamPosition++;
        }

        return streamPosition;
    }

    private int readWithOrFilter(int[] positions, int positionCount, List<TupleDomainFilter> filters, BitSet accumulator)
            throws IOException
    {
        int streamPosition = 0;
        boolean checkNulls = filters != null && filters.stream().anyMatch(f -> f.testNull());
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if (nullsAllowed) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                    }
                    if (accumulator != null && checkNulls) {
                        accumulator.set(position);
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                float value = dataStream.next();
                if ((accumulator != null && accumulator.get(position))
                        || filters == null || filters.stream().anyMatch(f -> f.testFloat(value))) {
                    if (accumulator != null) {
                        accumulator.set(position);
                    }
                }

                if (outputRequired) {
                    values[outputPositionCount] = floatToRawIntBits(value);
                    if (nullsAllowed && presentStream != null) {
                        nulls[outputPositionCount] = false;
                    }
                }
                outputPositions[outputPositionCount] = position;
                outputPositionCount++;
            }
            streamPosition++;
        }

        return streamPosition;
    }

    private int readAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        presentStream.skip(positions[positionCount - 1]);

        if (nullsAllowed) {
            outputPositionCount = positionCount;
        }
        else {
            outputPositionCount = 0;
        }

        allNulls = true;
        return positions[positionCount - 1] + 1;
    }

    private int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        if (positions[positionCount - 1] == positionCount - 1) {
            // no skipping
            if (presentStream != null) {
                // some nulls
                int nullCount = presentStream.getUnsetBits(positionCount, nulls);
                if (nullCount == positionCount) {
                    allNulls = true;
                }
                else {
                    for (int i = 0; i < positionCount; i++) {
                        if (!nulls[i]) {
                            values[i] = floatToRawIntBits(dataStream.next());
                        }
                    }
                }
            }
            else {
                // no nulls
                for (int i = 0; i < positionCount; i++) {
                    values[i] = floatToRawIntBits(dataStream.next());
                }
            }
            outputPositionCount = positionCount;
            return positions[positionCount - 1] + 1;
        }

        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                nulls[i] = true;
            }
            else {
                values[i] = floatToRawIntBits(dataStream.next());
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }
            streamPosition++;
        }
        outputPositionCount = positionCount;
        return streamPosition;
    }

    private void skip(int items)
            throws IOException
    {
        if (dataStream == null) {
            presentStream.skip(items);
        }
        else if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            dataStream.skip(dataToSkip);
        }
        else {
            dataStream.skip(items);
        }
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (allNulls) {
            return new RunLengthEncodedBlock(NULL_BLOCK, positionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount == outputPositionCount) {
            Block block = new IntArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values);
            nulls = null;
            values = null;
            return block;
        }

        int[] valuesCopy = new int[positionCount];
        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            valuesCopy[positionIndex] = this.values[i];
            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return new IntArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    private void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            values[positionIndex] = values[i];
            if (compactNulls) {
                nulls[positionIndex] = nulls[i];
            }
            outputPositions[positionIndex] = nextPosition;

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }
            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
    }

    @Override
    public void close()
    {
        values = null;
        outputPositions = null;
        nulls = null;

        presentStream = null;
        presentStreamSource = null;
        dataStream = null;
        dataStreamSource = null;

        systemMemoryContext.close();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
