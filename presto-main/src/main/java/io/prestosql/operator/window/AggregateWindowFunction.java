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
package io.prestosql.operator.window;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.aggregation.Accumulator;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.function.WindowFunction;
import io.prestosql.spi.function.WindowIndex;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"argumentChannels", "windowIndex", "accumulatorFactory"})
public class AggregateWindowFunction
        implements WindowFunction
{
    private final List<Integer> argumentChannels;
    private final AccumulatorFactory accumulatorFactory;

    // Snapshot: all windowIndex operations revolves around pagesIndex which is passed in and captured/restored outside
    // windowIndex fields in all window functions are reset when WindowPartition is created(see WindowPartition line 71)
    // so it doesn't need to be captured.
    private WindowIndex windowIndex;
    private Accumulator accumulator;
    private int currentStart;
    private int currentEnd;

    private AggregateWindowFunction(InternalAggregationFunction function, List<Integer> argumentChannels)
    {
        this.argumentChannels = ImmutableList.copyOf(argumentChannels);
        this.accumulatorFactory = function.bind(createArgs(function), Optional.empty());
    }

    @Override
    public void reset(WindowIndex windowIndex)
    {
        this.windowIndex = windowIndex;
        resetAccumulator();
    }

    @Override
    public void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd)
    {
        if (frameStart < 0) {
            // empty frame
            resetAccumulator();
        }
        else if ((frameStart == currentStart) && (frameEnd >= currentEnd)) {
            // same or expanding frame
            accumulate(currentEnd + 1, frameEnd);
            currentEnd = frameEnd;
        }
        else {
            // different frame
            resetAccumulator();
            accumulate(frameStart, frameEnd);
            currentStart = frameStart;
            currentEnd = frameEnd;
        }

        accumulator.evaluateFinal(output);
    }

    private void accumulate(int start, int end)
    {
        accumulator.addInput(windowIndex, argumentChannels, start, end);
    }

    private void resetAccumulator()
    {
        if (currentStart >= 0) {
            accumulator = accumulatorFactory.createAccumulator();
            currentStart = -1;
            currentEnd = -1;
        }
    }

    public static WindowFunctionSupplier supplier(Signature signature, final InternalAggregationFunction function)
    {
        requireNonNull(function, "function is null");
        return new AbstractWindowFunctionSupplier(signature, null)
        {
            @Override
            protected WindowFunction newWindowFunction(List<Integer> inputs)
            {
                return new AggregateWindowFunction(function, inputs);
            }
        };
    }

    private static List<Integer> createArgs(InternalAggregationFunction function)
    {
        ImmutableList.Builder<Integer> list = ImmutableList.builder();
        for (int i = 0; i < function.getParameterTypes().size(); i++) {
            list.add(i);
        }
        return list.build();
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        AggregateWindowFunctionState myState = new AggregateWindowFunctionState();
        if (accumulator != null) {
            myState.accumulator = accumulator.capture(serdeProvider);
        }
        myState.currentStart = currentStart;
        myState.currentEnd = currentEnd;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        AggregateWindowFunctionState myState = (AggregateWindowFunctionState) state;
        if (myState.accumulator == null) {
            this.accumulator = null;
        }
        else {
            if (this.accumulator == null) {
                this.accumulator = accumulatorFactory.createAccumulator();
            }
            this.accumulator.restore(myState.accumulator, serdeProvider);
        }
        this.currentStart = myState.currentStart;
        this.currentEnd = myState.currentEnd;
    }

    private static class AggregateWindowFunctionState
            implements Serializable
    {
        private Object accumulator;
        private int currentStart;
        private int currentEnd;
    }
}
