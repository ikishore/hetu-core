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

import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.RankingWindowFunction;
import io.prestosql.spi.function.WindowFunctionSignature;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;

import java.io.Serializable;

import static io.prestosql.spi.type.DoubleType.DOUBLE;

@WindowFunctionSignature(name = "cume_dist", returnType = "double")
public class CumulativeDistributionFunction
        extends RankingWindowFunction
{
    private long totalCount;
    private long count;

    @Override
    public void reset()
    {
        totalCount = windowIndex.size();
        count = 0;
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount, int currentPosition)
    {
        if (newPeerGroup) {
            count += peerGroupCount;
        }
        DOUBLE.writeDouble(output, ((double) count) / totalCount);
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        CumulativeDistributionFunctionState myState = new CumulativeDistributionFunctionState();
        myState.totalCount = totalCount;
        myState.count = count;
        myState.baseState = super.capture(serdeProvider);
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        CumulativeDistributionFunctionState myState = (CumulativeDistributionFunctionState) state;
        this.totalCount = myState.totalCount;
        this.count = myState.count;
        super.restore(myState.baseState, serdeProvider);
    }

    private static class CumulativeDistributionFunctionState
            implements Serializable
    {
        private long totalCount;
        private long count;
        private Object baseState;
    }
}
