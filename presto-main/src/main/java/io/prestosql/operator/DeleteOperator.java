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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.RestorableConfig;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

//TODO-cp-I38S9Q: Operator currently not supported for Snapshot
@RestorableConfig(unsupported = true)
public class DeleteOperator
        extends AbstractRowChangeOperator
{
    public static class DeleteOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final int rowIdChannel;
        private boolean closed;

        public DeleteOperatorFactory(int operatorId, PlanNodeId planNodeId, int rowIdChannel)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.rowIdChannel = rowIdChannel;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, DeleteOperator.class.getSimpleName());
            return new DeleteOperator(context, rowIdChannel);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new DeleteOperatorFactory(operatorId, planNodeId, rowIdChannel);
        }
    }

    private final int rowIdChannel;

    public DeleteOperator(OperatorContext operatorContext, int rowIdChannel)
    {
        super(operatorContext);
        this.rowIdChannel = rowIdChannel;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);

        //TODO-cp-I38S9Q: Operator currently not supported for Snapshot
        if (page instanceof MarkerPage) {
            throw new UnsupportedOperationException("Operator doesn't support snapshotting.");
        }

        Block rowIds = page.getBlock(rowIdChannel);
        pageSource().deleteRows(rowIds);
        rowCount += rowIds.getPositionCount();
    }

    @Override
    public Page pollMarker()
    {
        //TODO-cp-I38S9Q: Operator currently not supported for Snapshot
        return null;
    }
}
