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
package io.prestosql.execution;

import io.airlift.concurrent.SetThreadName;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.Session;
import io.prestosql.event.SplitMonitor;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.execution.executor.TaskExecutor;
import io.prestosql.memory.QueryContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.CommonTableExecutionContext;
import io.prestosql.operator.RouterContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.TypeProvider;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executor;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.SystemSessionProperties.isExchangeCompressionEnabled;
import static io.prestosql.execution.SqlTaskExecution.createSqlTaskExecution;
import static java.util.Objects.requireNonNull;

public class SqlTaskExecutionFactory
{
    private final Executor taskNotificationExecutor;

    private final TaskExecutor taskExecutor;

    private final LocalExecutionPlanner planner;
    private final SplitMonitor splitMonitor;
    private final boolean perOperatorCpuTimerEnabled;
    private final boolean cpuTimerEnabled;
    private final Metadata metadata;

    public SqlTaskExecutionFactory(
            Executor taskNotificationExecutor,
            TaskExecutor taskExecutor,
            LocalExecutionPlanner planner,
            SplitMonitor splitMonitor,
            TaskManagerConfig config,
            Metadata metadata)
    {
        this.taskNotificationExecutor = requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.planner = requireNonNull(planner, "planner is null");
        this.splitMonitor = requireNonNull(splitMonitor, "splitMonitor is null");
        requireNonNull(config, "config is null");
        this.perOperatorCpuTimerEnabled = config.isPerOperatorCpuTimerEnabled();
        this.cpuTimerEnabled = config.isTaskCpuTimerEnabled();
        this.metadata = metadata;
    }

    public SqlTaskExecution create(String taskInstanceId, Session session, QueryContext queryContext, TaskStateMachine taskStateMachine, OutputBuffer outputBuffer, PlanFragment fragment, List<TaskSource> sources, OptionalInt totalPartitions, Optional<PlanNodeId> consumer,
            Map<String, CommonTableExecutionContext> cteCtx, Map<String, RouterContext> routerCtx)
    {
        TaskContext taskContext = queryContext.addTaskContext(
                taskInstanceId,
                taskStateMachine,
                session,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled,
                totalPartitions,
                consumer,
                new PagesSerdeFactory(metadata.getFunctionAndTypeManager().getBlockEncodingSerde(), isExchangeCompressionEnabled(session)));

        LocalExecutionPlan localExecutionPlan;
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskStateMachine.getTaskId())) {
            try {
                localExecutionPlan = planner.plan(
                        taskContext,
                        fragment.getRoot(),
                        TypeProvider.copyOf(fragment.getSymbols()),
                        fragment.getPartitioningScheme(),
                        fragment.getStageExecutionDescriptor(),
                        fragment.getPartitionedSources(),
                        outputBuffer,
                        fragment.getFeederCTEId(),
                        fragment.getFeederCTEParentId(),
                        cteCtx,
                        routerCtx);
            }
            catch (Throwable e) {
                // planning failed
                taskStateMachine.failed(e);
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
        return createSqlTaskExecution(
                taskStateMachine,
                taskContext,
                outputBuffer,
                sources,
                localExecutionPlan,
                taskExecutor,
                taskNotificationExecutor,
                splitMonitor);
    }
}
