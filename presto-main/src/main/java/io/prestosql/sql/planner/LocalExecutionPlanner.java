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
package io.prestosql.sql.planner;

import com.google.common.base.VerifyException;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cube.CubeManager;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.execution.*;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.index.IndexManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.*;
import io.prestosql.operator.AggregationOperator.AggregationOperatorFactory;
import io.prestosql.operator.CubeFinishOperator.CubeFinishOperatorFactory;
import io.prestosql.operator.DeleteOperator.DeleteOperatorFactory;
import io.prestosql.operator.DevNullOperator.DevNullOperatorFactory;
import io.prestosql.operator.ExchangeOperator.ExchangeOperatorFactory;
import io.prestosql.operator.ExplainAnalyzeOperator.ExplainAnalyzeOperatorFactory;
import io.prestosql.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.prestosql.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import io.prestosql.operator.HashSemiJoinOperator.HashSemiJoinOperatorFactory;
import io.prestosql.operator.JoinOperatorFactory.OuterOperatorFactoryResult;
import io.prestosql.operator.LimitOperator.LimitOperatorFactory;
import io.prestosql.operator.LookupOuterOperator.LookupOuterOperatorFactory;
import io.prestosql.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import io.prestosql.operator.MergeOperator.MergeOperatorFactory;
import io.prestosql.operator.OrderByOperator.OrderByOperatorFactory;
import io.prestosql.operator.PartitionedOutputOperator.PartitionedOutputFactory;
import io.prestosql.operator.PipelineExecutionStrategy;
import io.prestosql.operator.Router;
import io.prestosql.operator.RouterContext;
import io.prestosql.operator.RowNumberOperator;
import io.prestosql.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.prestosql.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import io.prestosql.operator.SetBuilderOperator.SetSupplier;
import io.prestosql.operator.SpatialIndexBuilderOperator.SpatialIndexBuilderOperatorFactory;
import io.prestosql.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.prestosql.operator.SpatialJoinOperator.SpatialJoinOperatorFactory;
import io.prestosql.operator.StatisticsWriterOperator.StatisticsWriterOperatorFactory;
import io.prestosql.operator.StreamingAggregationOperator.StreamingAggregationOperatorFactory;
import io.prestosql.operator.TableDeleteOperator.TableDeleteOperatorFactory;
import io.prestosql.operator.TableScanOperator.TableScanOperatorFactory;
import io.prestosql.operator.TaskOutputOperator.TaskOutputFactory;
import io.prestosql.operator.TopNOperator.TopNOperatorFactory;
import io.prestosql.operator.ValuesOperator.ValuesOperatorFactory;
import io.prestosql.operator.WindowOperator.WindowOperatorFactory;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.aggregation.LambdaProvider;
import io.prestosql.operator.dynamicfilter.CrossRegionDynamicFilterOperator;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.operator.exchange.LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory;
import io.prestosql.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import io.prestosql.operator.exchange.LocalMergeSourceOperator.LocalMergeSourceOperatorFactory;
import io.prestosql.operator.exchange.PageChannelSelector;
import io.prestosql.operator.index.DynamicTupleFilterFactory;
import io.prestosql.operator.index.FieldSetFilteringRecordSet;
import io.prestosql.operator.index.IndexBuildDriverFactoryProvider;
import io.prestosql.operator.index.IndexJoinLookupStats;
import io.prestosql.operator.index.IndexLookupSourceFactory;
import io.prestosql.operator.index.IndexSourceOperator;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.operator.project.SharedFilterProcessor;
import io.prestosql.operator.window.FrameInfo;
import io.prestosql.operator.window.WindowFunctionSupplier;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorIndex;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.AggregationNode.Step;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.RouterNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.plan.WindowNode.Frame;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.*;
import io.prestosql.spi.sql.RowExpressionUtils;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.PartitioningSpillerFactory;
import io.prestosql.spiller.SingleStreamSpillerFactory;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.split.MappedRecordSet;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.planner.optimizations.IndexJoinOptimizer;
import io.prestosql.sql.planner.plan.*;
import io.prestosql.sql.planner.plan.TableWriterNode.DeleteTarget;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.relational.VariableToChannelTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.listener.StateStoreListenerManager;
import org.apache.commons.collections.map.HashedMap;

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.DiscreteDomain.integers;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Range.closedOpen;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.SystemSessionProperties.*;
import static io.prestosql.SystemSessionProperties.getAggregationOperatorUnspillMemoryLimit;
import static io.prestosql.SystemSessionProperties.getCteMaxPrefetchQueueSize;
import static io.prestosql.SystemSessionProperties.getCteMaxQueueSize;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverSize;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverValueCount;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringWaitTime;
import static io.prestosql.SystemSessionProperties.getFilterAndProjectMinOutputPageRowCount;
import static io.prestosql.SystemSessionProperties.getFilterAndProjectMinOutputPageSize;
import static io.prestosql.SystemSessionProperties.getSpillOperatorThresholdReuseExchange;
import static io.prestosql.SystemSessionProperties.getTaskConcurrency;
import static io.prestosql.SystemSessionProperties.getTaskWriterCount;
import static io.prestosql.SystemSessionProperties.isCrossRegionDynamicFilterEnabled;
import static io.prestosql.SystemSessionProperties.isEnableDynamicFiltering;
import static io.prestosql.SystemSessionProperties.isExchangeCompressionEnabled;
import static io.prestosql.SystemSessionProperties.isSpillEnabled;
import static io.prestosql.SystemSessionProperties.isSpillOrderBy;
import static io.prestosql.SystemSessionProperties.isSpillReuseExchange;
import static io.prestosql.SystemSessionProperties.isSpillWindowOperator;
import static io.prestosql.dynamicfilter.DynamicFilterCacheManager.createCacheKey;
import static io.prestosql.expressions.RowExpressionNodeInliner.replaceExpression;
import static io.prestosql.operator.CommonTableExpressionOperator.CommonTableExpressionOperatorFactory;
import static io.prestosql.operator.CreateIndexOperator.CreateIndexOperatorFactory;
import static io.prestosql.operator.DistinctLimitOperator.DistinctLimitOperatorFactory;
import static io.prestosql.operator.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import static io.prestosql.operator.NestedLoopJoinOperator.NestedLoopJoinOperatorFactory;
import static io.prestosql.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.operator.TableFinishOperator.TableFinishOperatorFactory;
import static io.prestosql.operator.TableFinishOperator.TableFinisher;
import static io.prestosql.operator.TableWriterOperator.FRAGMENT_CHANNEL;
import static io.prestosql.operator.TableWriterOperator.ROW_COUNT_CHANNEL;
import static io.prestosql.operator.TableWriterOperator.STATS_START_CHANNEL;
import static io.prestosql.operator.TableWriterOperator.TableWriterOperatorFactory;
import static io.prestosql.operator.WindowFunctionDefinition.window;
import static io.prestosql.operator.unnest.UnnestOperator.UnnestOperatorFactory;
import static io.prestosql.spi.StandardErrorCode.COMPILER_ERROR;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.function.OperatorType.*;
import static io.prestosql.spi.function.Signature.unmangleOperator;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT;
import static io.prestosql.spi.plan.AggregationNode.Step.FINAL;
import static io.prestosql.spi.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.spi.plan.JoinNode.Type.FULL;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.spi.plan.JoinNode.Type.RIGHT;
import static io.prestosql.spi.sql.RowExpressionUtils.TRUE_CONSTANT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static io.prestosql.spi.util.Reflection.constructorMethodHandle;
import static io.prestosql.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static io.prestosql.sql.gen.LambdaBytecodeGenerator.compileLambdaProvider;
import static io.prestosql.sql.planner.RowExpressionInterpreter.Level.OPTIMIZED;
import static io.prestosql.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toSymbol;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toVariableReference;
import static io.prestosql.sql.planner.plan.AssignmentUtils.identityAssignments;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.DeleteAsInsertTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.InsertTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.UpdateTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.VacuumTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.WriterTarget;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static io.prestosql.util.SpatialJoinUtils.ST_CONTAINS;
import static io.prestosql.util.SpatialJoinUtils.ST_DISTANCE;
import static io.prestosql.util.SpatialJoinUtils.ST_INTERSECTS;
import static io.prestosql.util.SpatialJoinUtils.ST_WITHIN;
import static io.prestosql.util.SpatialJoinUtils.extractSupportedSpatialComparisons;
import static io.prestosql.util.SpatialJoinUtils.extractSupportedSpatialFunctions;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;

public class LocalExecutionPlanner
{
    private static final Logger log = Logger.get(LocalExecutionPlanner.class);

    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;
    private final Optional<ExplainAnalyzeContext> explainAnalyzeContext;
    private final PageSourceProvider pageSourceProvider;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final PageSinkManager pageSinkManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final ExpressionCompiler expressionCompiler;
    private final PageFunctionCompiler pageFunctionCompiler;
    private final JoinFilterFunctionCompiler joinFilterFunctionCompiler;
    private final DataSize maxIndexMemorySize;
    private final IndexJoinLookupStats indexJoinLookupStats;
    private final DataSize maxPartialAggregationMemorySize;
    private final DataSize maxPagePartitioningBufferSize;
    private final DataSize maxLocalExchangeBufferSize;
    private final SpillerFactory spillerFactory;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;
    private final PagesIndex.Factory pagesIndexFactory;
    private final JoinCompiler joinCompiler;
    private final LookupJoinOperators lookupJoinOperators;
    private final OrderingCompiler orderingCompiler;
    private final StateStoreProvider stateStoreProvider;
    private final NodeInfo nodeInfo;
    private final CubeManager cubeManager;
    private final StateStoreListenerManager stateStoreListenerManager;
    private final DynamicFilterCacheManager dynamicFilterCacheManager;
    private final HeuristicIndexerManager heuristicIndexerManager;

    //for BQO
    private final SharedPagesIndex.Factory sharedPagesIndexFactory;
    private final SharedJoinOperators sharedOperators;

    @Inject
    public LocalExecutionPlanner(
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            Optional<ExplainAnalyzeContext> explainAnalyzeContext,
            PageSourceProvider pageSourceProvider,
            IndexManager indexManager,
            NodePartitioningManager nodePartitioningManager,
            PageSinkManager pageSinkManager,
            ExchangeClientSupplier exchangeClientSupplier,
            ExpressionCompiler expressionCompiler,
            PageFunctionCompiler pageFunctionCompiler,
            JoinFilterFunctionCompiler joinFilterFunctionCompiler,
            IndexJoinLookupStats indexJoinLookupStats,
            TaskManagerConfig taskManagerConfig,
            SpillerFactory spillerFactory,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            PartitioningSpillerFactory partitioningSpillerFactory,
            PagesIndex.Factory pagesIndexFactory,
            SharedPagesIndex.Factory sharedPagesIndexFactory,  //BQO
            JoinCompiler joinCompiler,
            LookupJoinOperators lookupJoinOperators,
            SharedJoinOperators sharedOperators, //BQO
            OrderingCompiler orderingCompiler,
            NodeInfo nodeInfo,
            StateStoreProvider stateStoreProvider,
            StateStoreListenerManager stateStoreListenerManager,
            DynamicFilterCacheManager dynamicFilterCacheManager,
            HeuristicIndexerManager heuristicIndexerManager,
            CubeManager cubeManager
            )
    {
        this.explainAnalyzeContext = requireNonNull(explainAnalyzeContext, "explainAnalyzeContext is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.indexManager = requireNonNull(indexManager, "indexManager is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
        this.expressionCompiler = requireNonNull(expressionCompiler, "compiler is null");
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        this.joinFilterFunctionCompiler = requireNonNull(joinFilterFunctionCompiler, "compiler is null");
        this.indexJoinLookupStats = requireNonNull(indexJoinLookupStats, "indexJoinLookupStats is null");
        this.maxIndexMemorySize = requireNonNull(taskManagerConfig, "taskManagerConfig is null").getMaxIndexMemoryUsage();
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.maxPartialAggregationMemorySize = taskManagerConfig.getMaxPartialAggregationMemoryUsage();
        this.maxPagePartitioningBufferSize = taskManagerConfig.getMaxPagePartitioningBufferSize();
        this.maxLocalExchangeBufferSize = taskManagerConfig.getMaxLocalExchangeBufferSize();
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.lookupJoinOperators = requireNonNull(lookupJoinOperators, "lookupJoinOperators is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStore is null");
        this.nodeInfo = nodeInfo;
        this.stateStoreListenerManager = requireNonNull(stateStoreListenerManager, "stateStoreListenerManager is null");
        this.dynamicFilterCacheManager = requireNonNull(dynamicFilterCacheManager, "dynamicFilterCacheManager is null");
        this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
        this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");

        this.sharedPagesIndexFactory = requireNonNull(sharedPagesIndexFactory, "sharedPagesIndexFactory is null"); //BQO
        this.sharedOperators = requireNonNull(sharedOperators, "sharedJoinOperators is null"); //BQO
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanNode plan,
            TypeProvider types,
            PartitioningScheme partitioningScheme,
            StageExecutionDescriptor stageExecutionDescriptor,
            List<PlanNodeId> partitionedSourceOrder,
            OutputBuffer outputBuffer,
            Optional<PlanFragmentId> producerCTEId,
            Optional<PlanNodeId> producerCTEParentId,
            Map<String, CommonTableExecutionContext> cteCtx,
            Map<String, RouterContext> routerCtx)
    {
        List<Symbol> outputLayout = partitioningScheme.getOutputLayout();

        System.out.println("Scheme: " + partitioningScheme.getPartitioning().getHandle().toString());

        if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SCALED_WRITER_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(COORDINATOR_DISTRIBUTION)) {
            return plan(taskContext, stageExecutionDescriptor, plan, outputLayout, types, partitionedSourceOrder, new TaskOutputFactory(outputBuffer), producerCTEId, producerCTEParentId, cteCtx, routerCtx);
        }

        System.out.println("Still here " + partitioningScheme.getPartitioning().getHandle().toString());

        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        List<Integer> partitionChannels;
        List<Optional<NullableValue>> partitionConstants;
        List<Type> partitionChannelTypes;
        if (partitioningScheme.getHashColumn().isPresent()) {
            partitionChannels = ImmutableList.of(outputLayout.indexOf(partitioningScheme.getHashColumn().get()));
            partitionConstants = ImmutableList.of(Optional.empty());
            partitionChannelTypes = ImmutableList.of(BIGINT);
        }
        else {
            partitionChannels = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return -1;
                        }
                        return outputLayout.indexOf(argument.getColumn());
                    })
                    .collect(toImmutableList());
            partitionConstants = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return Optional.of(argument.getConstant());
                        }
                        return Optional.<NullableValue>empty();
                    })
                    .collect(toImmutableList());
            partitionChannelTypes = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return argument.getConstant().getType();
                        }
                        return types.get(argument.getColumn());
                    })
                    .collect(toImmutableList());
        }

        System.out.println("Still here " + partitioningScheme.getPartitioning().getHandle().toString());

        PartitionFunction partitionFunction = nodePartitioningManager.getPartitionFunction(taskContext.getSession(), partitioningScheme, partitionChannelTypes);
        OptionalInt nullChannel = OptionalInt.empty();
        Set<Symbol> partitioningColumns = partitioningScheme.getPartitioning().getColumns();

        System.out.println("Still here " + partitionFunction.getClass().toString());

        // partitioningColumns expected to have one column in the normal case, and zero columns when partitioning on a constant
        checkArgument(!partitioningScheme.isReplicateNullsAndAny() || partitioningColumns.size() <= 1);
        if (partitioningScheme.isReplicateNullsAndAny() && partitioningColumns.size() == 1) {
            nullChannel = OptionalInt.of(outputLayout.indexOf(getOnlyElement(partitioningColumns)));
        }

        return plan(
                taskContext,
                stageExecutionDescriptor,
                plan,
                outputLayout,
                types,
                partitionedSourceOrder,
                new PartitionedOutputFactory(
                        partitionFunction,
                        partitionChannels,
                        partitionConstants,
                        partitioningScheme.isReplicateNullsAndAny(),
                        nullChannel,
                        outputBuffer,
                        maxPagePartitioningBufferSize),
                producerCTEId,
                producerCTEParentId,
                cteCtx,
                routerCtx);
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            StageExecutionDescriptor stageExecutionDescriptor,
            PlanNode plan,
            List<Symbol> outputLayout,
            TypeProvider types,
            List<PlanNodeId> partitionedSourceOrder,
            OutputFactory outputOperatorFactory,
            Optional<PlanFragmentId> producerCTEId,
            Optional<PlanNodeId> producerCTEParentId,
            Map<String, CommonTableExecutionContext> cteCtx,
            Map<String, RouterContext> routerCtx)
    {
        Session session = taskContext.getSession();
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(taskContext, types, metadata, dynamicFilterCacheManager, producerCTEId, producerCTEParentId, cteCtx, routerCtx);

        PhysicalOperation physicalOperation = plan.accept(new Visitor(session, stageExecutionDescriptor), context);

        Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(outputLayout, physicalOperation.getLayout());

        List<Type> outputTypes = outputLayout.stream()
                .map(types::get)
                .collect(toImmutableList());

        context.addDriverFactory(
                context.isInputDriver(),
                true,
                ImmutableList.<OperatorFactory>builder()
                        .addAll(physicalOperation.getOperatorFactories())
                        .add(outputOperatorFactory.createOutputOperator(
                                context.getNextOperatorId(),
                                plan.getId(),
                                outputTypes,
                                pagePreprocessor,
                                new PagesSerdeFactory(metadata.getBlockEncodingSerde(), isExchangeCompressionEnabled(session))))
                        .build(),
                context.getDriverInstanceCount(),
                physicalOperation.getPipelineExecutionStrategy());

        addLookupOuterDrivers(context);

        // notify operator factories that planning has completed
        context.getDriverFactories().stream()
                .map(DriverFactory::getOperatorFactories)
                .flatMap(List::stream)
                .filter(LocalPlannerAware.class::isInstance)
                .map(LocalPlannerAware.class::cast)
                .forEach(LocalPlannerAware::localPlannerComplete);

        return new LocalExecutionPlan(context.getDriverFactories(), partitionedSourceOrder, stageExecutionDescriptor, producerCTEId);
    }

    public static boolean readSharedOperationValue(){

        try {
            FileInputStream fstream = new FileInputStream("/scratch/venkates/bqo-working/input.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            int value = 0;
            while ((strLine = br.readLine()) != null) {
                value = Integer.parseInt(strLine);
            }
            // Close the input stream
            br.close();
            fstream.close();
            //print the contents of a
            if(value == 1){
                //System.out.println("VSK: Shared operation is true");
                return  true;
            }
            else {
                //System.out.println("VSK: Shared operation is false");
                return false;
            }

        } catch (Exception e) {// Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
        //System.out.println("VSK: It should not come here");
        return false;
    }

    private static void addLookupOuterDrivers(LocalExecutionPlanContext context)
    {
        // For an outer join on the lookup side (RIGHT or FULL) add an additional
        // driver to output the unused rows in the lookup source
        for (DriverFactory factory : context.getDriverFactories()) {
            List<OperatorFactory> operatorFactories = factory.getOperatorFactories();
            for (int i = 0; i < operatorFactories.size(); i++) {
                OperatorFactory operatorFactory = operatorFactories.get(i);
                if (!(operatorFactory instanceof JoinOperatorFactory || operatorFactory instanceof SharedJoinOperatorFactory)) {
                    continue;
                }

                Optional<OuterOperatorFactoryResult> outerOperatorFactoryResult = null;

                if(operatorFactory instanceof SharedJoinOperatorFactory) {
                    SharedJoinOperatorFactory lookupJoin = (SharedJoinOperatorFactory) operatorFactory;
                    outerOperatorFactoryResult = null;
                }
                else {
                    JoinOperatorFactory lookupJoin = (JoinOperatorFactory) operatorFactory;
                    outerOperatorFactoryResult = lookupJoin.createOuterOperatorFactory();
                }
                if (outerOperatorFactoryResult !=null && outerOperatorFactoryResult.isPresent()) {
                    // Add a new driver to output the unmatched rows in an outer join.
                    // We duplicate all of the factories above the JoinOperator (the ones reading from the joins),
                    // and replace the JoinOperator with the OuterOperator (the one that produces unmatched rows).
                    ImmutableList.Builder<OperatorFactory> newOperators = ImmutableList.builder();
                    newOperators.add(outerOperatorFactoryResult.get().getOuterOperatorFactory());
                    operatorFactories.subList(i + 1, operatorFactories.size()).stream()
                            .map(OperatorFactory::duplicate)
                            .forEach(newOperators::add);

                    context.addDriverFactory(false, factory.isOutputDriver(), newOperators.build(), OptionalInt.of(1), outerOperatorFactoryResult.get().getBuildExecutionStrategy());
                }
            }
        }
    }

    private static class LocalExecutionPlanContext
    {
        private final TaskContext taskContext;
        private final TypeProvider types;
        private final List<DriverFactory> driverFactories;
        private final Optional<IndexSourceContext> indexSourceContext;

        // the collector is shared with all subContexts to allow local dynamic filtering
        // with multiple table scans (e.g. co-located joins).
        private final LocalDynamicFiltersCollector dynamicFiltersCollector;

        // this is shared with all subContexts
        private final AtomicInteger nextPipelineId;

        private int nextOperatorId;
        private boolean inputDriver = true;
        private OptionalInt driverInstanceCount = OptionalInt.empty();
        private Map<PlanNodeId, OperatorFactory> cteOperationMap = new HashMap<>();
        private Map<PlanNodeId, OperatorFactory> routerOperationMap = new HashMap<>();
        private Map<String, CommonTableExecutionContext> cteCtx;
        Map<String, RouterContext> routerCtx;
        private static Map<String, PhysicalOperation> sourceInitialized = new ConcurrentHashMap<>();
        private final PlanNodeId consumerId;
        private final Optional<PlanFragmentId> producerCTEId;
        private final Optional<PlanNodeId> producerCTEParentId;

        public LocalExecutionPlanContext(TaskContext taskContext, TypeProvider types, Metadata metadata, DynamicFilterCacheManager dynamicFilterCacheManager,
                                         Optional<PlanFragmentId> producerCTEId,
                                         Optional<PlanNodeId> producerCTEParentId,
                                         Map<String, CommonTableExecutionContext> cteCtx,
                                         Map<String, RouterContext> routerCtx)
        {
            this(taskContext, types, new ArrayList<>(), Optional.empty(), new LocalDynamicFiltersCollector(taskContext, Optional.of(metadata), dynamicFilterCacheManager), new AtomicInteger(0), producerCTEId, producerCTEParentId, cteCtx, routerCtx);
        }

        private LocalExecutionPlanContext(
                TaskContext taskContext,
                TypeProvider types,
                List<DriverFactory> driverFactories,
                Optional<IndexSourceContext> indexSourceContext,
                LocalDynamicFiltersCollector dynamicFiltersCollector,
                AtomicInteger nextPipelineId,
                Optional<PlanFragmentId> producerCTEId,
                Optional<PlanNodeId> producerCTEParentId,
                Map<String, CommonTableExecutionContext> cteCtx,
                Map<String, RouterContext> routerCtx)
        {
            this.taskContext = taskContext;
            this.types = types;
            this.driverFactories = driverFactories;
            this.indexSourceContext = indexSourceContext;
            this.dynamicFiltersCollector = dynamicFiltersCollector;
            this.nextPipelineId = nextPipelineId;
            this.consumerId = taskContext.getConsumerId();
            this.producerCTEId = producerCTEId;
            this.producerCTEParentId = producerCTEParentId;
            this.cteCtx = cteCtx;
            this.routerCtx = routerCtx;
        }

        public void addDriverFactory(boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances, PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            if (pipelineExecutionStrategy == GROUPED_EXECUTION) {
                OperatorFactory firstOperatorFactory = operatorFactories.get(0);
                if (inputDriver) {
                    checkArgument(firstOperatorFactory instanceof ScanFilterAndProjectOperatorFactory || firstOperatorFactory instanceof TableScanOperatorFactory);
                }
                else {
                    checkArgument(firstOperatorFactory instanceof LocalExchangeSourceOperatorFactory || firstOperatorFactory instanceof LookupOuterOperatorFactory);
                }
            }

            if (SystemSessionProperties.isWorkProcessorPipelines(taskContext.getSession())) {
                operatorFactories = WorkProcessorPipelineSourceOperator.convertOperators(getNextOperatorId(), operatorFactories);
            }

            driverFactories.add(new DriverFactory(getNextPipelineId(), inputDriver, outputDriver, operatorFactories, driverInstances, pipelineExecutionStrategy));
        }

        private List<DriverFactory> getDriverFactories()
        {
            return ImmutableList.copyOf(driverFactories);
        }

        public Session getSession()
        {
            return taskContext.getSession();
        }

        public StageId getStageId()
        {
            return taskContext.getTaskId().getStageId();
        }

        public TaskId getTaskId()
        {
            return taskContext.getTaskId();
        }

        public TypeProvider getTypes()
        {
            return types;
        }

        public LocalDynamicFiltersCollector getDynamicFiltersCollector()
        {
            return dynamicFiltersCollector;
        }

        public Optional<IndexSourceContext> getIndexSourceContext()
        {
            return indexSourceContext;
        }

        private int getNextPipelineId()
        {
            return nextPipelineId.getAndIncrement();
        }

        private int getNextOperatorId()
        {
            return nextOperatorId++;
        }

        private boolean isInputDriver()
        {
            return inputDriver;
        }

        private void setInputDriver(boolean inputDriver)
        {
            this.inputDriver = inputDriver;
        }

        public LocalExecutionPlanContext createSubContext()
        {
            checkState(!indexSourceContext.isPresent(), "index build plan can not have sub-contexts");
            return new LocalExecutionPlanContext(taskContext, types, driverFactories, indexSourceContext, dynamicFiltersCollector, nextPipelineId, producerCTEId, producerCTEParentId, cteCtx, routerCtx);
        }

        public LocalExecutionPlanContext createIndexSourceSubContext(IndexSourceContext indexSourceContext)
        {
            return new LocalExecutionPlanContext(taskContext, types, driverFactories, Optional.of(indexSourceContext), dynamicFiltersCollector, nextPipelineId, producerCTEId, producerCTEParentId, cteCtx, routerCtx);
        }

        public OptionalInt getDriverInstanceCount()
        {
            return driverInstanceCount;
        }

        public void setDriverInstanceCount(int driverInstanceCount)
        {
            checkArgument(driverInstanceCount > 0, "driverInstanceCount must be > 0");
            if (this.driverInstanceCount.isPresent()) {
                checkState(this.driverInstanceCount.getAsInt() == driverInstanceCount, "driverInstance count already set to " + this.driverInstanceCount.getAsInt());
            }
            this.driverInstanceCount = OptionalInt.of(driverInstanceCount);
        }

        public CommonTableExecutionContext getRunningTask(String cteExecutorId, Set<PlanNodeId> consumers, PhysicalOperation source)
        {
            checkArgument(producerCTEParentId.isPresent(), "CTE parent Id must be there");
            if (source != null) {
                sourceInitialized.putIfAbsent(cteExecutorId, source);
            }

            return cteCtx.computeIfAbsent(cteExecutorId, k -> new CommonTableExecutionContext(cteExecutorId, consumers,
                                                            producerCTEParentId.get(), taskContext.getNotificationExecutor(),
                                                            taskContext.getTaskCount(),
                                                            getCteMaxQueueSize(getSession()),
                                                            getCteMaxPrefetchQueueSize(getSession())));
        }

        public RouterContext getRouterRunningTask(String routerExecutorId, Map<Integer, Long> paths, int querySetChannel, PhysicalOperation source)
        {
            checkArgument(producerCTEParentId.isPresent(), "CTE parent Id must be there");
            if (source != null) {
                sourceInitialized.putIfAbsent(routerExecutorId, source);
            }

            //System.out.println("EMPRHATIC ctx" + routerCtx);
            //System.out.println("EMPRHATIC");
            //System.out.println("EMPRHATIC");
            //System.out.println("EMPRHATIC");
            //System.out.println("EMPRHATIC");

            List<Long> partitions = new ArrayList<>();
            for (Map.Entry<Integer, Long> entry : paths.entrySet()) {
                //System.out.println(entry);
                if (entry.getKey() != -1) {
                    partitions.add(entry.getValue());
                } else {
                    Long mask = entry.getValue();
                    while (mask != 0) {
                        Long next = mask & (mask-1);

                        Long cur = mask - next;

                        partitions.add(cur);

                        //System.out.println(cur);

                        mask = next;
                    }
                }
            }
            //System.out.println("EMPRHATIC");
            //System.out.println("EMPRHATIC");
            //System.out.println("EMPRHATIC");
            //System.out.println("EMPRHATIC");

            //if (routerCtx.containsKey(routerExecutorId))
                //System.out.println("Already here!");

            return routerCtx.computeIfAbsent(routerExecutorId, k -> new RouterContext(partitions, querySetChannel));
        }

        public String getCteId(PlanNodeId cteNodeId)
        {
            return taskContext.getQueryContext().getQueryId().toString() + "-#-" + taskContext.getTaskId().getId() + "-#-" + cteNodeId.toString();
        }

        public String getRouterId(PlanNodeId cteNodeId)
        {
            return taskContext.getQueryContext().getQueryId().toString() + "-#-" + cteNodeId.toString();
        }

        public PlanNodeId getConsumerId()
        {
            checkArgument(consumerId != null, "Consumer Id cannot be null for CTE executor!");
            return consumerId;
        }

        public Optional<PlanFragmentId> getProducerCTEId()
        {
            return producerCTEId;
        }

        public Optional<PlanNodeId> getProducerCTEParentId()
        {
            return producerCTEParentId;
        }

        public boolean isCteInitialized(String cteId)
        {
            return sourceInitialized.containsKey(cteId);
        }

        public boolean isInitialized(String cteId)
        {
            return sourceInitialized.containsKey(cteId);
        }
    }

    private static class IndexSourceContext
    {
        private final SetMultimap<Symbol, Integer> indexLookupToProbeInput;

        public IndexSourceContext(SetMultimap<Symbol, Integer> indexLookupToProbeInput)
        {
            this.indexLookupToProbeInput = ImmutableSetMultimap.copyOf(requireNonNull(indexLookupToProbeInput, "indexLookupToProbeInput is null"));
        }

        private SetMultimap<Symbol, Integer> getIndexLookupToProbeInput()
        {
            return indexLookupToProbeInput;
        }
    }

    public static class LocalExecutionPlan
    {
        private final List<DriverFactory> driverFactories;
        private final List<PlanNodeId> partitionedSourceOrder;
        private final StageExecutionDescriptor stageExecutionDescriptor;
        private final Optional<PlanFragmentId> producerCTEId;

        public LocalExecutionPlan(List<DriverFactory> driverFactories, List<PlanNodeId> partitionedSourceOrder,
                                    StageExecutionDescriptor stageExecutionDescriptor, Optional<PlanFragmentId> producerCTEId)
        {
            this.driverFactories = ImmutableList.copyOf(requireNonNull(driverFactories, "driverFactories is null"));
            this.partitionedSourceOrder = ImmutableList.copyOf(requireNonNull(partitionedSourceOrder, "partitionedSourceOrder is null"));
            this.stageExecutionDescriptor = requireNonNull(stageExecutionDescriptor, "stageExecutionDescriptor is null");
            this.producerCTEId = producerCTEId;
        }

        public List<DriverFactory> getDriverFactories()
        {
            return driverFactories;
        }

        public List<PlanNodeId> getPartitionedSourceOrder()
        {
            return partitionedSourceOrder;
        }

        public StageExecutionDescriptor getStageExecutionDescriptor()
        {
            return stageExecutionDescriptor;
        }

        public Optional<PlanFragmentId> getProducerCTEId()
        {
            return producerCTEId;
        }
    }

    private class Visitor
            extends InternalPlanVisitor<PhysicalOperation, LocalExecutionPlanContext>
    {
        private final Session session;
        private final StageExecutionDescriptor stageExecutionDescriptor;
        private int DriverInstanceCountFudgeFactor = 1;

        private Visitor(Session session, StageExecutionDescriptor stageExecutionDescriptor)
        {
            this.session = session;
            DriverInstanceCountFudgeFactor = SystemSessionProperties.getSchedulerParameter(session);
            this.stageExecutionDescriptor = stageExecutionDescriptor;
            double threshold = 0.2;
            DataSize query_mem = getQueryMaxMemory(this.session);
            DataSize converted_mem = query_mem.convertTo(MEGABYTE);
        }

        @Override
        public PhysicalOperation visitRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (node.getOrderingScheme().isPresent()) {
                return createMergeSource(node, context);
            }

            return createRemoteSource(node, context);
        }

        private PhysicalOperation createMergeSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");

            //System.out.println("merge");
            // merging remote source must have a single driver
            context.setDriverInstanceCount(1);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), layout);
            List<SortOrder> sortOrder = orderingScheme.getOrderingList();

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            ImmutableList<Integer> outputChannels = IntStream.range(0, types.size())
                    .boxed()
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new MergeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    exchangeClientSupplier,
                    new PagesSerdeFactory(metadata.getBlockEncodingSerde(), isExchangeCompressionEnabled(session)),
                    orderingCompiler,
                    types,
                    outputChannels,
                    sortChannels,
                    sortOrder);

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        private PhysicalOperation createRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            //System.out.println("remote");
            if (!context.getDriverInstanceCount().isPresent()) {
                context.setDriverInstanceCount(getTaskConcurrency(session)/((int) DriverInstanceCountFudgeFactor));
            }

            OperatorFactory operatorFactory = new ExchangeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    exchangeClientSupplier,
                    new PagesSerdeFactory(metadata.getBlockEncodingSerde(), isExchangeCompressionEnabled(session)));

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitExplainAnalyze(ExplainAnalyzeNode node, LocalExecutionPlanContext context)
        {
            ExplainAnalyzeContext analyzeContext = explainAnalyzeContext
                    .orElseThrow(() -> new IllegalStateException("ExplainAnalyze can only run on coordinator"));
            PhysicalOperation source = node.getSource().accept(this, context);
            OperatorFactory operatorFactory = new ExplainAnalyzeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    analyzeContext.getQueryPerformanceFetcher(),
                    metadata,
                    node.isVerbose());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            // Only create DynamicFilterOperator if it's enabled
            if (isCrossRegionDynamicFilterEnabled(session)) {
                String queryId = context.getSession().getQueryId().getId();
                List<Symbol> inputSymbols = node.getSource().getOutputSymbols();
                OperatorFactory operatorFactory = new CrossRegionDynamicFilterOperator.CrossRegionDynamicFilterOperatorFactory(context.getNextOperatorId(), node.getId(), queryId, inputSymbols, context.getTypes(), dynamicFilterCacheManager, node.getColumnNames());
                return new PhysicalOperation(operatorFactory, makeLayout(node.getSource()), context, source);
            }

            return source;
        }

        @Override
        public PhysicalOperation visitRowNumber(RowNumberNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, source.getLayout());

            List<Type> partitionTypes = partitionChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(source.getLayout());

            // row number function goes in the last channel
            int channel = source.getTypes().size();
            outputMappings.put(node.getRowNumberSymbol(), channel);

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            OperatorFactory operatorFactory = new RowNumberOperator.RowNumberOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    node.getMaxRowCountPerPartition(),
                    hashChannel,
                    10_000,
                    joinCompiler);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        @Override
        public PhysicalOperation visitTopNRankingNumber(TopNRankingNumberNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, source.getLayout());
            List<Type> partitionTypes = partitionChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();
            List<Integer> sortChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());
            List<SortOrder> sortOrder = orderBySymbols.stream()
                    .map(symbol -> node.getOrderingScheme().getOrdering(symbol))
                    .collect(toImmutableList());

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(source.getLayout());

            if (!node.isPartial() || !partitionChannels.isEmpty()) {
                // row number function goes in the last channel
                int channel = source.getTypes().size();
                outputMappings.put(node.getRowNumberSymbol(), channel);
            }

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            OperatorFactory operatorFactory = new TopNRankingNumberOperator.TopNRankingNumberOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    sortOrder,
                    node.getMaxRowCountPerPartition(),
                    node.isPartial(),
                    hashChannel,
                    1000,
                    joinCompiler,
                    node.getRankingFunction());

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Integer> preGroupedChannels = ImmutableList.copyOf(getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitionedInputs()), source.getLayout()));

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrder = ImmutableList.of();

            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), source.getLayout());
                sortOrder = orderingScheme.getOrderingList();
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            ImmutableList.Builder<WindowFunctionDefinition> windowFunctionsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Symbol> windowFunctionOutputSymbolsBuilder = ImmutableList.builder();
            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                Optional<Integer> frameStartChannel = Optional.empty();
                Optional<Integer> frameEndChannel = Optional.empty();

                Frame frame = entry.getValue().getFrame();
                if (frame.getStartValue().isPresent()) {
                    frameStartChannel = Optional.of(source.getLayout().get(frame.getStartValue().get()));
                }
                if (frame.getEndValue().isPresent()) {
                    frameEndChannel = Optional.of(source.getLayout().get(frame.getEndValue().get()));
                }

                FrameInfo frameInfo = new FrameInfo(frame.getType(), frame.getStartType(), frameStartChannel, frame.getEndType(), frameEndChannel);

                Signature signature = entry.getValue().getSignature();
                ImmutableList.Builder<Integer> arguments = ImmutableList.builder();
                for (RowExpression argument : entry.getValue().getArguments()) {
                    checkState(argument instanceof VariableReferenceExpression);
                    Symbol argumentSymbol = new Symbol(((VariableReferenceExpression) argument).getName());
                    arguments.add(source.getLayout().get(argumentSymbol));
                }
                Symbol symbol = entry.getKey();
                WindowFunctionSupplier windowFunctionSupplier = metadata.getWindowFunctionImplementation(signature);
                Type type = metadata.getType(signature.getReturnType());
                windowFunctionsBuilder.add(window(windowFunctionSupplier, type, frameInfo, arguments.build()));
                windowFunctionOutputSymbolsBuilder.add(symbol);
            }

            List<Symbol> windowFunctionOutputSymbols = windowFunctionOutputSymbolsBuilder.build();

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.put(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getTypes().size();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            OperatorFactory operatorFactory = new WindowOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    windowFunctionsBuilder.build(),
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    node.getPreSortedOrderPrefix(),
                    10_000,
                    pagesIndexFactory,
                    isSpillEnabled(session) && isSpillWindowOperator(session),
                    spillerFactory,
                    orderingCompiler);

            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();

            List<Integer> sortChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            for (Symbol symbol : orderBySymbols) {
                sortChannels.add(source.getLayout().get(symbol));
                sortOrders.add(node.getOrderingScheme().getOrdering(symbol));
            }

            OperatorFactory operator = new TopNOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    (int) node.getCount(),
                    sortChannels,
                    sortOrders);

            return new PhysicalOperation(operator, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();

            List<Integer> orderByChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());

            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();
            for (Symbol symbol : orderBySymbols) {
                sortOrder.add(node.getOrderingScheme().getOrdering(symbol));
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            boolean spillEnabled = isSpillEnabled(context.getSession()) && isSpillOrderBy(context.getSession());

            OperatorFactory operator = new OrderByOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    10_000,
                    orderByChannels,
                    sortOrder.build(),
                    pagesIndexFactory,
                    spillEnabled,
                    Optional.of(spillerFactory),
                    orderingCompiler);

            return new PhysicalOperation(operator, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new LimitOperatorFactory(context.getNextOperatorId(), node.getId(), node.getCount());
            return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitDistinctLimit(DistinctLimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            List<Integer> distinctChannels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());

            OperatorFactory operatorFactory = new DistinctLimitOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    distinctChannels,
                    node.getLimit(),
                    hashChannel,
                    joinCompiler);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitGroupId(GroupIdNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            Map<Symbol, Integer> newLayout = new HashMap<>();
            ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();

            int outputChannel = 0;

            for (Symbol output : node.getGroupingSets().stream().flatMap(Collection::stream).collect(Collectors.toSet())) {
                newLayout.put(output, outputChannel++);
                outputTypes.add(source.getTypes().get(source.getLayout().get(node.getGroupingColumns().get(output))));
            }

            Map<Symbol, Integer> argumentMappings = new HashMap<>();
            for (Symbol output : node.getAggregationArguments()) {
                int inputChannel = source.getLayout().get(output);

                newLayout.put(output, outputChannel++);
                outputTypes.add(source.getTypes().get(inputChannel));
                argumentMappings.put(output, inputChannel);
            }

            // for every grouping set, create a mapping of all output to input channels (including arguments)
            ImmutableList.Builder<Map<Integer, Integer>> mappings = ImmutableList.builder();
            for (List<Symbol> groupingSet : node.getGroupingSets()) {
                ImmutableMap.Builder<Integer, Integer> setMapping = ImmutableMap.builder();

                for (Symbol output : groupingSet) {
                    setMapping.put(newLayout.get(output), source.getLayout().get(node.getGroupingColumns().get(output)));
                }

                for (Symbol output : argumentMappings.keySet()) {
                    setMapping.put(newLayout.get(output), argumentMappings.get(output));
                }

                mappings.add(setMapping.build());
            }

            newLayout.put(node.getGroupIdSymbol(), outputChannel);
            outputTypes.add(BIGINT);

            OperatorFactory groupIdOperatorFactory = new GroupIdOperator.GroupIdOperatorFactory(context.getNextOperatorId(),
                    node.getId(),
                    outputTypes.build(),
                    mappings.build());

            return new PhysicalOperation(groupIdOperatorFactory, newLayout, context, source);
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            if (node.getGroupingKeys().isEmpty()) {
                return planGlobalAggregation(node, source, context);
            }

            boolean spillEnabled = isSpillEnabled(context.getSession());
            DataSize unspillMemoryLimit = getAggregationOperatorUnspillMemoryLimit(context.getSession());

            return planGroupByAggregation(node, source, spillEnabled, unspillMemoryLimit, context);
        }

        @Override
        public PhysicalOperation visitCTEScan(CTEScanNode node, LocalExecutionPlanContext context)
        {
            CommonTableExecutionContext cteCtx;
            List<Type> outputTypes;
            PhysicalOperation source = null;

            synchronized (this.getClass()) {
                String cteId = context.getCteId(node.getId());
                int stageId = context.getTaskId().getStageId().getId();
                if (!context.isCteInitialized(cteId) && context.getProducerCTEId().isPresent()
                            && Integer.parseInt(context.getProducerCTEId().get().toString()) == stageId) {
                    source = node.getSource().accept(this, context);
                }

                /* Note: this should always be comming from remote node! */
                checkArgument(context.cteOperationMap.get(node.getId()) == null, "Cte node can be only 1 in a stage");

                cteCtx = context.getRunningTask(cteId, node.getConsumerPlans(), source);
                outputTypes = getSymbolTypes(node.getOutputSymbols(), context.getTypes());
            }

            CommonTableExpressionOperatorFactory cteOperatorFactory = new CommonTableExpressionOperatorFactory(context.getNextOperatorId(),
                    node.getId(),
                    cteCtx,
                    outputTypes,
                    getFilterAndProjectMinOutputPageSize(session),
                    getFilterAndProjectMinOutputPageRowCount(session));

            cteOperatorFactory.addConsumer(context.getConsumerId());

            context.cteOperationMap.put(node.getId(), cteOperatorFactory);
            if (source == null) {
                return new PhysicalOperation(cteOperatorFactory,
                        makeLayout(node),
                        context,
                        UNGROUPED_EXECUTION);
            }

            return new PhysicalOperation(cteOperatorFactory,
                    source.getLayout(),
                    context,
                    source);
        }

        @Override
        public PhysicalOperation visitRouter(RouterNode node, LocalExecutionPlanContext context)
        {
            RouterContext routerCtx;
            List<Type> outputTypes;
            PhysicalOperation source = null;

            Integer part = -1;

            synchronized (this.getClass()) {
                Integer query = node.getConsumerPlans().get(context.getConsumerId());

                /*System.out.println("enumerate paths " + node.getId() + " " + query + " " + context.getConsumerId());
                for (Map.Entry<Integer, Long> entry : node.getBranches().entrySet()) {
                    System.out.println(entry.getKey() + " " + entry.getValue() + " " + query);
                }*/

                String routerId = context.getRouterId(node.getId());
                int stageId = context.getTaskId().getStageId().getId();
                if (!context.isCteInitialized(routerId) && context.getProducerCTEId().isPresent()
                        && Integer.parseInt(context.getProducerCTEId().get().toString()) == stageId) {
                    source = node.getSource().accept(this, context);
                } else {
                    //throw new UnsupportedOperationException("this should not happen");
                }

                checkArgument(context.routerOperationMap.get(node.getId()) == null, "Cte node can be only 1 in a stage");

                routerCtx = context.getRouterRunningTask(routerId, node.getBranches(), node.getOutputSymbols().size()-1, source);
                outputTypes = getSymbolTypes(node.getOutputSymbols(), context.getTypes());

                Long[] partitions = routerCtx.getPartitions();

                //System.out.println("EMPRHATIC");
                //System.out.println("EMPRHATIC");
                //System.out.println("EMPRHATIC");
                for (int i = 0; i < partitions.length; i++) {
                    System.out.println(partitions[i] + " " + query + " " + routerCtx);
                    if (((1L << query) & partitions[i]) != 0) {
                        part = i;
                    }
                }
                //System.out.println("EMPRHATIC");
                //System.out.println("EMPRHATIC");
                //System.out.println("EMPRHATIC");
                //System.out.println("Router " + node.getCommonCTERefNum() + " " + part);

                checkArgument(part != -1);
            }

            //System.out.println(context.producerCTEParentId + " " + context.getConsumerId());
            //System.out.println(context.getConsumerId());

            Router.RouterFactory routerOperatorFactory = new Router.RouterFactory(context.getNextOperatorId(),
                    node.getId(),
                    routerCtx,
                    part,
                    context.producerCTEParentId.isPresent() && context.producerCTEParentId.get().equals(context.getConsumerId()),
                    node.getCommonCTERefNum());

            //routerOperatorFactory.addConsumer(context.getConsumerId());

            context.setDriverInstanceCount(getTaskConcurrency(session)/((int) DriverInstanceCountFudgeFactor));

            context.routerOperationMap.put(node.getId(), routerOperatorFactory);
            if (source == null) {
                return new PhysicalOperation(routerOperatorFactory,
                        makeLayout(node),
                        context,
                        UNGROUPED_EXECUTION);
            }

            return new PhysicalOperation(routerOperatorFactory,
                    makeLayout(node),
                    context,
                    source);
        }

        @Override
        public PhysicalOperation visitCreateIndex(CreateIndexNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new CreateIndexOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getCreateIndexMetadata(),
                    heuristicIndexerManager);

            return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitMarkDistinct(MarkDistinctNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Integer> channels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());
            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            MarkDistinctOperatorFactory operator = new MarkDistinctOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes(), channels, hashChannel, joinCompiler);
            return new PhysicalOperation(operator, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitSample(SampleNode node, LocalExecutionPlanContext context)
        {
            // For system sample, the splits are already filtered out, so no specific action needs to be taken here
            if (node.getSampleType() == SampleNode.Type.SYSTEM) {
                return node.getSource().accept(this, context);
            }

            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode = node.getSource();

            RowExpression filterExpression = node.getPredicate();
            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, Optional.of(filterExpression), AssignmentUtils.identityAsSymbolReferences(outputSymbols), outputSymbols);
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode;
            Optional<RowExpression> filterExpression = Optional.empty();
            if (node.getSource() instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node.getSource();
                sourceNode = filterNode.getSource();
                filterExpression = Optional.of(filterNode.getPredicate());
            }
            else {
                sourceNode = node.getSource();
            }

            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, filterExpression, node.getAssignments(), outputSymbols);
        }

        @Override
        public PhysicalOperation visitStoreForward(StoreForwardNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            StoreForwardOperator.StoreForwardFactory operatorFactory = new StoreForwardOperator.StoreForwardFactory(context.getNextOperatorId(), node.getId(), node.getFileName());

            return new PhysicalOperation(operatorFactory,
                    makeLayout(node),
                    context,
                    source);
        }

        private PhysicalOperation visitScanFilterAndProject(
                LocalExecutionPlanContext context,
                PlanNodeId planNodeId,
                PlanNode sourceNode,
                Optional<RowExpression> filterExpression,
                Assignments assignments,
                List<Symbol> outputSymbols)
        {
            if (outputSymbols.size() > 0 && outputSymbols.get(outputSymbols.size()-1).getName().startsWith("query_set")) {
                return visitSharedScanFilterAndProject(context, planNodeId, sourceNode, filterExpression, assignments, outputSymbols, ((TableScanNode) sourceNode).getQuerySet());
            }


            // if source is a table scan we fold it directly into the filter and project
            // otherwise we plan it as a normal operator
            Map<Symbol, Integer> sourceLayout;
            TableHandle table = null;
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            ReuseExchangeOperator.STRATEGY strategy = REUSE_STRATEGY_DEFAULT;
            UUID reuseTableScanMappingId = new UUID(0, 0);
            Integer consumerTableScanNodeCount = 0;
            if (sourceNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) sourceNode;
                table = tableScanNode.getTable();

                // extract the column handles and channel to type mapping
                sourceLayout = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    columns.add(tableScanNode.getAssignments().get(symbol));

                    Integer input = channel;
                    sourceLayout.put(symbol, input);

                    channel++;
                }

                strategy = tableScanNode.getStrategy();
                reuseTableScanMappingId = tableScanNode.getReuseTableScanMappingId();
                consumerTableScanNodeCount = tableScanNode.getConsumerTableScanNodeCount();
            }
            //TODO: This is a simple hack, it will be replaced when we add ability to push down sampling into connectors.
            // SYSTEM sampling is performed in the coordinator by dropping some random splits so the SamplingNode can be skipped here.
            else if (sourceNode instanceof SampleNode) {
                SampleNode sampleNode = (SampleNode) sourceNode;
                checkArgument(sampleNode.getSampleType() == SampleNode.Type.SYSTEM, "%s sampling is not supported", sampleNode.getSampleType());
                return visitScanFilterAndProject(context,
                        planNodeId,
                        sampleNode.getSource(),
                        filterExpression,
                        assignments,
                        outputSymbols);
            }
//            else if (sourceNode instanceof CTEScanNode) {
//                CTEScanNode cteScanNode = (CTEScanNode) sourceNode;
//                return createCTEScanFactory(cteScanNode, context, outputSymbols);
//            }
            else {
                // plan source
                source = sourceNode.accept(this, context);
                sourceLayout = source.getLayout();
            }

            // filterExpression may contain large function calls; evaluate them before compiling.
            if (filterExpression.isPresent()) {
                filterExpression = Optional.of(bindChannels(filterExpression.get(), sourceLayout, context.getTypes()));
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, i);
            }
            Map<Symbol, Integer> outputMappings = outputMappingsBuilder.build();

            Optional<DynamicFilters.ExtractResult> extractDynamicFilterResult = filterExpression.map(DynamicFilters::extractDynamicFilters);
            Optional<RowExpression> translatedFilter = extractDynamicFilterResult
                    .map(DynamicFilters.ExtractResult::getStaticConjuncts)
                    .map(RowExpressionUtils::combineConjuncts);

            // TODO: Execution must be plugged in here
            Supplier<Map<ColumnHandle, DynamicFilter>> dynamicFilterSupplier = getDynamicFilterSupplier(extractDynamicFilterResult, sourceNode, context);
            Optional<DynamicFilterSupplier> dynamicFilter = Optional.empty();
            if (dynamicFilterSupplier != null) {
                dynamicFilter = Optional.of(new DynamicFilterSupplier(dynamicFilterSupplier, System.currentTimeMillis(), getDynamicFilteringWaitTime(session).toMillis()));
            }

            List<RowExpression> projections = new ArrayList<>();
            for (Symbol symbol : outputSymbols) {
                projections.add(assignments.get(symbol));
            }

            List<RowExpression> translatedProjections = projections.stream()
                    .map(expression -> {
                        if (isExpression(expression)) {
                            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression));
                            return toRowExpression(castToExpression(expression), expressionTypes, sourceLayout);
                        }
                        else {
                            return bindChannels(expression, sourceLayout, context.getTypes());
                        }
                    })
                    .collect(toImmutableList());

            try {
                if (columns != null) {
                    Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(translatedFilter, translatedProjections, sourceNode.getId());
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    boolean spillEnabled = isSpillEnabled(session) && isSpillReuseExchange(session);
                    int spillerThreshold = getSpillOperatorThresholdReuseExchange(session) * 1024 * 1024; //convert from MB to bytes

                    SourceOperatorFactory operatorFactory = new ScanFilterAndProjectOperatorFactory(
                            context.getSession(),
                            context.getNextOperatorId(),
                            planNodeId,
                            sourceNode,
                            pageSourceProvider,
                            cursorProcessor,
                            pageProcessor,
                            table,
                            columns,
                            dynamicFilter,
                            projections.stream().map(expression -> {
                                if (isExpression(expression)) {
                                    return typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression)).get(NodeRef.of(castToExpression(expression)));
                                }
                                else {
                                    return expression.getType();
                                }
                            }).collect(toImmutableList()),
                            stateStoreProvider,
                            metadata,
                            dynamicFilterCacheManager,
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session),
                            strategy, reuseTableScanMappingId, spillEnabled, Optional.of(spillerFactory), spillerThreshold, consumerTableScanNodeCount);

                    return new PhysicalOperation(operatorFactory, outputMappings, context, stageExecutionDescriptor.isScanGroupedExecution(sourceNode.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
                }
                else {
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            pageProcessor,
                            projections.stream().map(expression -> {
                                if (isExpression(expression)) {
                                    return typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression)).get(NodeRef.of(castToExpression(expression)));
                                }
                                else {
                                    return expression.getType();
                                }
                            }).collect(toImmutableList()),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));

                    return new PhysicalOperation(operatorFactory, outputMappings, context, source);
                }
            }
            catch (RuntimeException e) {
                throw new PrestoException(COMPILER_ERROR, "Local Execution Planner: visitScanFilterAndProject", e);
            }
        }




        private PhysicalOperation  visitSharedScanFilterAndProject(
                LocalExecutionPlanContext context,
                PlanNodeId planNodeId,
                PlanNode sourceNode,
                Optional<RowExpression> filterExpression,
                Assignments assignments,
                List<Symbol> outputSymbols,
                Long querySet)
        {
            TableScanNode tableNode = (TableScanNode) sourceNode;
            List<ColumnHandle> tableColumns = new ArrayList<>();
            for (Symbol symbol : tableNode.getOutputSymbols()) {
                if (tableNode.getAssignments().containsKey(symbol)) {
                    tableColumns.add(tableNode.getAssignments().get(symbol));
                }
            }
            //System.out.println("VSK: VisitSeparateScanFilterProject "+"plan node "+sourceNode.getId()+" table output symbols: "+tableNode.getOutputSymbols()+" function args output symbols :"+outputSymbols+" node.getoutputSymbols: "+sourceNode.getOutputSymbols());
            //System.out.println("test 1");
            Assignments tableAssignments = identityAssignments(context.getTypes(), tableNode.getOutputSymbols());
            List<Type> types = tableAssignments.getExpressions().stream()
                    .map(expression -> expression.getType())
                    .collect(Collectors.toList());


            boolean spillEnabled = isSpillEnabled(session) && isSpillReuseExchange(session);
            int spillerThreshold = getSpillOperatorThresholdReuseExchange(session) * 1024 * 1024; //convert from MB to bytes
            Integer consumerTableScanNodeCount = tableNode.getConsumerTableScanNodeCount();

            OperatorFactory operatorFactory = new TableScanOperatorFactory(context.getSession(),
                    context.getNextOperatorId(),
                    tableNode,
                    pageSourceProvider,
                    tableNode.getTable(),
                    tableColumns,
                    types.subList(0, types.size()-1),
                    stateStoreProvider,
                    metadata,
                    dynamicFilterCacheManager,
                    getFilterAndProjectMinOutputPageSize(session),
                    getFilterAndProjectMinOutputPageRowCount(session), tableNode.getStrategy(), tableNode.getReuseTableScanMappingId(), spillEnabled, Optional.of(spillerFactory),
                    spillerThreshold, consumerTableScanNodeCount);

            Map<Symbol, Integer> sourceLayout;
            TableHandle table = null;
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            ReuseExchangeOperator.STRATEGY strategy = REUSE_STRATEGY_DEFAULT;
            UUID reuseTableScanMappingId = new UUID(0, 0);
            consumerTableScanNodeCount = 0;

            if (sourceNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) sourceNode;
                table = tableScanNode.getTable();

                // extract the column handles and channel to type mapping
                sourceLayout = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    if (tableNode.getAssignments().containsKey(symbol)) {
                        columns.add(tableScanNode.getAssignments().get(symbol));

                        Integer input = channel;
                        sourceLayout.put(symbol, input);

                        channel++;
                    }
                }

                strategy = tableScanNode.getStrategy();
                reuseTableScanMappingId = tableScanNode.getReuseTableScanMappingId();
                consumerTableScanNodeCount = tableScanNode.getConsumerTableScanNodeCount();
            }
            //TODO: This is a simple hack, it will be replaced when we add ability to push down sampling into connectors.
            // SYSTEM sampling is performed in the coordinator by dropping some random splits so the SamplingNode can be skipped here.
            else if (sourceNode instanceof SampleNode) {
                SampleNode sampleNode = (SampleNode) sourceNode;
                checkArgument(sampleNode.getSampleType() == SampleNode.Type.SYSTEM, "%s sampling is not supported", sampleNode.getSampleType());
                return visitScanFilterAndProject(context,
                        planNodeId,
                        sampleNode.getSource(),
                        filterExpression,
                        assignments,
                        outputSymbols);
            }
//            else if (sourceNode instanceof CTEScanNode) {
//                CTEScanNode cteScanNode = (CTEScanNode) sourceNode;
//                return createCTEScanFactory(cteScanNode, context, outputSymbols);
//            }
            else {
                // plan source
                source = sourceNode.accept(this, context);
                sourceLayout = source.getLayout();
            }

            // filterExpression may contain large function calls; evaluate them before compiling.
            if (filterExpression.isPresent()) {
                filterExpression = Optional.of(bindChannels(filterExpression.get(), sourceLayout, context.getTypes()));
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, i);
            }
            Map<Symbol, Integer> outputMappings = outputMappingsBuilder.build();

            Optional<DynamicFilters.ExtractResult> extractDynamicFilterResult = filterExpression.map(DynamicFilters::extractDynamicFilters);
            Optional<RowExpression> translatedFilter = extractDynamicFilterResult
                    .map(DynamicFilters.ExtractResult::getStaticConjuncts)
                    .map(RowExpressionUtils::combineConjuncts);

            // TODO: Execution must be plugged in here
            Supplier<Map<ColumnHandle, DynamicFilter>> dynamicFilterSupplier = getDynamicFilterSupplier(extractDynamicFilterResult, sourceNode, context);
            Optional<DynamicFilterSupplier> dynamicFilter = Optional.empty();
            if (dynamicFilterSupplier != null) {
                dynamicFilter = Optional.of(new DynamicFilterSupplier(dynamicFilterSupplier, System.currentTimeMillis(), getDynamicFilteringWaitTime(session).toMillis()));
            }

            List<RowExpression> projections = new ArrayList<>();
            for (Symbol symbol : outputSymbols) {
                projections.add(assignments.get(symbol));
            }

            List<RowExpression> translatedProjections = projections.subList(0, projections.size()-1).stream()
                    .map(expression -> {
                        if (isExpression(expression)) {
                            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression));
                            return toRowExpression(castToExpression(expression), expressionTypes, sourceLayout);
                        }
                        else {
                            return bindChannels(expression, sourceLayout, context.getTypes());
                        }
                    })
                    .collect(toImmutableList());

            for (RowExpression re : translatedProjections) {
                //System.out.println(re.getClass().toString());
            }

            List<RowExpression> fullProjections = new ArrayList<>(translatedProjections);
            fullProjections.add(new ConstantExpression(0L, BIGINT));

            List<RowExpression> identityProjections = new ArrayList<>();
            for (int i = 0; i < outputSymbols.size(); i++) {
                identityProjections.add(new InputReferenceExpression(i, context.getTypes().get(outputSymbols.get(i))));
            }

            try {
                /*if (columns != null) {
                    Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(translatedFilter, translatedProjections, sourceNode.getId());
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    //boolean spillEnabled = isSpillEnabled(session) && isSpillReuseExchange(session);
                    //int spillerThreshold = getSpillOperatorThresholdReuseExchange(session) * 1024 * 1024; //convert from MB to bytes

                    operatorFactory = new ScanFilterAndProjectOperatorFactory(
                            context.getSession(),
                            context.getNextOperatorId(),
                            planNodeId,
                            sourceNode,
                            pageSourceProvider,
                            cursorProcessor,
                            pageProcessor,
                            table,
                            columns,
                            dynamicFilter,
                            projections.stream().map(expression -> {
                                if (isExpression(expression)) {
                                    return typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression)).get(NodeRef.of(castToExpression(expression)));
                                } else {
                                    return expression.getType();
                                }
                            }).collect(toImmutableList()),
                            stateStoreProvider,
                            metadata,
                            dynamicFilterCacheManager,
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session),
                            strategy, reuseTableScanMappingId, spillEnabled, Optional.of(spillerFactory), spillerThreshold, consumerTableScanNodeCount);

                    throw new UnsupportedOperationException("columns != null");
                    //return new PhysicalOperation(operatorFactory, outputMappings, context, stageExecutionDescriptor.isScanGroupedExecution(sourceNode.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
                } else {
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            pageProcessor,
                            projections.stream().map(expression -> {
                                if (isExpression(expression)) {
                                    return typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression)).get(NodeRef.of(castToExpression(expression)));
                                } else {
                                    return expression.getType();
                                }
                            }).collect(toImmutableList()),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));

                    return new PhysicalOperation(operatorFactory, outputMappings, context, source);
                }
                else {*/

                //System.out.println("HERE " + querySet);

                Map<Symbol, List<OperatorType>> Qtypes = new HashMap<>();
                Map<Symbol, List<Long>> args = new HashMap<>();
                Map<Symbol, List<Long>> queryIds = new HashMap<>();
                //Map<Symbol, Integer> filterChannel = new HashMap();
                //int filterChannel = 0;
                String tableName = tableNode.getTable().getConnectorHandle().getTableName().toString();


                    Map<Integer, String> FilterInfo = tableNode.getPredicateCollector();
                    for (Integer key : FilterInfo.keySet()) {
                        String str = FilterInfo.get(key);
                        //System.out.println("key " + key + " filter info " + str);

                        List<String> strQueue = new ArrayList<>();
                        strQueue.add(str);

                        while (strQueue.size() > 0) {
                            String nextStr = strQueue.remove(0);

                            if (nextStr.startsWith("AND")) {
                                String inner = nextStr.substring(4, nextStr.length()-1);
                                //System.out.println(inner);

                                int counter = 0;
                                int middle = -1;

                                for (int i = 0; i < inner.length(); i++) {
                                    if (inner.charAt(i) == '(') {
                                        counter++;
                                    } else if (inner.charAt(i) == ')') {
                                        counter--;
                                    } else if (inner.charAt(i) == ',') {
                                        if (counter == 0) {
                                            middle = i;
                                        }
                                    }
                                }

                                assert(middle != -1);

                                String first = inner.substring(0, middle);
                                String second = inner.substring(middle+2, inner.length());

                                //System.out.println(first + " " + second);
                                strQueue.add(first);
                                strQueue.add(second);
                            } else {
                                List<Symbol> filterChannelSymbol = getFilterChannelSymbol(outputSymbols, nextStr);
                                assert(filterChannelSymbol.size() == 1);

                                Symbol symbol = filterChannelSymbol.get(0);

                                if (!Qtypes.containsKey(symbol)) Qtypes.put(symbol, new ArrayList<>());
                                if (!args.containsKey(symbol)) args.put(symbol, new ArrayList<>());
                                if (!queryIds.containsKey(symbol)) queryIds.put(symbol, new ArrayList<>());

                                queryIds.get(symbol).add(new Long(key));

                                if (nextStr.contains("greater_than_or_equal")){
                                    Qtypes.get(symbol).add(GREATER_THAN_OR_EQUAL);
                                }
                                else if (nextStr.contains("greater_than")){
                                    Qtypes.get(symbol).add(GREATER_THAN);
                                }
                                else if (nextStr.contains("less_than_or_equal")){

                                    Qtypes.get(symbol).add(LESS_THAN_OR_EQUAL);
                                }
                                else if (nextStr.contains("less_than")){
                                    Qtypes.get(symbol).add(LESS_THAN);
                                }
                                else if (nextStr.contains("equal")){
                                    Qtypes.get(symbol).add(EQUAL);
                                }
                                else
                                    throw new UnsupportedOperationException("unsupported operation (srinivas) " + tableName);

                                Long argValue = extractArgVal(nextStr);
                                args.get(symbol).add(argValue);

                                //filterChannel.put(symbol, getChannelsForSymbols(filterChannelSymbol, sourceLayout).get(0));
                            }
                        }
                    }
                    //System.out.println("Types :"+ Qtypes);
                    //System.out.println("args :"+ args);
                    //System.out.println("query ids "+queryIds);

                //System.out.println(tableName + " " + querySet);

                PhysicalOperation top = new PhysicalOperation(operatorFactory, outputMappings, context, Optional.empty(), stageExecutionDescriptor.isScanGroupedExecution(sourceNode.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);

                List<OperatorFactory> opFac = new ArrayList<>();
                opFac.add(operatorFactory);

                boolean first = true;

                for (Symbol symbol : args.keySet()) {
                    Integer filterChannel = -1;

                    if (first) {
                        for (int i = 0; i < tableNode.getOutputSymbols().size(); i++) {
                            if (symbol.getName().compareTo(tableNode.getOutputSymbols().get(i).getName()) == 0) {
                                //System.out.println(tableNode.getOutputSymbols().get(i).getName());
                                filterChannel = i;
                                break;
                            }
                        }
                    } else {
                        for (int i = 0; i < outputSymbols.size(); i++) {
                            if (symbol.getName().compareTo(outputSymbols.get(i).getName()) == 0) {
                                //System.out.println(tableNode.getOutputSymbols().get(i).getName());
                                filterChannel = i;
                                break;
                            }
                        }
                    }

                    if (filterChannel < 0) {
                        throw new IllegalArgumentException("Filter channel not found");
                    }

                    //System.out.println(symbol.toString() + " " + filterChannel + " " + querySet + " " + ((first) ? -1 : (outputSymbols.size()-1)));

                    Supplier<SharedFilterProcessor> pageProcessor = expressionCompiler.compilePageSharedProcessor(Qtypes.get(symbol), args.get(symbol), queryIds.get(symbol), (first) ? translatedProjections : identityProjections, Optional.of(context.getStageId() + "_" + planNodeId), querySet, filterChannel, (first) ? -1 : (outputSymbols.size()-1));

                    List<Type> temp = new ArrayList<>(projections.stream().map(expression -> {
                        if (isExpression(expression)) {
                            return typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression)).get(NodeRef.of(castToExpression(expression)));
                        } else {
                            return expression.getType();
                        }
                    }).collect(toImmutableList()));
                    //temp.add(BIGINT);

                    assert(temp.size() == outputSymbols.size());

                    OperatorFactory operatorSharedFilterFactory = new SharedFilterAndProjectOperator.SharedFilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            new PlanNodeId(planNodeId + "_" + opFac.size()),
                            pageProcessor,
                            temp,
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));


                    opFac.add(operatorSharedFilterFactory);
                    top = new PhysicalOperation(operatorSharedFilterFactory, outputMappings, context, top);
                    first = false;
                }

                if (opFac.size() == 1) {
                    Supplier<SharedFilterProcessor> pageProcessor = expressionCompiler.compilePageSharedProcessor(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId + "_" + opFac.size()), querySet, -1, -1);

                    List<Type> temp = new ArrayList<>(projections.stream().map(expression -> {
                        if (isExpression(expression)) {
                            return typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression)).get(NodeRef.of(castToExpression(expression)));
                        } else {
                            return expression.getType();
                        }
                    }).collect(toImmutableList()));
                    //temp.add(BIGINT);

                    assert(temp.size() == outputSymbols.size());

                    OperatorFactory operatorSharedFilterFactory = new SharedFilterAndProjectOperator.SharedFilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            new PlanNodeId(planNodeId + "_" + opFac.size()),
                            pageProcessor,
                            temp,
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));


                    opFac.add(operatorSharedFilterFactory);
                    top = new PhysicalOperation(operatorSharedFilterFactory, outputMappings, context, top);
                }

                return top;
                //return new PhysicalOperation(opFac, outputMappings, context, Optional.empty(), stageExecutionDescriptor.isScanGroupedExecution(sourceNode.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
                //}
            }
            catch (RuntimeException e) {
                e.printStackTrace();
                throw new PrestoException(COMPILER_ERROR, "Compiler failed for sharing", e);
            }
        }



        private List<Symbol> getFilterChannelSymbol(List<Symbol> outputSymbols, String str) {
            List<Symbol> channelList = new ArrayList<>();
            for(Symbol sym : outputSymbols){
                if (str.contains(sym.getName())){
                    channelList.add(sym);
                    return channelList;
                }
            }
            throw new UnsupportedOperationException("Should not come here wrt filter info. addition");
            //return null;
        }

        private Long extractArgVal (String str){

            int firstOpenbracketPosition = -1;
            String withinParnathesis = null;
            for (int i = 0; i < str.length(); i++) {
                char x = str.charAt(i);

                if (x == '(') {
                    firstOpenbracketPosition = i;
                } else if (x == ')') {
                    withinParnathesis = new String(str.substring(firstOpenbracketPosition + 1, i));
                }
            }

            //System.out.println("VSK: extractArgVal "+withinParnathesis);

            if (withinParnathesis == null)
                throw new UnsupportedOperationException("extractArgVal (Srinivas) ");

            if(withinParnathesis.contains(",")){
                String valString =  withinParnathesis.split(",")[1].trim();
                Long val = Long.valueOf(valString);
                //System.out.println("value is "+val);
                return val;
            }
            else{
                throw new UnsupportedOperationException("extractArgVal (Srinivas) ");
            }
        }


        // TODO: This should be refactored, so that there's an optimizer that merges scan-filter-project into a single PlanNode
        private PhysicalOperation visitScanFilterAndProject_original(
                LocalExecutionPlanContext context,
                PlanNodeId planNodeId,
                PlanNode sourceNode,
                Optional<RowExpression> filterExpression,
                Assignments assignments,
                List<Symbol> outputSymbols)
        {
            // if source is a table scan we fold it directly into the filter and project
            // otherwise we plan it as a normal operator
            Map<Symbol, Integer> sourceLayout;
            TableHandle table = null;
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            ReuseExchangeOperator.STRATEGY strategy = REUSE_STRATEGY_DEFAULT;
            UUID reuseTableScanMappingId = new UUID(0, 0);
            Integer consumerTableScanNodeCount = 0;
            if (sourceNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) sourceNode;
                table = tableScanNode.getTable();

                // extract the column handles and channel to type mapping
                sourceLayout = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    columns.add(tableScanNode.getAssignments().get(symbol));

                    Integer input = channel;
                    sourceLayout.put(symbol, input);

                    channel++;
                }

                strategy = tableScanNode.getStrategy();
                reuseTableScanMappingId = tableScanNode.getReuseTableScanMappingId();
                consumerTableScanNodeCount = tableScanNode.getConsumerTableScanNodeCount();
            }
            //TODO: This is a simple hack, it will be replaced when we add ability to push down sampling into connectors.
            // SYSTEM sampling is performed in the coordinator by dropping some random splits so the SamplingNode can be skipped here.
            else if (sourceNode instanceof SampleNode) {
                SampleNode sampleNode = (SampleNode) sourceNode;
                checkArgument(sampleNode.getSampleType() == SampleNode.Type.SYSTEM, "%s sampling is not supported", sampleNode.getSampleType());
                return visitScanFilterAndProject(context,
                        planNodeId,
                        sampleNode.getSource(),
                        filterExpression,
                        assignments,
                        outputSymbols);
            }
//            else if (sourceNode instanceof CTEScanNode) {
//                CTEScanNode cteScanNode = (CTEScanNode) sourceNode;
//                return createCTEScanFactory(cteScanNode, context, outputSymbols);
//            }
            else {
                // plan source
                source = sourceNode.accept(this, context);
                sourceLayout = source.getLayout();
            }

            // filterExpression may contain large function calls; evaluate them before compiling.
            if (filterExpression.isPresent()) {
                filterExpression = Optional.of(bindChannels(filterExpression.get(), sourceLayout, context.getTypes()));
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, i);
            }
            Map<Symbol, Integer> outputMappings = outputMappingsBuilder.build();

            Optional<DynamicFilters.ExtractResult> extractDynamicFilterResult = filterExpression.map(DynamicFilters::extractDynamicFilters);
            Optional<RowExpression> translatedFilter = extractDynamicFilterResult
                    .map(DynamicFilters.ExtractResult::getStaticConjuncts)
                    .map(RowExpressionUtils::combineConjuncts);

            // TODO: Execution must be plugged in here
            Supplier<Map<ColumnHandle, DynamicFilter>> dynamicFilterSupplier = getDynamicFilterSupplier(extractDynamicFilterResult, sourceNode, context);
            Optional<DynamicFilterSupplier> dynamicFilter = Optional.empty();
            if (dynamicFilterSupplier != null) {
                dynamicFilter = Optional.of(new DynamicFilterSupplier(dynamicFilterSupplier, System.currentTimeMillis(), getDynamicFilteringWaitTime(session).toMillis()));
            }

            List<RowExpression> projections = new ArrayList<>();
            for (Symbol symbol : outputSymbols) {
                projections.add(assignments.get(symbol));
            }

            List<RowExpression> translatedProjections = projections.stream()
                    .map(expression -> {
                        if (isExpression(expression)) {
                            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression));
                            return toRowExpression(castToExpression(expression), expressionTypes, sourceLayout);
                        }
                        else {
                            return bindChannels(expression, sourceLayout, context.getTypes());
                        }
                    })
                    .collect(toImmutableList());

            try {
                if (columns != null) {
                    Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(translatedFilter, translatedProjections, sourceNode.getId());
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    boolean spillEnabled = isSpillEnabled(session) && isSpillReuseExchange(session);
                    int spillerThreshold = getSpillOperatorThresholdReuseExchange(session) * 1024 * 1024; //convert from MB to bytes

                    SourceOperatorFactory operatorFactory = new ScanFilterAndProjectOperatorFactory(
                            context.getSession(),
                            context.getNextOperatorId(),
                            planNodeId,
                            sourceNode,
                            pageSourceProvider,
                            cursorProcessor,
                            pageProcessor,
                            table,
                            columns,
                            dynamicFilter,
                            projections.stream().map(expression -> {
                                if (isExpression(expression)) {
                                    return typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression)).get(NodeRef.of(castToExpression(expression)));
                                }
                                else {
                                    return expression.getType();
                                }
                            }).collect(toImmutableList()),
                            stateStoreProvider,
                            metadata,
                            dynamicFilterCacheManager,
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session),
                            strategy, reuseTableScanMappingId, spillEnabled, Optional.of(spillerFactory), spillerThreshold, consumerTableScanNodeCount);

                    return new PhysicalOperation(operatorFactory, outputMappings, context, stageExecutionDescriptor.isScanGroupedExecution(sourceNode.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
                }
                else {
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            pageProcessor,
                            projections.stream().map(expression -> {
                                if (isExpression(expression)) {
                                    return typeAnalyzer.getTypes(context.getSession(), context.getTypes(), castToExpression(expression)).get(NodeRef.of(castToExpression(expression)));
                                }
                                else {
                                    return expression.getType();
                                }
                            }).collect(toImmutableList()),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));

                    return new PhysicalOperation(operatorFactory, outputMappings, context, source);
                }
            }
            catch (RuntimeException e) {
                throw new PrestoException(COMPILER_ERROR, "Compiler failed", e);
            }
        }

        private Supplier<Map<ColumnHandle, DynamicFilter>> getDynamicFilterSupplier(Optional<DynamicFilters.ExtractResult> extractDynamicFilterResult, PlanNode sourceNode, LocalExecutionPlanContext context)
        {
            Optional<List<DynamicFilters.Descriptor>> dynamicFilters = extractDynamicFilterResult.map(DynamicFilters.ExtractResult::getDynamicConjuncts);
            if (dynamicFilters.isPresent() && !dynamicFilters.get().isEmpty()) {
                log.debug("[TableScan] Dynamic filters: %s", dynamicFilters);
                if (sourceNode instanceof TableScanNode) {
                    TableScanNode tableScanNode = (TableScanNode) sourceNode;
                    LocalDynamicFiltersCollector collector = context.getDynamicFiltersCollector();
                    collector.initContext(dynamicFilters.get(), SymbolUtils.toLayOut(tableScanNode.getOutputSymbols()));
                    dynamicFilters.get().forEach(dynamicFilter -> dynamicFilterCacheManager.registerTask(createCacheKey(dynamicFilter.getId(), session.getQueryId().getId()), context.getTaskId()));
                    return () -> collector.getDynamicFilters(tableScanNode);
                }
            }
            else if (isCrossRegionDynamicFilterEnabled(context.getSession())) {
                if (sourceNode instanceof TableScanNode) {
                    LocalDynamicFiltersCollector collector = context.getDynamicFiltersCollector();
                    if (collector.checkTableIsDcTable((TableScanNode) sourceNode)) {
                        // if cross-region-dynamic-filter is enabled and the table is a dc table, should consider push down the cross region bloom filter to next cluster
                        return BloomFilterUtils.getCrossRegionDynamicFilterSupplier(dynamicFilterCacheManager, context.getSession().getQueryId().getId(), (TableScanNode) sourceNode);
                    }
                }
            }
            return null;
        }

        private RowExpression toRowExpression(Expression expression, Map<NodeRef<Expression>, Type> types, Map<Symbol, Integer> layout)
        {
            return SqlToRowExpressionTranslator.translate(expression, SCALAR, types, layout, metadata, session, true);
        }

        private RowExpression bindChannels(RowExpression expression, Map<Symbol, Integer> sourceLayout, TypeProvider types)
        {
            Type type = expression.getType();
            Object value = new RowExpressionInterpreter(expression, metadata, session.toConnectorSession(), OPTIMIZED).optimize();
            if (value instanceof RowExpression) {
                RowExpression optimized = (RowExpression) value;
                // building channel info
                Map<VariableReferenceExpression, Integer> layout = new LinkedHashMap<>();
                sourceLayout.forEach((symbol, num) -> layout.put(toVariableReference(symbol, types.get(symbol)), num));
                expression = VariableToChannelTranslator.translate(optimized, layout);
            }
            else {
                expression = constant(value, type);
            }
            return expression;
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode = node;
            Optional<RowExpression> filterExpression = Optional.empty();

            List<Symbol> outputSymbols = node.getOutputSymbols();
            if (outputSymbols.get(0).getName().contains("schema_name")){
                return visitTableScan_old(node, context);
            }
            else{
                return visitScanFilterAndProject(context, node.getId(), sourceNode, filterExpression, identityAssignments(context.getTypes(), node.getOutputSymbols()), outputSymbols);
            }
        }

        //@Override
        public PhysicalOperation visitTableScan_old(TableScanNode node, LocalExecutionPlanContext context)
        {
            List<ColumnHandle> columns = new ArrayList<>();
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));
            }

            Assignments assignments = identityAssignments(context.getTypes(), node.getOutputSymbols());
            List<Type> types = assignments.getExpressions().stream()
                    .map(expression -> expression.getType())
                    .collect(Collectors.toList());

            boolean spillEnabled = isSpillEnabled(session) && isSpillReuseExchange(session);
            int spillerThreshold = getSpillOperatorThresholdReuseExchange(session) * 1024 * 1024; //convert from MB to bytes
            Integer consumerTableScanNodeCount = node.getConsumerTableScanNodeCount();

            OperatorFactory operatorFactory = new TableScanOperatorFactory(context.getSession(),
                    context.getNextOperatorId(),
                    node,
                    pageSourceProvider,
                    node.getTable(),
                    columns,
                    types,
                    stateStoreProvider,
                    metadata,
                    dynamicFilterCacheManager,
                    getFilterAndProjectMinOutputPageSize(session),
                    getFilterAndProjectMinOutputPageRowCount(session), node.getStrategy(), node.getReuseTableScanMappingId(), spillEnabled, Optional.of(spillerFactory), spillerThreshold, consumerTableScanNodeCount);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, stageExecutionDescriptor.isScanGroupedExecution(node.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitValues(ValuesNode node, LocalExecutionPlanContext context)
        {
            // a values node must have a single driver
            context.setDriverInstanceCount(1);

            if (node.getRows().isEmpty()) {
                OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), ImmutableList.of());
                return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
            }

            List<Type> outputTypes = getSymbolTypes(node.getOutputSymbols(), context.getTypes());
            PageBuilder pageBuilder = new PageBuilder(node.getRows().size(), outputTypes);
            for (List<RowExpression> row : node.getRows()) {
                pageBuilder.declarePosition();
                for (int i = 0; i < row.size(); i++) {
                    // evaluate the literal value
                    Object result = RowExpressionInterpreter.rowExpressionInterpreter(row.get(i), metadata, context.getSession().toConnectorSession()).evaluate();
                    writeNativeValue(outputTypes.get(i), pageBuilder.getBlockBuilder(i), result);
                }
            }

            OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), ImmutableList.of(pageBuilder.build()));
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitUnnest(UnnestNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableList.Builder<Type> replicateTypes = ImmutableList.builder();
            for (Symbol symbol : node.getReplicateSymbols()) {
                replicateTypes.add(context.getTypes().get(symbol));
            }
            List<Symbol> unnestSymbols = ImmutableList.copyOf(node.getUnnestSymbols().keySet());
            ImmutableList.Builder<Type> unnestTypes = ImmutableList.builder();
            for (Symbol symbol : unnestSymbols) {
                unnestTypes.add(context.getTypes().get(symbol));
            }
            Optional<Symbol> ordinalitySymbol = node.getOrdinalitySymbol();
            Optional<Type> ordinalityType = ordinalitySymbol.map(context.getTypes()::get);
            ordinalityType.ifPresent(type -> checkState(type.equals(BIGINT), "Type of ordinalitySymbol must always be BIGINT."));

            List<Integer> replicateChannels = getChannelsForSymbols(node.getReplicateSymbols(), source.getLayout());
            List<Integer> unnestChannels = getChannelsForSymbols(unnestSymbols, source.getLayout());

            // Source channels are always laid out first, followed by the unnested symbols
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getReplicateSymbols()) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            for (Symbol symbol : unnestSymbols) {
                for (Symbol unnestedSymbol : node.getUnnestSymbols().get(symbol)) {
                    outputMappings.put(unnestedSymbol, channel);
                    channel++;
                }
            }
            if (ordinalitySymbol.isPresent()) {
                outputMappings.put(ordinalitySymbol.get(), channel);
                channel++;
            }
            OperatorFactory operatorFactory = new UnnestOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    replicateChannels,
                    replicateTypes.build(),
                    unnestChannels,
                    unnestTypes.build(),
                    ordinalityType.isPresent());
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        @Override
        public PhysicalOperation visitVacuumTable(VacuumTableNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory dummyOperatorFactory = new DevNullOperatorFactory(context.getNextOperatorId(), node.getId());
            PhysicalOperation source = new PhysicalOperation(dummyOperatorFactory, makeLayoutFromOutputSymbols(node.getInputSymbols()),
                    context, stageExecutionDescriptor.isScanGroupedExecution(node.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            outputMapping.put(node.getOutputSymbols().get(0), ROW_COUNT_CHANNEL);
            outputMapping.put(node.getOutputSymbols().get(1), FRAGMENT_CHANNEL);

            OperatorFactory statisticsOperatorFactory = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            PARTIAL,
                            STATS_START_CHANNEL,
                            outputMapping,
                            source,
                            context,
                            true);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        PARTIAL,
                        Optional.empty(),
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        new DataSize(0, BYTE),
                        context,
                        STATS_START_CHANNEL,
                        outputMapping,
                        200,
                        // This aggregation must behave as INTERMEDIATE.
                        // Using INTERMEDIATE aggregation directly
                        // is not possible, as it doesn't accept raw input data.
                        // Disabling partial pre-aggregation memory limit effectively
                        // turns PARTIAL aggregation into INTERMEDIATE.
                        Optional.empty(),
                        true);
            }).orElse(new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));
            OperatorFactory operatorFactory = new VacuumTableOperator.VacuumTableOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSourceProvider,
                    pageSinkManager,
                    node.getTarget(),
                    node.getTable(),
                    session,
                    statisticsOperatorFactory,
                    getSymbolTypes(node.getOutputSymbols(), context.getTypes()),
                    Optional.of(context.getTaskId()));
            return new PhysicalOperation(operatorFactory,
                    makeLayout(node),
                    context,
                    stageExecutionDescriptor.isScanGroupedExecution(node.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
        }

        private ImmutableMap<Symbol, Integer> makeLayout(PlanNode node)
        {
            return makeLayoutFromOutputSymbols(node.getOutputSymbols());
        }

        private ImmutableMap<Symbol, Integer> makeLayoutFromOutputSymbols(List<Symbol> outputSymbols)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : outputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            return outputMappings.build();
        }

        @Override
        public PhysicalOperation visitIndexSource(IndexSourceNode node, LocalExecutionPlanContext context)
        {
            checkState(context.getIndexSourceContext().isPresent(), "Must be in an index source context");
            IndexSourceContext indexSourceContext = context.getIndexSourceContext().get();

            SetMultimap<Symbol, Integer> indexLookupToProbeInput = indexSourceContext.getIndexLookupToProbeInput();
            checkState(indexLookupToProbeInput.keySet().equals(node.getLookupSymbols()));

            // Finalize the symbol lookup layout for the index source
            List<Symbol> lookupSymbolSchema = ImmutableList.copyOf(node.getLookupSymbols());

            // Identify how to remap the probe key Input to match the source index lookup layout
            ImmutableList.Builder<Integer> remappedProbeKeyChannelsBuilder = ImmutableList.builder();
            // Identify overlapping fields that can produce the same lookup symbol.
            // We will filter incoming keys to ensure that overlapping fields will have the same value.
            ImmutableList.Builder<Set<Integer>> overlappingFieldSetsBuilder = ImmutableList.builder();
            for (Symbol lookupSymbol : lookupSymbolSchema) {
                Set<Integer> potentialProbeInputs = indexLookupToProbeInput.get(lookupSymbol);
                checkState(!potentialProbeInputs.isEmpty(), "Must have at least one source from the probe input");
                if (potentialProbeInputs.size() > 1) {
                    overlappingFieldSetsBuilder.add(potentialProbeInputs.stream().collect(toImmutableSet()));
                }
                remappedProbeKeyChannelsBuilder.add(Iterables.getFirst(potentialProbeInputs, null));
            }
            List<Set<Integer>> overlappingFieldSets = overlappingFieldSetsBuilder.build();
            List<Integer> remappedProbeKeyChannels = remappedProbeKeyChannelsBuilder.build();
            Function<RecordSet, RecordSet> probeKeyNormalizer = recordSet -> {
                if (!overlappingFieldSets.isEmpty()) {
                    recordSet = new FieldSetFilteringRecordSet(metadata, recordSet, overlappingFieldSets);
                }
                return new MappedRecordSet(recordSet, remappedProbeKeyChannels);
            };

            // Declare the input and output schemas for the index and acquire the actual Index
            List<ColumnHandle> lookupSchema = Lists.transform(lookupSymbolSchema, forMap(node.getAssignments()));
            List<ColumnHandle> outputSchema = Lists.transform(node.getOutputSymbols(), forMap(node.getAssignments()));
            ConnectorIndex index = indexManager.getIndex(session, node.getIndexHandle(), lookupSchema, outputSchema);

            OperatorFactory operatorFactory = new IndexSourceOperator.IndexSourceOperatorFactory(context.getNextOperatorId(), node.getId(), index, probeKeyNormalizer);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        /**
         * This method creates a mapping from each index source lookup symbol (directly applied to the index)
         * to the corresponding probe key Input
         */
        private SetMultimap<Symbol, Integer> mapIndexSourceLookupSymbolToProbeKeyInput(IndexJoinNode node, Map<Symbol, Integer> probeKeyLayout)
        {
            Set<Symbol> indexJoinSymbols = node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getIndex)
                    .collect(toImmutableSet());

            // Trace the index join symbols to the index source lookup symbols
            // Map: Index join symbol => Index source lookup symbol
            Map<Symbol, Symbol> indexKeyTrace = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), indexJoinSymbols);

            // Map the index join symbols to the probe key Input
            Multimap<Symbol, Integer> indexToProbeKeyInput = HashMultimap.create();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                indexToProbeKeyInput.put(clause.getIndex(), probeKeyLayout.get(clause.getProbe()));
            }

            // Create the mapping from index source look up symbol to probe key Input
            ImmutableSetMultimap.Builder<Symbol, Integer> builder = ImmutableSetMultimap.builder();
            for (Map.Entry<Symbol, Symbol> entry : indexKeyTrace.entrySet()) {
                Symbol indexJoinSymbol = entry.getKey();
                Symbol indexLookupSymbol = entry.getValue();
                builder.putAll(indexLookupSymbol, indexToProbeKeyInput.get(indexJoinSymbol));
            }
            return builder.build();
        }

        @Override
        public PhysicalOperation visitIndexJoin(IndexJoinNode node, LocalExecutionPlanContext context)
        {
            List<IndexJoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<Symbol> probeSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getProbe);
            List<Symbol> indexSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getIndex);

            // Plan probe side
            PhysicalOperation probeSource = node.getProbeSource().accept(this, context);
            List<Integer> probeChannels = getChannelsForSymbols(probeSymbols, probeSource.getLayout());
            OptionalInt probeHashChannel = node.getProbeHashSymbol().map(channelGetter(probeSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            // The probe key channels will be handed to the index according to probeSymbol order
            Map<Symbol, Integer> probeKeyLayout = new HashMap<>();
            for (int i = 0; i < probeSymbols.size(); i++) {
                // Duplicate symbols can appear and we only need to take take one of the Inputs
                probeKeyLayout.put(probeSymbols.get(i), i);
            }

            // Plan the index source side
            SetMultimap<Symbol, Integer> indexLookupToProbeInput = mapIndexSourceLookupSymbolToProbeKeyInput(node, probeKeyLayout);
            LocalExecutionPlanContext indexContext = context.createIndexSourceSubContext(new IndexSourceContext(indexLookupToProbeInput));
            PhysicalOperation indexSource = node.getIndexSource().accept(this, indexContext);
            List<Integer> indexOutputChannels = getChannelsForSymbols(indexSymbols, indexSource.getLayout());
            OptionalInt indexHashChannel = node.getIndexHashSymbol().map(channelGetter(indexSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            // Identify just the join keys/channels needed for lookup by the index source (does not have to use all of them).
            Set<Symbol> indexSymbolsNeededBySource = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), ImmutableSet.copyOf(indexSymbols)).keySet();

            Set<Integer> lookupSourceInputChannels = node.getCriteria().stream()
                    .filter(equiJoinClause -> indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                    .map(IndexJoinNode.EquiJoinClause::getProbe)
                    .map(probeKeyLayout::get)
                    .collect(toImmutableSet());

            Optional<DynamicTupleFilterFactory> dynamicTupleFilterFactory = Optional.empty();
            if (lookupSourceInputChannels.size() < probeKeyLayout.values().size()) {
                int[] nonLookupInputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getProbe)
                        .map(probeKeyLayout::get)
                        .collect(toImmutableList()));
                int[] nonLookupOutputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getIndex)
                        .map(indexSource.getLayout()::get)
                        .collect(toImmutableList()));

                int filterOperatorId = indexContext.getNextOperatorId();
                dynamicTupleFilterFactory = Optional.of(new DynamicTupleFilterFactory(
                        filterOperatorId,
                        node.getId(),
                        nonLookupInputChannels,
                        nonLookupOutputChannels,
                        indexSource.getTypes(),
                        pageFunctionCompiler));
            }

            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider = new IndexBuildDriverFactoryProvider(
                    indexContext.getNextPipelineId(),
                    indexContext.getNextOperatorId(),
                    node.getId(),
                    indexContext.isInputDriver(),
                    indexSource.getTypes(),
                    indexSource.getOperatorFactories(),
                    dynamicTupleFilterFactory);

            IndexLookupSourceFactory indexLookupSourceFactory = new IndexLookupSourceFactory(
                    lookupSourceInputChannels,
                    indexOutputChannels,
                    indexHashChannel,
                    indexSource.getTypes(),
                    indexSource.getLayout(),
                    indexBuildDriverFactoryProvider,
                    maxIndexMemorySize,
                    indexJoinLookupStats,
                    SystemSessionProperties.isShareIndexLoading(session),
                    pagesIndexFactory,
                    joinCompiler);

            verify(probeSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION);
            verify(indexSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION);
            JoinBridgeManager<LookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    false,
                    UNGROUPED_EXECUTION,
                    UNGROUPED_EXECUTION,
                    lifespan -> indexLookupSourceFactory,
                    indexLookupSourceFactory.getOutputTypes());

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from index side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTypes().size();
            for (Map.Entry<Symbol, Integer> entry : indexSource.getLayout().entrySet()) {
                Integer input = entry.getValue();
                outputMappings.put(entry.getKey(), offset + input);
            }

            OperatorFactory lookupJoinOperatorFactory;
            OptionalInt totalOperatorsCount = context.getDriverInstanceCount();
            switch (node.getType()) {
                case INNER:
                    lookupJoinOperatorFactory = lookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeSource.getTypes(), probeChannels, probeHashChannel, Optional.empty(), totalOperatorsCount, unsupportedPartitioningSpillerFactory());
                    break;
                case SOURCE_OUTER:
                    lookupJoinOperatorFactory = lookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeSource.getTypes(), probeChannels, probeHashChannel, Optional.empty(), totalOperatorsCount, unsupportedPartitioningSpillerFactory());
                    break;
                default:
                    throw new AssertionError("Unknown type: " + node.getType());
            }
            return new PhysicalOperation(lookupJoinOperatorFactory, outputMappings.build(), context, probeSource);
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            if (node.isCrossJoin()) {
                return createNestedLoopJoin(node, context);
            }

            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            // TODO: Execution must be plugged in here
            if (!node.getDynamicFilters().isEmpty()) {
                log.debug("[Join] Dynamic filters: %s", node.getDynamicFilters());
            }

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            switch (node.getType()) {
                case INNER:
                case LEFT:
                case RIGHT:
                case FULL:
                    if (node.getOutputSymbols().size() == 0 || !node.getOutputSymbols().get(node.getOutputSymbols().size()-1).getName().startsWith("query_set"))
                        return createLookupJoin(node, node.getLeft(), leftSymbols, node.getLeftHashSymbol(), node.getRight(), rightSymbols, node.getRightHashSymbol(), context);
                    else
                        return createSharedLookupJoin(node, node.getLeft(), leftSymbols, node.getLeftHashSymbol(), node.getRight(), rightSymbols, node.getRightHashSymbol(), context);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        public boolean readSharedOperationValue(){

            try {
                FileInputStream fstream = new FileInputStream("/scratch/venkates/bqo-working/input.txt");
                BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
                String strLine;
                int value = 0;
                while ((strLine = br.readLine()) != null) {
                    value = Integer.parseInt(strLine);
                }
                // Close the input stream
                br.close();
                fstream.close();
                //print the contents of a
                if(value == 1){
                    //System.out.println("VSK: Shared operation is true");
                    return  true;
                }
                else {
                    //System.out.println("VSK: Shared operation is false");
                    return false;
                }

            } catch (Exception e) {// Catch exception if any
                System.err.println("Error: " + e.getMessage());
            }
            //System.out.println("VSK: It should not come here");
            return false;
        }

        @Override
        public PhysicalOperation visitSpatialJoin(SpatialJoinNode node, LocalExecutionPlanContext context)
        {
            RowExpression filterExpression = node.getFilter();
            List<CallExpression> spatialFunctions = extractSupportedSpatialFunctions(filterExpression);
            for (CallExpression spatialFunction : spatialFunctions) {
                Optional<PhysicalOperation> operation = tryCreateSpatialJoin(context, node, removeExpressionFromFilter(filterExpression, spatialFunction), spatialFunction, Optional.empty(), Optional.empty());
                if (operation.isPresent()) {
                    return operation.get();
                }
            }

            List<CallExpression> spatialComparisons = extractSupportedSpatialComparisons(filterExpression);
            for (CallExpression spatialComparison : spatialComparisons) {
                if (unmangleOperator(spatialComparison.getSignature().getName()) == LESS_THAN || unmangleOperator(spatialComparison.getSignature().getName()) == LESS_THAN_OR_EQUAL) {
                    // ST_Distance(a, b) <= r
                    RowExpression radius = spatialComparison.getArguments().get(1);
                    if (radius instanceof VariableReferenceExpression && node.getRight().getOutputSymbols().contains(toSymbol(((VariableReferenceExpression) radius)))) {
                        CallExpression spatialFunction = (CallExpression) spatialComparison.getArguments().get(0);
                        Optional<PhysicalOperation> operation = tryCreateSpatialJoin(context, node, removeExpressionFromFilter(filterExpression, spatialComparison), spatialFunction, Optional.of((VariableReferenceExpression) radius), Optional.of(unmangleOperator(spatialComparison.getSignature().getName())));
                        if (operation.isPresent()) {
                            return operation.get();
                        }
                    }
                }
            }

            throw new VerifyException("No valid spatial relationship found for spatial join");
        }

        private Optional<PhysicalOperation> tryCreateSpatialJoin(
                LocalExecutionPlanContext context,
                SpatialJoinNode node,
                Optional<RowExpression> filterExpression,
                CallExpression spatialFunction,
                Optional<VariableReferenceExpression> radius,
                Optional<OperatorType> comparisonOperator)
        {
            List<RowExpression> arguments = spatialFunction.getArguments();
            verify(arguments.size() == 2);

            if (!(arguments.get(0) instanceof VariableReferenceExpression) || !(arguments.get(1) instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }

            VariableReferenceExpression firstVariable = (VariableReferenceExpression) arguments.get(0);
            VariableReferenceExpression secondVariable = (VariableReferenceExpression) arguments.get(1);

            PlanNode probeNode = node.getLeft();
            Set<SymbolReference> probeSymbols = getSymbolReferences(probeNode.getOutputSymbols());

            PlanNode buildNode = node.getRight();
            Set<SymbolReference> buildSymbols = getSymbolReferences(buildNode.getOutputSymbols());

            if (probeSymbols.contains(new SymbolReference(firstVariable.getName())) && buildSymbols.contains(new SymbolReference(secondVariable.getName()))) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        toSymbol(firstVariable),
                        buildNode,
                        toSymbol(secondVariable),
                        radius.map(VariableReferenceSymbolConverter::toSymbol),
                        spatialTest(spatialFunction, true, comparisonOperator),
                        filterExpression,
                        context));
            }
            if (probeSymbols.contains(new SymbolReference(secondVariable.getName())) && buildSymbols.contains(new SymbolReference(firstVariable.getName()))) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        toSymbol(secondVariable),
                        buildNode,
                        toSymbol(firstVariable),
                        radius.map(VariableReferenceSymbolConverter::toSymbol),
                        spatialTest(spatialFunction, false, comparisonOperator),
                        filterExpression,
                        context));
            }
            return Optional.empty();
        }

        private Optional<RowExpression> removeExpressionFromFilter(RowExpression filter, RowExpression expression)
        {
            RowExpression updatedJoinFilter = replaceExpression(filter, ImmutableMap.of(expression, TRUE_CONSTANT));
            return updatedJoinFilter == TRUE_CONSTANT ? Optional.empty() : Optional.of(updatedJoinFilter);
        }

        private SpatialPredicate spatialTest(CallExpression functionCall, boolean probeFirst, Optional<OperatorType> comparisonOperator)
        {
            switch (functionCall.getSignature().getName().toString().toLowerCase(Locale.ENGLISH)) {
                case ST_CONTAINS:
                    if (probeFirst) {
                        return (buildGeometry, probeGeometry, radius) -> probeGeometry.contains(buildGeometry);
                    }
                    else {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.contains(probeGeometry);
                    }
                case ST_WITHIN:
                    if (probeFirst) {
                        return (buildGeometry, probeGeometry, radius) -> probeGeometry.within(buildGeometry);
                    }
                    else {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.within(probeGeometry);
                    }
                case ST_INTERSECTS:
                    return (buildGeometry, probeGeometry, radius) -> buildGeometry.intersects(probeGeometry);
                case ST_DISTANCE:
                    if (comparisonOperator.get() == LESS_THAN) {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) < radius.getAsDouble();
                    }
                    else if (comparisonOperator.get() == LESS_THAN_OR_EQUAL) {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) <= radius.getAsDouble();
                    }
                    else {
                        throw new UnsupportedOperationException("Unsupported comparison operator: " + comparisonOperator.get());
                    }
                default:
                    throw new UnsupportedOperationException("Unsupported spatial function: " + functionCall.getSignature().getName());
            }
        }

        private Set<SymbolReference> getSymbolReferences(Collection<Symbol> symbols)
        {
            return symbols.stream().map(SymbolUtils::toSymbolReference).collect(toImmutableSet());
        }

        private PhysicalOperation createNestedLoopJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation probeSource = node.getLeft().accept(this, context);

            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getRight().accept(this, buildContext);

            checkState(
                    buildSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION,
                    "Build source of a nested loop join is expected to be GROUPED_EXECUTION.");
            checkArgument(node.getType() == INNER, "NestedLoopJoin is only used for inner join");

            JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                    false,
                    probeSource.getPipelineExecutionStrategy(),
                    buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new NestedLoopJoinPagesSupplier(),
                    buildSource.getTypes());
            NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    nestedLoopJoinBridgeManager);

            int taskCount = buildContext.getDriverInstanceCount().orElse(1);
            checkArgument(taskCount == 1, "Expected local execution to not be parallel");

            ImmutableList.Builder<OperatorFactory> factoriesBuilder = ImmutableList.builder();
            factoriesBuilder.addAll(buildSource.getOperatorFactories());

            createDynamicFilter(node, context, taskCount).ifPresent(
                    filter -> {
                        List<DynamicFilterSourceOperator.Channel> filterBuildChannels = filter
                                .getBuildChannels()
                                .entrySet()
                                .stream()
                                .map(entry -> {
                                    String filterId = entry.getKey();
                                    int index = entry.getValue();
                                    Type type = buildSource.getTypes().get(index);
                                    return new DynamicFilterSourceOperator.Channel(filterId, type, index, context.getSession().getQueryId().toString());
                                })
                                .collect(Collectors.toList());
                        factoriesBuilder.add(
                                new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                                        buildContext.getNextOperatorId(),
                                        node.getId(),
                                        filter.getValueConsumer(), /** the consumer to process all values collected to build the dynamic filter */
                                        filterBuildChannels,
                                        getDynamicFilteringMaxPerDriverValueCount(buildContext.getSession()),
                                        getDynamicFilteringMaxPerDriverSize(buildContext.getSession())));
                    });

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    factoriesBuilder
                            .add(nestedLoopBuildOperatorFactory)
                            .build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from build side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTypes().size();
            for (Map.Entry<Symbol, Integer> entry : buildSource.getLayout().entrySet()) {
                outputMappings.put(entry.getKey(), offset + entry.getValue());
            }

            OperatorFactory operatorFactory = new NestedLoopJoinOperatorFactory(context.getNextOperatorId(), node.getId(), nestedLoopJoinBridgeManager);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, probeSource);
        }

        private PhysicalOperation createSpatialLookupJoin(
                SpatialJoinNode node,
                PlanNode probeNode,
                Symbol probeSymbol,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                SpatialPredicate spatialRelationshipTest,
                Optional<RowExpression> joinFilter,
                LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            PagesSpatialIndexFactory pagesSpatialIndexFactory = createPagesSpatialIndexFactory(node,
                    buildNode,
                    buildSymbol,
                    radiusSymbol,
                    probeSource.getLayout(),
                    spatialRelationshipTest,
                    joinFilter,
                    context);

            OperatorFactory operator = createSpatialLookupJoin(node, probeNode, probeSource, probeSymbol, pagesSpatialIndexFactory, context);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.build(), context, probeSource);
        }

        private OperatorFactory createSpatialLookupJoin(SpatialJoinNode node,
                PlanNode probeNode,
                PhysicalOperation probeSource,
                Symbol probeSymbol,
                PagesSpatialIndexFactory pagesSpatialIndexFactory,
                LocalExecutionPlanContext context)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Symbol> probeOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> probeNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            Function<Symbol, Integer> probeChannelGetter = channelGetter(probeSource);
            int probeChannel = probeChannelGetter.apply(probeSymbol);

            Optional<Integer> partitionChannel = node.getLeftPartitionSymbol().map(probeChannelGetter::apply);

            return new SpatialJoinOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getType(),
                    probeTypes,
                    probeOutputChannels,
                    probeChannel,
                    partitionChannel,
                    pagesSpatialIndexFactory);
        }

        private PagesSpatialIndexFactory createPagesSpatialIndexFactory(
                SpatialJoinNode node,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                Map<Symbol, Integer> probeLayout,
                SpatialPredicate spatialRelationshipTest,
                Optional<RowExpression> joinFilter,
                LocalExecutionPlanContext context)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);
            List<Symbol> buildOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> buildNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            Map<Symbol, Integer> buildLayout = buildSource.getLayout();
            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(buildOutputSymbols, buildLayout));
            Function<Symbol, Integer> buildChannelGetter = channelGetter(buildSource);
            Integer buildChannel = buildChannelGetter.apply(buildSymbol);
            Optional<Integer> radiusChannel = radiusSymbol.map(buildChannelGetter::apply);

            Optional<JoinFilterFunctionFactory> filterFunctionFactory = joinFilter
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeLayout,
                            buildLayout,
                            context.getTypes(),
                            context.getSession()));

            Optional<Integer> partitionChannel = node.getRightPartitionSymbol().map(buildChannelGetter::apply);

            SpatialIndexBuilderOperatorFactory builderOperatorFactory = new SpatialIndexBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes(),
                    buildOutputChannels,
                    buildChannel,
                    radiusChannel,
                    partitionChannel,
                    spatialRelationshipTest,
                    node.getKdbTree(),
                    filterFunctionFactory,
                    10_000,
                    pagesIndexFactory);

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    ImmutableList.<OperatorFactory>builder()
                            .addAll(buildSource.getOperatorFactories())
                            .add(builderOperatorFactory)
                            .build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            return builderOperatorFactory.getPagesSpatialIndexFactory();
        }

        private PhysicalOperation createLookupJoin(JoinNode node,
                PlanNode probeNode,
                List<Symbol> probeSymbols,
                Optional<Symbol> probeHashSymbol,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                Optional<Symbol> buildHashSymbol,
                LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            boolean spillEnabled = isSpillEnabled(session) && node.isSpillable().orElseThrow(() -> new IllegalArgumentException("spillable not yet set"));
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory =
                    createLookupSourceFactory(node, buildNode, buildSymbols, buildHashSymbol, probeSource, context, spillEnabled);

            OperatorFactory operator = createLookupJoin(node, probeSource, probeSymbols, probeHashSymbol, lookupSourceFactory, context, spillEnabled);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.build(), context, probeSource);
        }

        private PhysicalOperation createSharedLookupJoin(JoinNode node,
                                                   PlanNode probeNode,
                                                   List<Symbol> probeSymbols,
                                                   Optional<Symbol> probeHashSymbol,
                                                   PlanNode buildNode,
                                                   List<Symbol> buildSymbols,
                                                   Optional<Symbol> buildHashSymbol,
                                                   LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            boolean spillEnabled = isSpillEnabled(session) && node.isSpillable().orElseThrow(() -> new IllegalArgumentException("spillable not yet set"));
            SharedJoinBridgeManager<SharedPartitionedLookupSourceFactory> lookupSourceFactory =
                    createSharedLookupSourceFactory(node, buildNode, buildSymbols, buildHashSymbol, probeSource, context, spillEnabled);

            OperatorFactory operator = createSharedLookupJoin(node, probeSource, probeSymbols, probeHashSymbol, buildNode, lookupSourceFactory, context, spillEnabled);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();

            //the new one
            /*List<Symbol> outputSymbols = new ArrayList<>(probeNode.getOutputSymbols());
            for (int i =0; i< buildNode.getOutputSymbols().size(); i++){
                outputSymbols.add(buildNode.getOutputSymbols().get(i));
            }*/

            /*List<Symbol> outputSymbols = new ArrayList<>();

            outputSymbols.addAll(node.getOutputSymbols().subList(0, node.getOutputSymbols().size()-1).stream()
                    .filter(symbol -> node.getLeft().getOutputSymbols().contains(symbol))
                    .collect(toImmutableList()));

            outputSymbols.addAll(node.getOutputSymbols().subList(0, node.getOutputSymbols().size()-1).stream()
                    .filter(symbol -> node.getRight().getOutputSymbols().contains(symbol))
                    .collect(toImmutableList()));

            outputSymbols.add(node.getOutputSymbols().get(node.getOutputSymbols().size()-1));

            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }*/

            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.build(), context, probeSource);
        }

        private Optional<LocalDynamicFilter> createDynamicFilter(JoinNode node, LocalExecutionPlanContext context, int partitionCount)
        {
            if (!isEnableDynamicFiltering(context.getSession())) {
                return Optional.empty();
            }
            if (node.getDynamicFilters().isEmpty()) {
                return Optional.empty();
            }
            LocalDynamicFiltersCollector collector = context.getDynamicFiltersCollector();
            return LocalDynamicFilter
                    .create(node, partitionCount, context.getSession(), context.taskContext.getTaskId(), stateStoreProvider)
                    .map(filter -> {
                        // Intersect dynamic filters' predicates when they become ready,
                        // in order to support multiple join nodes in the same plan fragment.
                        addSuccessCallback(filter.getDynamicFilterResultFuture(), collector::intersectDynamicFilter);
                        return filter;
                    });
        }

        private Optional<LocalDynamicFilter> createDynamicFilter(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            if (!node.getDynamicFilterId().isPresent()) {
                return Optional.empty();
            }
            LocalDynamicFiltersCollector collector = context.getDynamicFiltersCollector();
            return LocalDynamicFilter
                    .create(node, context.getSession(), context.taskContext.getTaskId(), stateStoreProvider)
                    .map(filter -> {
                        addSuccessCallback(filter.getDynamicFilterResultFuture(), collector::intersectDynamicFilter);
                        return filter;
                    });
        }

        private JoinBridgeManager<PartitionedLookupSourceFactory> createLookupSourceFactory(
                JoinNode node,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                Optional<Symbol> buildHashSymbol,
                PhysicalOperation probeSource,
                LocalExecutionPlanContext context,
                boolean spillEnabled)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);

            if (buildSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION) {
                checkState(
                        probeSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION,
                        "Build execution is GROUPED_EXECUTION. Probe execution is expected be GROUPED_EXECUTION, but is UNGROUPED_EXECUTION.");
            }

            List<Symbol> buildOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> node.getRight().getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(buildOutputSymbols, buildSource.getLayout()));
            List<Integer> buildChannels = ImmutableList.copyOf(getChannelsForSymbols(buildSymbols, buildSource.getLayout()));
            OptionalInt buildHashChannel = buildHashSymbol.map(channelGetter(buildSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            boolean buildOuter = node.getType() == RIGHT || node.getType() == FULL;
            int taskCount = buildContext.getDriverInstanceCount().orElse(1);

            Optional<JoinFilterFunctionFactory> filterFunctionFactory = node.getFilter()
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeSource.getLayout(),
                            buildSource.getLayout(),
                            context.getTypes(),
                            context.getSession()));

            Optional<SortExpressionContext> sortExpressionContext = node.getFilter().flatMap(filter -> SortExpressionExtractor.extractSortExpression(metadata, node.getRightOutputSymbols(), filter));

            Optional<Integer> sortChannel = sortExpressionContext
                    .map(SortExpressionContext::getSortExpression)
                    .map(sortExpression -> sortExpressionAsSortChannel(sortExpression, probeSource.getLayout(), buildSource.getLayout(), context));

            List<JoinFilterFunctionFactory> searchFunctionFactories = sortExpressionContext
                    .map(SortExpressionContext::getSearchExpressions)
                    .map(searchExpressions -> searchExpressions.stream()
                            .map(searchExpression -> compileJoinFilterFunction(
                                    searchExpression,
                                    probeSource.getLayout(),
                                    buildSource.getLayout(),
                                    context.getTypes(),
                                    context.getSession()))
                            .collect(toImmutableList()))
                    .orElse(ImmutableList.of());

            ImmutableList<Type> buildOutputTypes = buildOutputChannels.stream()
                    .map(buildSource.getTypes()::get)
                    .collect(toImmutableList());
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    buildOuter,
                    probeSource.getPipelineExecutionStrategy(),
                    buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new PartitionedLookupSourceFactory(
                            buildSource.getTypes(),
                            buildOutputTypes,
                            buildChannels.stream()
                                    .map(buildSource.getTypes()::get)
                                    .collect(toImmutableList()),
                            taskCount,
                            buildSource.getLayout(),
                            buildOuter),
                    buildOutputTypes);

            ImmutableList.Builder<OperatorFactory> factoriesBuilder = new ImmutableList.Builder();
            factoriesBuilder.addAll(buildSource.getOperatorFactories());

            createDynamicFilter(node, context, taskCount).ifPresent(
                    filter -> {
                        List<DynamicFilterSourceOperator.Channel> filterBuildChannels = filter
                                .getBuildChannels()
                                .entrySet()
                                .stream()
                                .map(entry -> {
                                    String filterId = entry.getKey();
                                    int index = entry.getValue();
                                    Type type = buildSource.getTypes().get(index);
                                    return new DynamicFilterSourceOperator.Channel(filterId, type, index, context.getSession().getQueryId().toString());
                                })
                                .collect(Collectors.toList());
                        factoriesBuilder.add(
                                new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                                        buildContext.getNextOperatorId(),
                                        node.getId(),
                                        filter.getValueConsumer(), /** the consumer to process all values collected to build the dynamic filter */
                                        filterBuildChannels,
                                        getDynamicFilteringMaxPerDriverValueCount(buildContext.getSession()),
                                        getDynamicFilteringMaxPerDriverSize(buildContext.getSession())));
                    });

            HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    lookupSourceFactoryManager,
                    buildOutputChannels,
                    buildChannels,
                    buildHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    10_000,
                    pagesIndexFactory,
                    spillEnabled && !buildOuter && taskCount > 1,
                    singleStreamSpillerFactory);

            factoriesBuilder.add(hashBuilderOperatorFactory);

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    factoriesBuilder.build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            return lookupSourceFactoryManager;
        }

        private SharedJoinBridgeManager<SharedPartitionedLookupSourceFactory> createSharedLookupSourceFactory(
                JoinNode node,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                Optional<Symbol> buildHashSymbol,
                PhysicalOperation probeSource,
                LocalExecutionPlanContext context,
                boolean spillEnabled)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);

            //System.out.println("VSK: CreateSharedLoopkipSourceFactory: buildNode.getOutputSymbols: "+buildNode.getOutputSymbols()+" node.getrightoutputsymbols: "+node.getRightOutputSymbols());

            if (buildSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION) {
                checkState(
                        probeSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION,
                        "Build execution is GROUPED_EXECUTION. Probe execution is expected be GROUPED_EXECUTION, but is UNGROUPED_EXECUTION.");
            }

            List<Symbol> buildOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> node.getRight().getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(buildOutputSymbols, buildSource.getLayout()));
            List<Integer> buildChannels = ImmutableList.copyOf(getChannelsForSymbols(buildSymbols, buildSource.getLayout()));
            OptionalInt buildHashChannel = buildHashSymbol.map(channelGetter(buildSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            boolean buildOuter = node.getType() == RIGHT || node.getType() == FULL;
            int taskCount = buildContext.getDriverInstanceCount().orElse(1);

            Optional<JoinFilterFunctionFactory> filterFunctionFactory = node.getFilter()
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeSource.getLayout(),
                            buildSource.getLayout(),
                            context.getTypes(),
                            context.getSession()));

            Optional<SortExpressionContext> sortExpressionContext = node.getFilter().flatMap(filter -> SortExpressionExtractor.extractSortExpression(metadata, node.getRightOutputSymbols(), filter));

            Optional<Integer> sortChannel = sortExpressionContext
                    .map(SortExpressionContext::getSortExpression)
                    .map(sortExpression -> sortExpressionAsSortChannel(sortExpression, probeSource.getLayout(), buildSource.getLayout(), context));

            List<JoinFilterFunctionFactory> searchFunctionFactories = sortExpressionContext
                    .map(SortExpressionContext::getSearchExpressions)
                    .map(searchExpressions -> searchExpressions.stream()
                            .map(searchExpression -> compileJoinFilterFunction(
                                    searchExpression,
                                    probeSource.getLayout(),
                                    buildSource.getLayout(),
                                    context.getTypes(),
                                    context.getSession()))
                            .collect(toImmutableList()))
                    .orElse(ImmutableList.of());

            /*original: ImmutableList<Type> buildOutputTypes = buildOutputChannels.stream()
                    .map(buildSource.getTypes()::get)
                    .collect(toImmutableList());*/
            List<Type> temp = new ArrayList<>(buildOutputChannels.stream()
                    .map(buildSource.getTypes()::get)
                    .collect(toImmutableList()));
            //temp.add(BIGINT);
            ImmutableList<Type> buildOutputTypes = ImmutableList.copyOf(temp);

            temp = new ArrayList<>(buildSource.getTypes());
            //temp.add(BIGINT);
            final List<Type> buildSourceType = temp;

            List<Integer> tempChannel = new ArrayList<>(buildOutputChannels);
            //tempChannel.add(buildSource.getTypes().size());
            buildOutputChannels = ImmutableList.copyOf(tempChannel);

            //System.out.println("VSK: CreateSharedLookupSourceFactory: buildOutputChannels: "+ buildOutputChannels+" buildSouceType: "+buildSourceType+" buildOutput Types:"+buildOutputTypes);

            SharedJoinBridgeManager<SharedPartitionedLookupSourceFactory> lookupSourceFactoryManager = new SharedJoinBridgeManager<>(
                    buildOuter,
                    probeSource.getPipelineExecutionStrategy(),
                    buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new SharedPartitionedLookupSourceFactory(
                            //buildSource.getTypes(),
                            buildSourceType,
                            buildOutputTypes,
                            buildChannels.stream()
                                    .map(buildSource.getTypes()::get)
                                    .collect(toImmutableList()),
                            taskCount,
                            buildSource.getLayout(),
                            buildOuter),
                    buildOutputTypes);

            ImmutableList.Builder<OperatorFactory> factoriesBuilder = new ImmutableList.Builder();
            factoriesBuilder.addAll(buildSource.getOperatorFactories());

            createDynamicFilter(node, context, taskCount).ifPresent(
                    filter -> {
                        List<DynamicFilterSourceOperator.Channel> filterBuildChannels = filter
                                .getBuildChannels()
                                .entrySet()
                                .stream()
                                .map(entry -> {
                                    String filterId = entry.getKey();
                                    int index = entry.getValue();
                                    Type type = buildSource.getTypes().get(index);
                                    return new DynamicFilterSourceOperator.Channel(filterId, type, index, context.getSession().getQueryId().toString());
                                })
                                .collect(Collectors.toList());
                        factoriesBuilder.add(
                                new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                                        buildContext.getNextOperatorId(),
                                        node.getId(),
                                        filter.getValueConsumer(), /** the consumer to process all values collected to build the dynamic filter */
                                        filterBuildChannels,
                                        getDynamicFilteringMaxPerDriverValueCount(buildContext.getSession()),
                                        getDynamicFilteringMaxPerDriverSize(buildContext.getSession())));
                    });

            SharedHashBuilderOperator.SharedHashBuilderOperatorFactory hashBuilderOperatorFactory = new SharedHashBuilderOperator.SharedHashBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    lookupSourceFactoryManager,
                    buildOutputChannels,
                    buildChannels,
                    buildHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    buildSource.getTypes().size()-1,
                    searchFunctionFactories,
                    10_000,
                    sharedPagesIndexFactory,
                    spillEnabled && !buildOuter && taskCount > 1,
                    singleStreamSpillerFactory);

            factoriesBuilder.add(hashBuilderOperatorFactory);

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    factoriesBuilder.build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            return lookupSourceFactoryManager;
        }


        private JoinFilterFunctionFactory compileJoinFilterFunction(
                RowExpression filterExpression,
                Map<Symbol, Integer> probeLayout,
                Map<Symbol, Integer> buildLayout,
                TypeProvider types,
                Session session)
        {
            Map<Symbol, Integer> joinSourcesLayout = createJoinSourcesLayout(buildLayout, probeLayout);
            return joinFilterFunctionCompiler.compileJoinFilterFunction(bindChannels(filterExpression, joinSourcesLayout, types), buildLayout.size());
        }

        private int sortExpressionAsSortChannel(
                RowExpression sortExpression,
                Map<Symbol, Integer> probeLayout,
                Map<Symbol, Integer> buildLayout,
                LocalExecutionPlanContext context)
        {
            Map<Symbol, Integer> joinSourcesLayout = createJoinSourcesLayout(buildLayout, probeLayout);
            RowExpression rewrittenSortExpression = bindChannels(sortExpression, joinSourcesLayout, context.getTypes());
            checkArgument(rewrittenSortExpression instanceof InputReferenceExpression, "Unsupported expression type [%s]", rewrittenSortExpression);
            return ((InputReferenceExpression) rewrittenSortExpression).getField();
        }

        private OperatorFactory createSharedLookupJoin(
                JoinNode node,
                PhysicalOperation probeSource,
                List<Symbol> probeSymbols,
                Optional<Symbol> probeHashSymbol,
                PlanNode buildNode,
                SharedJoinBridgeManager<? extends SharedLookupSourceFactory> lookupSourceFactoryManager,
                LocalExecutionPlanContext context,
                boolean spillEnabled)
        {
            List<Type> probeTypes = new ArrayList<>(probeSource.getTypes());
            //probeTypes.add(BIGINT);
            List<Symbol> probeOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> node.getLeft().getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            List<Integer> probeJoinChannels = ImmutableList.copyOf(getChannelsForSymbols(probeSymbols, probeSource.getLayout()));
            OptionalInt probeHashChannel = probeHashSymbol.map(channelGetter(probeSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());
            OptionalInt totalOperatorsCount = context.getDriverInstanceCount();
            checkState(!spillEnabled || totalOperatorsCount.isPresent(), "A fixed distribution is required for JOIN when spilling is enabled");


            //List<Integer> tempChannel = new ArrayList<>(probeOutputChannels);
            //tempChannel.add(probeSource.getTypes().size()-1);
            //probeOutputChannels = ImmutableList.copyOf(tempChannel);

            //System.out.println("VSK: Create SharedLookup join: Probe output channel :"+probeOutputChannels); //+"VSK: Probe Hash Symbol:"+probeHashSymbol.toString()+" Build symbols: "+buildSymbols.toString());
            //System.out.println("VSK probe Types "+probeTypes+" probe join channel: "+probeJoinChannels+" query set channel is "+(probeTypes.size()-1)+" build query set channel: "+buildNode.getOutputSymbols().size());

            return sharedOperators.innerJoin(
                    context.getNextOperatorId(),
                    node.getId(),
                    lookupSourceFactoryManager,
                    probeTypes,
                    probeJoinChannels,
                    probeHashChannel,
                    probeTypes.size()-1,
                    buildNode.getOutputSymbols().size()-1,
                    Optional.of(probeOutputChannels),
                    totalOperatorsCount,
                    partitioningSpillerFactory);

        }

        private OperatorFactory createLookupJoin(
                JoinNode node,
                PhysicalOperation probeSource,
                List<Symbol> probeSymbols,
                Optional<Symbol> probeHashSymbol,
                JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
                LocalExecutionPlanContext context,
                boolean spillEnabled)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Symbol> probeOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> node.getLeft().getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            List<Integer> probeJoinChannels = ImmutableList.copyOf(getChannelsForSymbols(probeSymbols, probeSource.getLayout()));
            OptionalInt probeHashChannel = probeHashSymbol.map(channelGetter(probeSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());
            OptionalInt totalOperatorsCount = context.getDriverInstanceCount();
            checkState(!spillEnabled || totalOperatorsCount.isPresent(), "A fixed distribution is required for JOIN when spilling is enabled");

            switch (node.getType()) {
                case INNER:
                    return lookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                case LEFT:
                    return lookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                case RIGHT:
                    return lookupJoinOperators.lookupOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                case FULL:
                    return lookupJoinOperators.fullOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        private Map<Symbol, Integer> createJoinSourcesLayout(Map<Symbol, Integer> lookupSourceLayout, Map<Symbol, Integer> probeSourceLayout)
        {
            ImmutableMap.Builder<Symbol, Integer> joinSourcesLayout = ImmutableMap.builder();
            joinSourcesLayout.putAll(lookupSourceLayout);
            for (Map.Entry<Symbol, Integer> probeLayoutEntry : probeSourceLayout.entrySet()) {
                joinSourcesLayout.put(probeLayoutEntry.getKey(), probeLayoutEntry.getValue() + lookupSourceLayout.size());
            }
            return joinSourcesLayout.build();
        }

        @Override
        public PhysicalOperation visitSemiJoin(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = node.getSource().accept(this, context);

            // Plan build
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getFilteringSource().accept(this, buildContext);
            checkState(buildSource.getPipelineExecutionStrategy() == probeSource.getPipelineExecutionStrategy(), "build and probe have different pipelineExecutionStrategy");
            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);
            checkArgument(partitionCount == 1, "Expected local execution to not be parallel");

            int probeChannel = probeSource.getLayout().get(node.getSourceJoinSymbol());
            int buildChannel = buildSource.getLayout().get(node.getFilteringSourceJoinSymbol());

            ImmutableList.Builder<OperatorFactory> buildOperatorFactories = new ImmutableList.Builder<>();
            buildOperatorFactories.addAll(buildSource.getOperatorFactories());

            node.getDynamicFilterId().ifPresent(filterId -> {
                // Add a DynamicFilterSourceOperatorFactory to build operator factories
                log.debug("[Semi-join] Dynamic filter: %s", filterId);
                LocalDynamicFilter filterConsumer = createDynamicFilter(node, context).orElse(null);
                if (filterConsumer != null) {
                    addSuccessCallback(filterConsumer.getDynamicFilterResultFuture(), context::getDynamicFiltersCollector);
                    buildOperatorFactories.add(new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                            buildContext.getNextOperatorId(),
                            node.getId(),
                            filterConsumer.getValueConsumer(),
                            ImmutableList.of(new DynamicFilterSourceOperator.Channel(filterId, buildSource.getTypes().get(buildChannel), buildChannel, context.getSession().getQueryId().toString())),
                            getDynamicFilteringMaxPerDriverValueCount(context.getSession()),
                            getDynamicFilteringMaxPerDriverSize(context.getSession())));
                }
            });

            Optional<Integer> buildHashChannel = node.getFilteringSourceHashSymbol().map(channelGetter(buildSource));
            Optional<Integer> probeHashChannel = node.getSourceHashSymbol().map(channelGetter(probeSource));

            SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes().get(buildChannel),
                    buildChannel,
                    buildHashChannel,
                    10_000,
                    joinCompiler);
            buildOperatorFactories.add(setBuilderOperatorFactory);
            SetSupplier setProvider = setBuilderOperatorFactory.getSetProvider();
            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    buildOperatorFactories.build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            // Source channels are always laid out first, followed by the boolean output symbol
            Map<Symbol, Integer> outputMappings = ImmutableMap.<Symbol, Integer>builder()
                    .putAll(probeSource.getLayout())
                    .put(node.getSemiJoinOutput(), probeSource.getLayout().size())
                    .build();

            HashSemiJoinOperatorFactory operator = new HashSemiJoinOperatorFactory(context.getNextOperatorId(), node.getId(), setProvider, probeSource.getTypes(), probeChannel, probeHashChannel);
            return new PhysicalOperation(operator, outputMappings, context, probeSource);
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            if (node.getPartitioningScheme().isPresent()) {
                PartitioningHandle partitioningHandle = node.getPartitioningScheme().get().getPartitioning().getHandle();
                if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
                    context.setDriverInstanceCount(getTaskWriterCount(session));
                }
                else {
                    context.setDriverInstanceCount(1);
                }
            }
            else {
                context.setDriverInstanceCount(getTaskWriterCount(session));
            }

            // serialize writes by forcing data through a single writer
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            outputMapping.put(node.getOutputSymbols().get(0), ROW_COUNT_CHANNEL);
            outputMapping.put(node.getOutputSymbols().get(1), FRAGMENT_CHANNEL);

            OperatorFactory statisticsAggregation = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            PARTIAL,
                            STATS_START_CHANNEL,
                            outputMapping,
                            source,
                            context,
                            true);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        PARTIAL,
                        Optional.empty(),
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        new DataSize(0, BYTE),
                        context,
                        STATS_START_CHANNEL,
                        outputMapping,
                        200,
                        // This aggregation must behave as INTERMEDIATE.
                        // Using INTERMEDIATE aggregation directly
                        // is not possible, as it doesn't accept raw input data.
                        // Disabling partial pre-aggregation memory limit effectively
                        // turns PARTIAL aggregation into INTERMEDIATE.
                        Optional.empty(),
                        true);
            }).orElse(new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            List<Integer> inputChannels = node.getColumns().stream()
                    .map(source::symbolToChannel)
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSinkManager,
                    node.getTarget(),
                    inputChannels,
                    session,
                    statisticsAggregation,
                    getSymbolTypes(node.getOutputSymbols(), context.getTypes()),
                    Optional.of(context.getTaskId()));

            return new PhysicalOperation(operatorFactory, outputMapping.build(), context, source);
        }

        @Override
        public PhysicalOperation visitStatisticsWriterNode(StatisticsWriterNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            StatisticAggregationsDescriptor<Integer> descriptor = node.getDescriptor().map(symbol -> source.getLayout().get(symbol));

            OperatorFactory operatorFactory = new StatisticsWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    computedStatistics -> metadata.finishStatisticsCollection(session, ((StatisticsWriterNode.WriteStatisticsHandle) node.getTarget()).getHandle(), computedStatistics),
                    node.isRowCountEnabled(),
                    descriptor);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitTableFinish(TableFinishNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();

            OperatorFactory statisticsAggregation = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            FINAL,
                            0,
                            outputMapping,
                            source,
                            context,
                            true);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        FINAL,
                        Optional.empty(),
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        new DataSize(0, BYTE),
                        context,
                        0,
                        outputMapping,
                        200,
                        // final aggregation ignores partial pre-aggregation memory limit
                        Optional.empty(),
                        true);
            }).orElse(new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            Map<Symbol, Integer> aggregationOutput = outputMapping.build();
            StatisticAggregationsDescriptor<Integer> descriptor = node.getStatisticsAggregationDescriptor()
                    .map(desc -> desc.map(aggregationOutput::get))
                    .orElse(StatisticAggregationsDescriptor.empty());

            OperatorFactory operatorFactory = new TableFinishOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    createTableFinisher(session, node, metadata),
                    statisticsAggregation,
                    descriptor,
                    session);
            Map<Symbol, Integer> layout = ImmutableMap.of(node.getOutputSymbols().get(0), 0);

            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        @Override
        public PhysicalOperation visitCubeFinish(CubeFinishNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            OperatorFactory operatorFactory = new CubeFinishOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    session,
                    cubeManager,
                    node.getCubeName(),
                    node.getDataPredicate(),
                    node.isOverwrite());
            Map<Symbol, Integer> layout = ImmutableMap.of(node.getOutputSymbols().get(0), 0);
            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        @Override
        public PhysicalOperation visitDelete(DeleteNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new DeleteOperatorFactory(context.getNextOperatorId(), node.getId(), source.getLayout().get(node.getRowId()));

            Map<Symbol, Integer> layout = ImmutableMap.<Symbol, Integer>builder()
                    .put(node.getOutputSymbols().get(0), 0)
                    .put(node.getOutputSymbols().get(1), 1)
                    .build();

            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        @Override
        public PhysicalOperation visitTableDelete(TableDeleteNode node, LocalExecutionPlanContext context)
        {
            Optional<PhysicalOperation> source = Optional.empty();
            Optional<Map<Symbol, Integer>> sourceLayout = Optional.empty();
            if (node.getSource() != null) {
                source = Optional.of(node.getSource().accept(this, context));
                sourceLayout = Optional.of(source.get().getLayout());
            }
            OperatorFactory operatorFactory = new TableDeleteOperatorFactory(context.getNextOperatorId(), node.getId(), metadata, session, node.getTarget(),
                    sourceLayout, node.getFilter(), node.getAssignments(), context.getTypes());

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitUnion(UnionNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("Union node should not be present in a local execution plan");
        }

        @Override
        public PhysicalOperation visitEnforceSingleRow(EnforceSingleRowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new EnforceSingleRowOperator.EnforceSingleRowOperatorFactory(context.getNextOperatorId(), node.getId());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitAssignUniqueId(AssignUniqueId node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new AssignUniqueIdOperator.AssignUniqueIdOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getScope() == LOCAL, "Only local exchanges are supported in the local planner");

            if (node.getOrderingScheme().isPresent()) {
                return createLocalMerge(node, context);
            }

            return createLocalExchange(node, context);
        }

        private PhysicalOperation createLocalMerge(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");
            checkState(node.getSources().size() == 1, "single source is expected");

            // local merge source must have a single driver
            context.setDriverInstanceCount(1);

            PlanNode sourceNode = getOnlyElement(node.getSources());
            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = sourceNode.accept(this, subContext);

            int operatorsCount = subContext.getDriverInstanceCount().orElse(1);
            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            LocalExchangeFactory exchangeFactory = new LocalExchangeFactory(
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    operatorsCount,
                    types,
                    ImmutableList.of(),
                    Optional.empty(),
                    source.getPipelineExecutionStrategy(),
                    maxLocalExchangeBufferSize);

            List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());
            List<Symbol> expectedLayout = node.getInputs().get(0);
            Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(expectedLayout, source.getLayout());
            operatorFactories.add(new LocalExchangeSinkOperatorFactory(
                    exchangeFactory,
                    subContext.getNextOperatorId(),
                    node.getId(),
                    exchangeFactory.newSinkFactoryId(),
                    pagePreprocessor));
            context.addDriverFactory(subContext.isInputDriver(), false, operatorFactories, subContext.getDriverInstanceCount(), source.getPipelineExecutionStrategy());
            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), layout);
            List<SortOrder> orderings = orderingScheme.getOrderingList();
            OperatorFactory operatorFactory = new LocalMergeSourceOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    exchangeFactory,
                    types,
                    orderingCompiler,
                    sortChannels,
                    orderings);
            return new PhysicalOperation(operatorFactory, layout, context, UNGROUPED_EXECUTION);
        }

        private PhysicalOperation createLocalExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            int driverInstanceCount;
            if (node.getType() == ExchangeNode.Type.GATHER) {
                driverInstanceCount = 1;
                context.setDriverInstanceCount(1);
            }
            else if (context.getDriverInstanceCount().isPresent()) {
                driverInstanceCount = context.getDriverInstanceCount().getAsInt();
            }
            else {
                driverInstanceCount = getTaskConcurrency(session)/DriverInstanceCountFudgeFactor;
                context.setDriverInstanceCount(driverInstanceCount);
            }

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            List<Integer> channels = node.getPartitioningScheme().getPartitioning().getArguments().stream()
                    .map(argument -> node.getOutputSymbols().indexOf(argument.getColumn()))
                    .collect(toImmutableList());
            Optional<Integer> hashChannel = node.getPartitioningScheme().getHashColumn()
                    .map(symbol -> node.getOutputSymbols().indexOf(symbol));

            PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy = GROUPED_EXECUTION;
            List<DriverFactoryParameters> driverFactoryParametersList = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode sourceNode = node.getSources().get(i);

                LocalExecutionPlanContext subContext = context.createSubContext();
                PhysicalOperation source = sourceNode.accept(this, subContext);
                driverFactoryParametersList.add(new DriverFactoryParameters(subContext, source));

                if (source.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION) {
                    exchangeSourcePipelineExecutionStrategy = UNGROUPED_EXECUTION;
                }
            }

            LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    driverInstanceCount,
                    types,
                    channels,
                    hashChannel,
                    exchangeSourcePipelineExecutionStrategy,
                    maxLocalExchangeBufferSize);
            for (int i = 0; i < node.getSources().size(); i++) {
                DriverFactoryParameters driverFactoryParameters = driverFactoryParametersList.get(i);
                PhysicalOperation source = driverFactoryParameters.getSource();
                LocalExecutionPlanContext subContext = driverFactoryParameters.getSubContext();

                List<Symbol> expectedLayout = node.getInputs().get(i);
                Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(expectedLayout, source.getLayout());
                List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());

                operatorFactories.add(new LocalExchangeSinkOperatorFactory(
                        localExchangeFactory,
                        subContext.getNextOperatorId(),
                        node.getId(),
                        localExchangeFactory.newSinkFactoryId(),
                        pagePreprocessor));
                context.addDriverFactory(
                        subContext.isInputDriver(),
                        false,
                        operatorFactories,
                        subContext.getDriverInstanceCount(),
                        exchangeSourcePipelineExecutionStrategy);
            }

            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            // instance count must match the number of partitions in the exchange
            verify(context.getDriverInstanceCount().getAsInt() == localExchangeFactory.getBufferCount(),
                    "driver instance count must match the number of exchange partitions");

            return new PhysicalOperation(new LocalExchangeSourceOperatorFactory(context.getNextOperatorId(), node.getId(), localExchangeFactory), makeLayout(node), context, exchangeSourcePipelineExecutionStrategy);
        }

        @Override
        public PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private List<Type> getSourceOperatorTypes(PlanNode node, TypeProvider types)
        {
            return getSymbolTypes(node.getOutputSymbols(), types);
        }

        private List<Type> getSymbolTypes(List<Symbol> symbols, TypeProvider types)
        {
            return symbols.stream()
                    .map(types::get)
                    .collect(toImmutableList());
        }

        private AccumulatorFactory buildAccumulatorFactory(
                PhysicalOperation source,
                Aggregation aggregation)
        {
            InternalAggregationFunction internalAggregationFunction = metadata.getAggregateFunctionImplementation(aggregation.getSignature());

            List<Integer> valueChannels = new ArrayList<>();
            for (RowExpression argument : aggregation.getArguments()) {
                if (!(argument instanceof LambdaDefinitionExpression)) {
                    checkArgument(argument instanceof VariableReferenceExpression, "argument must be variable reference");
                    valueChannels.add(source.getLayout().get(new Symbol(((VariableReferenceExpression) argument).getName())));
                }
            }

            List<LambdaProvider> lambdaProviders = new ArrayList<>();
            List<LambdaDefinitionExpression> lambdas = aggregation.getArguments().stream()
                    .filter(LambdaDefinitionExpression.class::isInstance)
                    .map(LambdaDefinitionExpression.class::cast)
                    .collect(toImmutableList());
            for (int i = 0; i < lambdas.size(); i++) {
                List<Class<?>> lambdaInterfaces = internalAggregationFunction.getLambdaInterfaces();
                Class<? extends LambdaProvider> lambdaProviderClass = compileLambdaProvider(lambdas.get(i), metadata, lambdaInterfaces.get(i));
                try {
                    lambdaProviders.add((LambdaProvider) constructorMethodHandle(lambdaProviderClass, ConnectorSession.class).invoke(session.toConnectorSession()));
                }
                catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            }

            Optional<Integer> maskChannel = aggregation.getMask().map(value -> source.getLayout().get(value));
            List<SortOrder> sortOrders = ImmutableList.of();
            List<Symbol> sortKeys = ImmutableList.of();
            if (aggregation.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = aggregation.getOrderingScheme().get();
                sortKeys = orderingScheme.getOrderBy();
                sortOrders = sortKeys.stream()
                        .map(orderingScheme::getOrdering)
                        .collect(toImmutableList());
            }

            return internalAggregationFunction.bind(
                    valueChannels,
                    maskChannel,
                    source.getTypes(),
                    getChannelsForSymbols(sortKeys, source.getLayout()),
                    sortOrders,
                    pagesIndexFactory,
                    aggregation.isDistinct(),
                    joinCompiler,
                    lambdaProviders,
                    session);
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source, LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            AggregationOperatorFactory operatorFactory = createAggregationOperatorFactory(
                    node.getId(),
                    node.getAggregations(),
                    node.getStep(),
                    0,
                    outputMappings,
                    source,
                    context,
                    node.getStep().isOutputPartial());
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        private AggregationOperatorFactory createAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<Symbol, Aggregation> aggregations,
                Step step,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                PhysicalOperation source,
                LocalExecutionPlanContext context,
                boolean useSystemMemory)
        {
            int outputChannel = startOutputChannel;
            ImmutableList.Builder<AccumulatorFactory> accumulatorFactories = ImmutableList.builder();
            for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                accumulatorFactories.add(buildAccumulatorFactory(source, aggregation));
                outputMappings.put(symbol, outputChannel); // one aggregation per channel
                outputChannel++;
            }
            return new AggregationOperatorFactory(context.getNextOperatorId(), planNodeId, step, accumulatorFactories.build(), useSystemMemory);
        }

        private PhysicalOperation planGroupByAggregation(
                AggregationNode node,
                PhysicalOperation source,
                boolean spillEnabled,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> mappings = ImmutableMap.builder();
            OperatorFactory operatorFactory = createHashAggregationOperatorFactory(
                    node.getId(),
                    node.getAggregations(),
                    node.getGlobalGroupingSets(),
                    node.getGroupingKeys(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol(),
                    source,
                    node.hasDefaultOutput(),
                    spillEnabled,
                    node.isStreamable(),
                    unspillMemoryLimit,
                    context,
                    0,
                    mappings,
                    10_000,
                    Optional.of(maxPartialAggregationMemorySize),
                    node.getStep().isOutputPartial());
            return new PhysicalOperation(operatorFactory, mappings.build(), context, source);
        }

        private OperatorFactory createHashAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<Symbol, Aggregation> aggregations,
                Set<Integer> globalGroupingSets,
                List<Symbol> groupBySymbols,
                Step step,
                Optional<Symbol> hashSymbol,
                Optional<Symbol> groupIdSymbol,
                PhysicalOperation source,
                boolean hasDefaultOutput,
                boolean spillEnabled,
                boolean isStreamable,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                int expectedGroups,
                Optional<DataSize> maxPartialAggregationMemorySize,
                boolean useSystemMemory)
        {
            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
            for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();

                accumulatorFactories.add(buildAccumulatorFactory(source, aggregation));
                aggregationOutputSymbols.add(symbol);
            }

            // add group-by key fields each in a separate channel
            int channel = startOutputChannel;
            Optional<Integer> groupIdChannel = Optional.empty();
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, channel);
                if (groupIdSymbol.isPresent() && groupIdSymbol.get().equals(symbol)) {
                    groupIdChannel = Optional.of(channel);
                }
                channel++;
            }

            // hashChannel follows the group by channels
            if (hashSymbol.isPresent()) {
                outputMappings.put(hashSymbol.get(), channel++);
            }

            // aggregations go in following channels
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            List<Integer> groupByChannels = getChannelsForSymbols(groupBySymbols, source.getLayout());
            List<Type> groupByTypes = groupByChannels.stream()
                    .map(entry -> source.getTypes().get(entry))
                    .collect(toImmutableList());

            if (isStreamable) {
                return new StreamingAggregationOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        source.getTypes(),
                        groupByTypes,
                        groupByChannels,
                        step,
                        accumulatorFactories,
                        joinCompiler);
            }
            else {
                Optional<Integer> hashChannel = hashSymbol.map(channelGetter(source));
                return new HashAggregationOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        groupByTypes,
                        groupByChannels,
                        ImmutableList.copyOf(globalGroupingSets),
                        step,
                        hasDefaultOutput,
                        accumulatorFactories,
                        hashChannel,
                        groupIdChannel,
                        expectedGroups,
                        maxPartialAggregationMemorySize,
                        spillEnabled,
                        unspillMemoryLimit,
                        spillerFactory,
                        joinCompiler,
                        useSystemMemory);
            }
        }
    }

    private static List<Type> getTypes(List<Expression> expressions, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        return expressions.stream()
                .map(NodeRef::of)
                .map(expressionTypes::get)
                .collect(toImmutableList());
    }

    private static TableFinisher createTableFinisher(Session session, TableFinishNode node, Metadata metadata)
    {
        WriterTarget target = node.getTarget();
        return (fragments, statistics) -> {
            if (target instanceof CreateTarget) {
                return metadata.finishCreateTable(session, ((CreateTarget) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof InsertTarget) {
                return metadata.finishInsert(session, ((InsertTarget) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof UpdateTarget) {
                return metadata.finishUpdate(session, ((UpdateTarget) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof VacuumTarget) {
                return metadata.finishVacuum(session, ((VacuumTarget) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof DeleteTarget) {
                metadata.finishDelete(session, ((DeleteTarget) target).getHandle(), fragments);
                return Optional.empty();
            }
            else if (target instanceof DeleteAsInsertTarget) {
                metadata.finishDeleteAsInsert(session, ((DeleteAsInsertTarget) target).getHandle(), fragments, statistics);
                return Optional.empty();
            }
            else {
                throw new AssertionError("Unhandled target type: " + target.getClass().getName());
            }
        };
    }

    private static Function<Page, Page> enforceLayoutProcessor(List<Symbol> expectedLayout, Map<Symbol, Integer> inputLayout)
    {

        int[] channels = expectedLayout.stream()
                .peek(symbol -> checkArgument(inputLayout.containsKey(symbol), "channel not found for symbol: %s", symbol))
                .mapToInt(inputLayout::get)
                .toArray();

        if (Arrays.equals(channels, range(0, inputLayout.size()).toArray())) {
            // this is an identity mapping
            return Function.identity();
        }

        return new PageChannelSelector(channels);
    }

    private static List<Integer> getChannelsForSymbols(List<Symbol> symbols, Map<Symbol, Integer> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : symbols) {
            builder.add(layout.get(symbol));
        }
        return builder.build();
    }

    private static Function<Symbol, Integer> channelGetter(PhysicalOperation source)
    {
        return input -> {
            checkArgument(source.getLayout().containsKey(input));
            return source.getLayout().get(input);
        };
    }

    /**
     * Encapsulates an physical operator plus the mapping of logical symbols to channel/field
     */
    private static class PhysicalOperation
    {
        private final List<OperatorFactory> operatorFactories;
        private final Map<Symbol, Integer> layout;
        private final List<Type> types;

        private final PipelineExecutionStrategy pipelineExecutionStrategy;

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, LocalExecutionPlanContext context, PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            this(operatorFactory, layout, context, Optional.empty(), pipelineExecutionStrategy);
        }

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, LocalExecutionPlanContext context, PhysicalOperation source)
        {
            this(operatorFactory, layout, context, Optional.of(requireNonNull(source, "source is null")), source.getPipelineExecutionStrategy());
        }

        private PhysicalOperation(
                OperatorFactory operatorFactory,
                Map<Symbol, Integer> layout,
                LocalExecutionPlanContext context,
                Optional<PhysicalOperation> source,
                PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            requireNonNull(operatorFactory, "operatorFactory is null");
            requireNonNull(layout, "layout is null");
            requireNonNull(context, "context is null");
            requireNonNull(source, "source is null");
            requireNonNull(pipelineExecutionStrategy, "pipelineExecutionStrategy is null");

            this.operatorFactories = ImmutableList.<OperatorFactory>builder()
                    .addAll(source.map(PhysicalOperation::getOperatorFactories).orElse(ImmutableList.of()))
                    .add(operatorFactory)
                    .build();
            this.layout = ImmutableMap.copyOf(layout);
            this.types = toTypes(layout, context);
            this.pipelineExecutionStrategy = pipelineExecutionStrategy;
        }

        private PhysicalOperation(
                List<OperatorFactory> operatorFactory,
                Map<Symbol, Integer> layout,
                LocalExecutionPlanContext context,
                Optional<PhysicalOperation> source,
                PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            requireNonNull(operatorFactory, "operatorFactory is null");
            requireNonNull(layout, "layout is null");
            requireNonNull(context, "context is null");
            requireNonNull(source, "source is null");
            requireNonNull(pipelineExecutionStrategy, "pipelineExecutionStrategy is null");

            List<OperatorFactory> temp1 = new ArrayList<>();
            List<OperatorFactory> temp3 = new ArrayList<>();

            temp1 = ImmutableList.<OperatorFactory>builder()
                    .addAll(source.map(PhysicalOperation::getOperatorFactories).orElse(ImmutableList.of()))
                    .add(operatorFactory.get(0))
                    .build();

            List<OperatorFactory> temp2 = new ArrayList<>();

            temp2 = ImmutableList.<OperatorFactory>builder()
                    .addAll(source.map(PhysicalOperation::getOperatorFactories).orElse(ImmutableList.of()))
                    .add(operatorFactory.get(1))
                    .build();

            for (OperatorFactory opFac: temp1)
                temp3.add(opFac);
            for (OperatorFactory opFac: temp2)
                temp3.add(opFac);
            this.operatorFactories = ImmutableList.copyOf(temp3);
            this.layout = ImmutableMap.copyOf(layout);
            this.types = toTypes(layout, context);
            this.pipelineExecutionStrategy = pipelineExecutionStrategy;
        }

        private static List<Type> toTypes(Map<Symbol, Integer> layout, LocalExecutionPlanContext context)
        {
            // verify layout covers all values
            int channelCount = layout.values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
            checkArgument(
                    layout.size() == channelCount && ImmutableSet.copyOf(layout.values()).containsAll(ContiguousSet.create(closedOpen(0, channelCount), integers())),
                    "Layout does not have a symbol for every output channel: %s", layout);
            Map<Integer, Symbol> channelLayout = ImmutableBiMap.copyOf(layout).inverse();

            return range(0, channelCount)
                    .mapToObj(channelLayout::get)
                    .map(context.getTypes()::get)
                    .collect(toImmutableList());
        }

        public int symbolToChannel(Symbol input)
        {
            checkArgument(layout.containsKey(input));
            return layout.get(input);
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public Map<Symbol, Integer> getLayout()
        {
            return layout;
        }

        private List<OperatorFactory> getOperatorFactories()
        {
            return operatorFactories;
        }

        public PipelineExecutionStrategy getPipelineExecutionStrategy()
        {
            return pipelineExecutionStrategy;
        }
    }

    private static class DriverFactoryParameters
    {
        private final LocalExecutionPlanContext subContext;
        private final PhysicalOperation source;

        public DriverFactoryParameters(LocalExecutionPlanContext subContext, PhysicalOperation source)
        {
            this.subContext = subContext;
            this.source = source;
        }

        public LocalExecutionPlanContext getSubContext()
        {
            return subContext;
        }

        public PhysicalOperation getSource()
        {
            return source;
        }
    }
}
