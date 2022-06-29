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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cost.*;
import io.prestosql.cube.CubeManager;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.execution.scheduler.ExecutionPolicy;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.SplitSchedulerStats;
import io.prestosql.execution.scheduler.SqlQueryScheduler;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.failuredetector.FailureDetector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.memory.VersionedMemoryPoolId;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.ForScheduler;
import io.prestosql.query.CachedSqlQueryExecution;
import io.prestosql.query.CachedSqlQueryExecutionPlan;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.*;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.type.Type;
import io.prestosql.split.SplitManager;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.TreePrinter;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.DistributedExecutionPlanner;
import io.prestosql.sql.planner.InputExtractor;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.PlanFragmenter;
import io.prestosql.sql.planner.PlanOptimizers;
import io.prestosql.sql.planner.StageExecutionPlan;
import io.prestosql.sql.planner.SubPlan;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.caching.CacheWorkloadProfiler;
import io.prestosql.sql.planner.optimizations.*;
import io.prestosql.sql.planner.*;
import io.prestosql.sql.planner.datapath.globalplanner.GlobalPlanPrinter;
import io.prestosql.sql.planner.datapath.globalplanner.GlobalPlaner;
import io.prestosql.sql.planner.datapath.globalplanner.JoinCostCalculator;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.sql.planner.datapath.globalplanner.JoinInformationExtractor;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.planprinter.PlanPrinter.textDistributedPlan;


import io.prestosql.sql.planner.plan.*;
import io.prestosql.sql.planner.planprinter.PlanPrinter;
import io.prestosql.sql.relational.Expressions;
import io.prestosql.sql.tree.Except;
import io.prestosql.sql.tree.Explain;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.HetuConfig;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;


import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.SystemSessionProperties.isCrossRegionDynamicFilterEnabled;
import static io.prestosql.SystemSessionProperties.isEnableDynamicFiltering;
import static io.prestosql.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.execution.scheduler.SqlQueryScheduler.createSqlQueryScheduler;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTERS;
import static io.prestosql.statestore.StateStoreConstants.QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static jdk.nashorn.internal.objects.NativeFunction.call;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    static public AtomicInteger currentlyOptimized = new AtomicInteger(0);
    static public AtomicInteger allQueryCount = new AtomicInteger(0);

    static public Plan lowCostQuery;
    static public AtomicInteger overlapOrderCount = new AtomicInteger(0);
    static public AtomicInteger currentBatchSize = new AtomicInteger(0);
    static Map<Integer, Plan> topologicalOrderPlanList = new HashMap<>();
    static boolean planAggregationPhase = true;



    //static long cummulativePlanningTime = 0;
    //static  int cumulativeOperatorCount  =0;
    //static ArrayList<Long> cumulativePlanTimeList = new ArrayList<Long>();
    //static long DPPlanningTime = 0;
    //static double localPlansCummulativeCost = 0;
    static Map<Integer, List<Long>> numberOfJoinsToRunningTimeMapping = new HashMap<Integer, List<Long>>();
    static Map<Integer, JoinInformationExtractor.JoinInformation> JoinGraphMap = new HashMap<Integer, JoinInformationExtractor.JoinInformation>();
    static List<PlanNode> logicalPlanList = new ArrayList<>();
    static List<QueryId> queryIdList = new ArrayList<>();
    static PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    static Map<Symbol, Type> typesCollector = new HashMap<>();
    static Map<PlanNodeId, PlanNodeStatsEstimate> statsCollector = new HashMap<>();
    static Map<PlanNodeId, PlanCostEstimate> costsCollector = new HashMap<>();
    static PlanSymbolAllocator planSymbolAllocator = new PlanSymbolAllocator();

    static GlobalPlaner globalPlaner = new GlobalPlaner();

    static CacheWorkloadProfiler cacheWorkloadProfiler = new CacheWorkloadProfiler(2000000.0);

    public static boolean forceJoinOrder = false;

    private static final Logger log = Logger.get(SqlQueryExecution.class);

    private static final OutputBufferId OUTPUT_BUFFER_ID = new OutputBufferId(0);

    private final QueryStateMachine stateMachine;
    private final String slug;
    private final Metadata metadata;
    private final CubeManager cubeManager;
    private final SqlParser sqlParser;
    private final SplitManager splitManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
    private final List<PlanOptimizer> planOptimizers;
    private final PlanFragmenter planFragmenter;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final int scheduleSplitBatchSize;
    private final ExecutorService queryExecutor;
    private final ScheduledExecutorService schedulerExecutor;
    private final FailureDetector failureDetector;

    private final AtomicReference<SqlQueryScheduler> queryScheduler = new AtomicReference<>();
    private final AtomicReference<Plan> queryPlan = new AtomicReference<>();
    private final NodeTaskMap nodeTaskMap;
    private final ExecutionPolicy executionPolicy;
    private final SplitSchedulerStats schedulerStats;
    private final Analysis analysis;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final DynamicFilterService dynamicFilterService;
    private final HeuristicIndexerManager heuristicIndexerManager;
    private final StateStoreProvider stateStoreProvider;

    public SqlQueryExecution(
            PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            String slug,
            Metadata metadata,
            CubeManager cubeManager,
            AccessControl accessControl,
            SqlParser sqlParser,
            SplitManager splitManager,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            List<PlanOptimizer> planOptimizers,
            PlanFragmenter planFragmenter,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            int scheduleSplitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            QueryExplainer queryExplainer,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector,
            DynamicFilterService dynamicFilterService,
            HeuristicIndexerManager heuristicIndexerManager,
            StateStoreProvider stateStoreProvider)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            this.slug = requireNonNull(slug, "slug is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");

            checkArgument(scheduleSplitBatchSize > 0, "scheduleSplitBatchSize must be greater than 0");
            this.scheduleSplitBatchSize = scheduleSplitBatchSize;

            this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
            this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");

            // clear dynamic filter tasks and data created for this query
            stateMachine.addStateChangeListener(state -> {
                if (isEnableDynamicFiltering(stateMachine.getSession()) && state.isDone()) {
                    dynamicFilterService.clearDynamicFiltersForQuery(stateMachine.getQueryId().getId());
                }
            });

            // analyze query
            requireNonNull(preparedQuery, "preparedQuery is null");
            Analyzer analyzer = new Analyzer(
                    stateMachine.getSession(),
                    metadata,
                    sqlParser,
                    accessControl,
                    Optional.of(queryExplainer),
                    preparedQuery.getParameters(),
                    warningCollector,
                    heuristicIndexerManager,
                    cubeManager);
            this.analysis = analyzer.analyze(preparedQuery.getStatement());

            stateMachine.setUpdateType(analysis.getUpdateType());

            // when the query finishes cache the final query info, and clear the reference to the output stage
            AtomicReference<SqlQueryScheduler> queryScheduler = this.queryScheduler;
            stateMachine.addStateChangeListener(state -> {
                //Set the AsyncRunning flag if query is capable of running async
                if (analysis.isAsyncQuery() && state == QueryState.RUNNING) {
                    stateMachine.setRunningAsync(true);
                }

                if (!state.isDone()) {
                    return;
                }

                // query is now done, so abort any work that is still running
                SqlQueryScheduler scheduler = queryScheduler.get();
                if (scheduler != null) {
                    scheduler.abort();
                }
            });

            this.remoteTaskFactory = new MemoryTrackingRemoteTaskFactory(requireNonNull(remoteTaskFactory, "remoteTaskFactory is null"), stateMachine);
        }
    }

    @Override
    public String getSlug()
    {
        return slug;
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getUserMemoryReservation();
        }
        if (scheduler == null) {
            return new DataSize(0, BYTE);
        }
        return succinctBytes(scheduler.getUserMemoryReservation());
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalMemoryReservation();
        }
        if (scheduler == null) {
            return new DataSize(0, BYTE);
        }
        return succinctBytes(scheduler.getTotalMemoryReservation());
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalCpuTime();
        }
        if (scheduler == null) {
            return new Duration(0, SECONDS);
        }
        return scheduler.getTotalCpuTime();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getFinalQueryInfo()
                .map(BasicQueryInfo::new)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.ofNullable(queryScheduler.get()).map(SqlQueryScheduler::getBasicStageStats)));
    }

    private void findMappingFromPlan(Map<String, Set<String>> mapping, PlanNode sourceNode)
    {
        if (sourceNode != null && sourceNode instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) sourceNode;
            Map<Symbol, RowExpression> assignments = projectNode.getAssignments().getMap();
            for (Symbol symbol : assignments.keySet()) {
                if (mapping.containsKey(symbol.getName())) {
                    Set<String> sets = mapping.get(symbol.getName());
                    RowExpression expression = assignments.get(symbol);
                    if (expression instanceof VariableReferenceExpression) {
                        sets.add(((VariableReferenceExpression) expression).getName());
                    }
                    else {
                        sets.add(expression.toString());
                    }
                }
                else {
                    for (Map.Entry<String, Set<String>> entry : mapping.entrySet()) {
                        if (entry.getValue().contains(symbol.getName())) {
                            RowExpression expression = assignments.get(symbol);
                            if (expression instanceof VariableReferenceExpression) {
                                entry.getValue().add(((VariableReferenceExpression) expression).getName());
                            }
                            else {
                                entry.getValue().add(expression.toString());
                            }
                        }
                    }
                }
            }
        }

        for (PlanNode planNode : sourceNode.getSources()) {
            findMappingFromPlan(mapping, planNode);
        }
    }

    private void handleCrossRegionDynamicFilter(PlanRoot plan)
    {
        if (!isCrossRegionDynamicFilterEnabled(getSession()) || plan == null) {
            return;
        }

        StateStore stateStore = stateStoreProvider.getStateStore();
        if (stateStore == null) {
            return;
        }

        String queryId = getSession().getQueryId().getId();
        log.debug("queryId=%s begin to find columnToColumnMapping.", queryId);
        PlanNode outputNode = plan.getRoot().getFragment().getRoot();
        Map<String, Set<String>> columnToSymbolMapping = new HashMap<>();

        if (outputNode != null && outputNode instanceof OutputNode) {
            List<String> queryColumnNames = ((OutputNode) outputNode).getColumnNames();
            List<Symbol> outputSymbols = outputNode.getOutputSymbols();

            Map<String, Set<String>> tmpMapping = new HashMap<>(outputSymbols.size());
            for (Symbol symbol : outputNode.getOutputSymbols()) {
                Set<String> sets = new HashSet();
                sets.add(symbol.getName());
                tmpMapping.put(symbol.getName(), sets);
            }

            for (PlanFragment fragment : plan.getRoot().getAllFragments()) {
                if ("0".equals(fragment.getId().toString())) {
                    continue;
                }

                PlanNode sourceNode = fragment.getRoot();
                findMappingFromPlan(tmpMapping, sourceNode);
            }

            for (int i = 0; i < outputSymbols.size(); i++) {
                columnToSymbolMapping.put(queryColumnNames.get(i), tmpMapping.get(outputSymbols.get(i).getName()));
            }
        }

        // save mapping into stateStore
        StateMap<String, Object> mappingStateMap = (StateMap<String, Object>) stateStore.getOrCreateStateCollection(CROSS_REGION_DYNAMIC_FILTERS, StateCollection.Type.MAP);
        mappingStateMap.put(queryId + QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING, columnToSymbolMapping);
        log.debug("queryId=%s, add columnToSymbolMapping into hazelcast success.", queryId + QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING);
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            try {
                // transition to planning
                if (!stateMachine.transitionToPlanning()) {
                    // query already started or finished
                    return;
                }

                // analyze query
                PlanRoot plan = analyzeQuery();

                try {
                    handleCrossRegionDynamicFilter(plan);
                }
                catch (Throwable e) {
                    // ignore any exception
                    log.warn("something unexpected happened.. cause: %s", e.getMessage());
                }

                //System.out.println("before distribution");
                // plan distribution of query
                planDistribution(plan);
                //System.out.println("after distribution");

                // transition to starting
                if (!stateMachine.transitionToStarting()) {
                    // query already started or finished
                    return;
                }

                //System.out.println("DEPLOY");

                // if query is not finished, start the scheduler, otherwise cancel it
                SqlQueryScheduler scheduler = queryScheduler.get();

                if (!stateMachine.isDone()) {
                    scheduler.start();
                }
            }
            catch (Throwable e) {
                fail(e);
                throwIfInstanceOf(e, Error.class);
            }
        }
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            stateMachine.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    public static boolean readFilterInfoValue(){

    try {
        //FileInputStream fstream = new FileInputStream("/scratch/venkates/openLookEng-working/joinForcing.txt");
        //System.out.println("Working Directory = " + System.getProperty("user.dir"));
        FileInputStream fstream = new FileInputStream("./filterInfo.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
        String strLine;
        int value = 0;
        while ((strLine = br.readLine()) != null) {
            value = Integer.parseInt(strLine);
        }
        // Close the input stream
        br.close();
        fstream.close();

        if(value == 1){
            //System.out.println("VSK: Filter Info is true ");
            return  true;
        }
        else {
            //System.out.println("VSK: Filter Info is false");
            return false;
        }

    } catch (Exception e) {// Catch exception if any
        System.err.println("Error: " + e.getMessage());
    }
    //System.out.println("VSK: It should not come here");
    return false;
}


    private PlanRoot analyzeQuery()
    {
        try {
            return doAnalyzeQuery();
        }
        catch (StackOverflowError e) {
            throw new PrestoException(NOT_SUPPORTED, "statement is too large (stack overflow during analysis)");
        }
    }

    private void resetVariables(){
//        queryCount = 0;   //This needs to be taken care //TODO
//        allQueryCount = 0;
        logicalPlanList = new ArrayList<PlanNode>();

//        cummulativePlanningTime = 0;
//        cumulativeOperatorCount  =0;
//        cumulativePlanTimeList = new ArrayList<Long>();
//        DPPlanningTime = 0;
//        localPlansCummulativeCost = 0;
        numberOfJoinsToRunningTimeMapping = new HashMap<Integer, List<Long>>();
        JoinGraphMap = new HashMap<Integer, JoinInformationExtractor.JoinInformation>();
        logicalPlanList = new ArrayList<>();
        queryIdList = new ArrayList<>();
        idAllocator = new PlanNodeIdAllocator();
        typesCollector = new HashMap<>();
        statsCollector = new HashMap<>();
        costsCollector = new HashMap<>();
        planSymbolAllocator = new PlanSymbolAllocator();
    }

    private PlanNode preparePlanForBatching (Plan plan, Integer allQueryCountLocal)
    {
        for (Map.Entry<Symbol, Type> entry : plan.getTypes().allTypes().entrySet()) {
            typesCollector.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<PlanNodeId, PlanNodeStatsEstimate> entry : plan.getStatsAndCosts().getStats().entrySet()) {
            statsCollector.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<PlanNodeId, PlanCostEstimate> entry : plan.getStatsAndCosts().getCosts().entrySet()) {
            costsCollector.put(entry.getKey(), entry.getValue());
        }

        //String plantxt_local = PlanPrinter.textLogicalPlan(plan.getRoot(), new TypeProvider(typesCollector), metadata, new StatsAndCosts(statsCollector, costsCollector), stateMachine.getSession(), 0, true);
        //System.out.println(plantxt_local);

        Integer queryId = new Integer(allQueryCountLocal);
        PlanNode strippedPlan = plan.getRoot().getSources().get(0);
        strippedPlan = new StoreForwardNode(idAllocator.getNextId(), strippedPlan, "/home/root1/openLookEng/EPFL-BQO/bqo-to-huawei/output/out-" + queryId.toString() + "-", strippedPlan.getOutputSymbols());
        Symbol firstSymbol = strippedPlan.getOutputSymbols().get(0);

        Map<Symbol, AggregationNode.Aggregation> aggCalls = new HashMap<>();
        List<RowExpression> exprList = ImmutableList.of(new VariableReferenceExpression(firstSymbol.getName(), typesCollector.get(firstSymbol)));
        Optional<Symbol> x = Optional.empty();
        Optional<OrderingScheme> y = Optional.empty();
        Optional<Symbol> z = Optional.empty();

        AggregationNode.Aggregation aggr = new AggregationNode.Aggregation(
                new Signature("count", AGGREGATE, BIGINT.getTypeSignature(), typesCollector.get(firstSymbol).getTypeSignature()),
                exprList,
                false,
                x,
                y,
                z);

        Symbol aggrSymbol = planSymbolAllocator.newSymbol("count_sym", BIGINT);
        typesCollector.put(aggrSymbol, BIGINT);
        aggCalls.put(aggrSymbol, aggr);

        AggregationNode.GroupingSetDescriptor gsd = AggregationNode.singleGroupingSet(ImmutableList.of());

        PlanNode metaNode = new AggregationNode(idAllocator.getNextId(), strippedPlan, aggCalls, gsd, new ArrayList<>(), AggregationNode.Step.SINGLE, Optional.empty(), Optional.empty());

        PlanNode gather = new ExchangeNode(
                idAllocator.getNextId(),
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.REMOTE,
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), metaNode.getOutputSymbols()),
                ImmutableList.of(metaNode),
                ImmutableList.of(metaNode.getOutputSymbols()),
                Optional.empty());

        PlanNode gather2 = new ExchangeNode(
                idAllocator.getNextId(),
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.LOCAL,
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), gather.getOutputSymbols()),
                ImmutableList.of(gather),
                ImmutableList.of(gather.getOutputSymbols()),
                Optional.empty());

        Symbol firstSymbol2 = gather2.getOutputSymbols().get(0);

        Map<Symbol, AggregationNode.Aggregation> aggCalls2 = new HashMap<>();
        List<RowExpression> exprList2 = ImmutableList.of(new VariableReferenceExpression(firstSymbol2.getName(), BIGINT));
        Optional<Symbol> x2 = Optional.empty();
        Optional<OrderingScheme> y2 = Optional.empty();
        Optional<Symbol> z2 = Optional.empty();

        AggregationNode.Aggregation aggr2 = new AggregationNode.Aggregation(
                new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()),
                exprList2,
                false,
                x2,
                y2,
                z2);
        aggCalls2.put(firstSymbol2, aggr2);

        PlanNode metaNode2 = new AggregationNode(idAllocator.getNextId(), gather2, aggCalls2, gsd, new ArrayList<>(), AggregationNode.Step.SINGLE, Optional.empty(), Optional.empty());

        return metaNode2;
    }

    private PlanRoot doAnalyzeQuery()
    {
        // time analysis phase
        stateMachine.beginAnalysis();

        int allQueryCountLocal = allQueryCount.incrementAndGet();

        // plan query
        //this is to communicate with ReorderJoin that it should not consider a forced join order
//        try {
//            Files.write(Paths.get("./joinForcing.txt"), new String("0").getBytes());
//        } catch (IOException ex) {
//            ex.printStackTrace();
//        }

        double cpu_const = 1;
        double memory_const = 1;

        //forceJoinOrder = false; //TODO
//        if(readFilterInfoValue()){
//            aggregateFilterInfo.clear();
//        }

        //System.out.println("before 1st Create plan call");

        Plan plan = null;

        try {
            //PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            plan = createPlan(analysis, stateMachine.getSession(), planOptimizers, idAllocator, metadata, new TypeAnalyzer(sqlParser, metadata), statsCalculator, costCalculator, stateMachine.getWarningCollector());
            String plantxt2 = PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata, plan.getStatsAndCosts(), stateMachine.getSession(), 0, true);
            System.out.println(plantxt2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //System.out.println("after 1st Create plan call");
        FeaturesConfig config = new FeaturesConfig();

        PlanCostEstimate old_cost = plan.getStatsAndCosts().getCosts().get(plan.getRoot().getId());
        double full_old_cost = old_cost.getCpuCost() * config.getCpuCostWeight() + old_cost.getMaxMemory()*config.getMemoryCostWeight() + old_cost.getNetworkCost()*config.getNetworkCostWeight();

        //System.out.println("Post-plan");

        Map<Symbol, Type> types = plan.getTypes().allTypes();

        CaptureLineage captureLineage = new CaptureLineage(plan.getStatsAndCosts(), plan.getTypes());
        CaptureLineage.Lineage captureLineageResult;
        try {
            captureLineageResult = captureLineage.visitPlan(plan.getRoot(), null);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        //System.out.println("Lineage Mapping String");
        //Map<PlanNodeId, CaptureLineage.Lineage> LineageMapping = captureLineage.getLineageMapping();
        //for (PlanNodeId Key: LineageMapping.keySet()){
            //System.out.println("Key = "+Key.toString()+" value = "+LineageMapping.get(Key).toString());
            //if (LineageMapping.get(Key).getPlanNodeStatsEstimate() != null) System.out.println(LineageMapping.get(Key).getPlanNodeStatsEstimate());
            //System.out.println();
        //}

        //System.out.println(captureLineageResult.toString());

        //System.out.println("PLAN PRODUCED");

        //start of global Plan
        // plan query
        long startTime =0, stopTime=0;
        long newQueryPlanningTime =0;

        newQueryPlanningTime += 0;
        //System.out.println("\n" + "HERE YOU CAN ACCESS THE LOGICAL PLAN");
        //System.out.println("\n" + io.prestosql.sql.planner.planprinter.PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata, plan.getStatsAndCosts(), stateMachine.getSession(), 0, true));
        //System.out.println("\n CPU CCOST" + plan.getStatsAndCosts().getStats());


        //running time calculations

        //queryCount++;

        //actualQueryCount = queryCount-3;
        //System.out.println("QUERY COUNT: " + queryCount + " ActualQuery Count: " + actualQueryCount);

        if (allQueryCountLocal == 1) {
            lowCostQuery = plan;
        }

        /*if (allQueryCountLocal > 1 && plan.getRoot().getSources().size() == 1 && plan.getRoot().getSources().get(0) instanceof ValuesNode) {
            //System.out.println(plan.getRoot().getSources().get(0).toString());
            synchronized (this.getClass()){
                planAggregationPhase = false;
                System.out.println("Current batch size "+currentBatchSize.intValue());

                //reset topological variables
                topologicalOrderPlanList = new HashMap<>();;
                overlapOrderCount.getAndSet(0);
                JoinGraphMap = new HashMap<>();;
            }
        }*/

        if (SystemSessionProperties.isBatchEnabled(getSession()) && allQueryCountLocal > 1/*  &&  !planAggregationPhase*/) {
            cacheWorkloadProfiler.updateRunningViews(getSession(), metadata);
            cacheWorkloadProfiler.updateEvictions(getSession(), metadata);

            System.out.println("With cache: ");

            List<CaptureLineage.Lineage> subexpressionCache;

            if (SystemSessionProperties.isCachingPartioningEnabled(getSession()))
                subexpressionCache = cacheWorkloadProfiler.getAvailableViews();
            else
                subexpressionCache = new ArrayList<>();
//            subexpressionCache = cacheWorkloadProfiler.getAvailableViews();

            //change to line below to disable caching
            //List<CaptureLineage.Lineage> subexpressionCache = new ArrayList<>();

            for (CaptureLineage.Lineage lineage : subexpressionCache) {
                System.out.println(lineage.toString());
            }

            //System.out.println("ACTUAL QUERY COUNT: " + queryCount);

            CaptureLineage captureLineageCarrier = new CaptureLineage(plan.getStatsAndCosts(), plan.getTypes());

            if (globalPlaner.useDP || globalPlaner.useBacktracking) {
                //System.out.println("GOT QUERY INSIDE DATAPATH GLOBAL PLANNER");
                startTime = System.currentTimeMillis();
                globalPlaner.nonInclusiveTime = 0;
                globalPlaner.dpTime = 0;
                globalPlaner.operatorCount = 0;
                //List<JoinNode> globalPlanOutputJoinNodes = GlobalPlaner.acceptNewQuery(plan.getRoot(), plan.getStatsAndCosts(), logicalPlanList.size()+1); //TODO replace by below
                CaptureLineage captureLineageCurrent = new CaptureLineage(plan.getStatsAndCosts(), plan.getTypes());
                captureLineageCurrent.visitPlan(plan.getRoot(), null);
                List<JoinNode> globalPlanOutputJoinNodes = null;

                try {
                    globalPlanOutputJoinNodes = globalPlaner.acceptNewQuery(plan.getRoot(), plan.getStatsAndCosts(), allQueryCountLocal, getSession(), subexpressionCache, captureLineageCurrent.getLastValid());
                } catch (Exception e) {
                    e.printStackTrace();
                }

                stopTime = System.currentTimeMillis();
                newQueryPlanningTime += (stopTime - startTime);// - GlobalPlaner.nonInclusiveTime;
                int numberOfJoins = GlobalPlaner.getNumberOfJoins(plan.getRoot());

//                if (numberOfJoinsToRunningTimeMapping.containsKey(numberOfJoins)) {
//                    numberOfJoinsToRunningTimeMapping.get(numberOfJoins).add(newQueryPlanningTime);
//                } else {
//                    numberOfJoinsToRunningTimeMapping.put(numberOfJoins, new ArrayList<>());
//                    numberOfJoinsToRunningTimeMapping.get(numberOfJoins).add(newQueryPlanningTime);
//                }
                //cummulativePlanningTime += newQueryPlanningTime;
                //cumulativeOperatorCount += GlobalPlaner.operatorCount;
                //cumulativePlanTimeList.add(cummulativePlanningTime);
                //DPPlanningTime += GlobalPlaner.dpTime;

                //System.out.println("\nGLOBAL PLAN"); TODO
                System.out.println("\n" + GlobalPlanPrinter.globalPlanToText(globalPlanOutputJoinNodes, allQueryCountLocal));
                //System.out.println("\nEND OF GLOBAL PLAN");
                System.out.println("force join string for this plan  is "+GlobalPlanPrinter.joinString.toString());

                captureLineageCarrier.setJoinHintString(GlobalPlanPrinter.joinString.toString());
                captureLineageCarrier.setSelectedViews(globalPlaner.selectedViews);



                //System.out.println("QUERY COUNT: " + queryCount + ", New query planning time(ms): " + (newQueryPlanningTime) + ", Cumulative planning time(ms): " + (cummulativePlanningTime) + " total DP time is " + DPPlanningTime);
                // TODO System.out.println("QUERY COUNT: " + queryCount + ", new operator count : " + GlobalPlaner.operatorCount + ", Cumulative operator count: " + cumulativeOperatorCount);

                //cost calculations
                double newQueryPlanCost = JoinCostCalculator.calculateJoinCost(plan.getRoot(), plan.getStatsAndCosts());
                //localPlansCummulativeCost += newQueryPlanCost;
                double globalPlanCost = globalPlaner.getGlobalPlanCost();
                //System.out.println("QUERY COUNT: " + queryCount + "New query cost: " + newQueryPlanCost + ", Cumulative local plans cost: " + localPlansCummulativeCost + ", Cumulative global plan cost: " + globalPlanCost);
            }


            List<CaptureLineage.Lineage> toMaterialize;
            if (SystemSessionProperties.isCachingPartioningEnabled(getSession()))
                toMaterialize = globalPlaner.selectedViews.stream().filter(view -> cacheWorkloadProfiler.getPendingViews().contains(view)).collect(Collectors.toList());
		    else
	            toMaterialize = new ArrayList<CaptureLineage.Lineage>();

//            toMaterialize = globalPlaner.selectedViews.stream().filter(view -> cacheWorkloadProfiler.getPendingViews().contains(view)).collect(Collectors.toList());

            System.out.println("To Materialize ");
            int i =1;
            for (CaptureLineage.Lineage newView : toMaterialize) {
                System.out.println("View "+i+"\n"+newView.toString());
            }

            for (CaptureLineage.Lineage newView : toMaterialize) {
                Optional<CacheWorkloadProfiler.PlanWithCacheMetadata> planMaterialize = cacheWorkloadProfiler.getPlan(newView);
                if (planMaterialize.isPresent()) {
                    try {
                        CacheWorkloadProfiler.PlanWithCacheMetadata planWithCacheMetadata = planMaterialize.get();

                        PlanNode planNodeWriter = cacheWorkloadProfiler.getCreateTablePlan(planWithCacheMetadata, newView, metadata, getSession(), planSymbolAllocator, idAllocator, typesCollector, stateMachine.getWarningCollector(), statsCalculator, costCalculator);
                        BeginTableWrite beginTableWrite = new BeginTableWrite(metadata);
                        planNodeWriter = beginTableWrite.optimize(planNodeWriter, getSession(), new TypeProvider(typesCollector), planSymbolAllocator, idAllocator, stateMachine.getWarningCollector());
                        Plan planWriter = new Plan(planNodeWriter, new TypeProvider(typesCollector), new StatsAndCosts(statsCollector, costsCollector));

                        CaptureLineage captureLineageNew = new CaptureLineage(planWriter.getStatsAndCosts(), planWriter.getTypes());
                        CaptureLineage.Lineage lineageNew = captureLineageNew.visitPlan(planWriter.getRoot(), null);

                        if (lineageNew.getTarget() == null) throw new UnsupportedOperationException("target not found");

                        newView.setTarget(lineageNew.getTarget());
                        cacheWorkloadProfiler.setToPlanned(newView);

                        int creationQueryCount =  allQueryCount.incrementAndGet();
                        PlanNode metaNode = preparePlanForBatching(planWriter, creationQueryCount);
                        synchronized (this.getClass()) {
                            logicalPlanList.add(metaNode);
                            queryIdList.add(stateMachine.getSession().getQueryId());
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        throw e;
                    }
                }
            }

            if (!captureLineageResult.isFinalized()) {
                Plan plan_new;

                try {
                    plan_new = createPlan(analysis, stateMachine.getSession(), planOptimizers, idAllocator, metadata, new TypeAnalyzer(sqlParser, metadata), statsCalculator, costCalculator, stateMachine.getWarningCollector(), captureLineageCarrier);

                    CaptureLineage captureLineageNew = new CaptureLineage(plan_new.getStatsAndCosts(), plan_new.getTypes());
                    CaptureLineage.Lineage lineageNew = captureLineageNew.visitPlan(plan_new.getRoot(), null);
                    cacheWorkloadProfiler.appendToBatch(plan_new, captureLineageNew.getLastValid(), captureLineageNew.getLineageMapping());
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
                //System.out.println("new plan");

                List<CaptureLineage.Lineage> usedViews = globalPlaner.selectedViews.stream().filter(view -> cacheWorkloadProfiler.getMaterializedViews().contains(view)).collect(Collectors.toList());
                System.out.println("Used Views are ");
                int view_cnt = 1;
                for (CaptureLineage.Lineage l : usedViews){
                    System.out.println("view number "+view_cnt+" "+l.toString());
                }

                CaptureLineage captureLineageNew = new CaptureLineage(plan_new.getStatsAndCosts(), plan_new.getTypes());
                captureLineageNew.visitPlan(plan_new.getRoot(), null);
                SubexpressionReplacement sr = new SubexpressionReplacement(metadata, getSession(), idAllocator, planSymbolAllocator, typesCollector, captureLineageNew.getLineageMapping(), usedViews);
                PlanNode newRoot = sr.visitPlan(plan_new.getRoot(), null);
                plan_new = new Plan(newRoot, plan_new.getTypes(), plan_new.getStatsAndCosts());


                PlanCostEstimate new_cost = plan_new.getStatsAndCosts().getCosts().get(plan_new.getRoot().getId());
                double full_new_cost = new_cost.getCpuCost() * config.getCpuCostWeight() + new_cost.getMaxMemory() * config.getMemoryCostWeight() + new_cost.getNetworkCost() * config.getNetworkCostWeight();
                //System.out.println("old: cpu cost " + old_cost.getCpuCost() + " memory cost " + old_cost.getMaxMemory() + " full cost " + full_old_cost);
                //System.out.println("new: cpu cost " + new_cost.getCpuCost() + " memory cost " + new_cost.getMaxMemory() + " full cost " + full_new_cost);

                String plantxt_new = PlanPrinter.textLogicalPlan(plan_new.getRoot(), plan_new.getTypes(), metadata, plan_new.getStatsAndCosts(), stateMachine.getSession(), 0, true);
                //System.out.println(plantxt_new);

                double one_query_cost_threshold = SystemSessionProperties.getSingleQueryCostThreshold(getSession());
                System.out.println("Datapath finished");
                if (full_new_cost < one_query_cost_threshold * full_old_cost) {
                    //System.out.println("updated the new join order");
                    plan = plan_new;
                }
            }

            PlanNode metaNode = preparePlanForBatching(plan, allQueryCountLocal);

            int cur = currentlyOptimized.getAndAdd(1);

            //System.out.println("Currently optimized " + cur);

            synchronized (this.getClass()) {
                overlapOrderCount.getAndAdd(1);
                if (overlapOrderCount.intValue() >= currentBatchSize.intValue()) {
                    overlapOrderCount.getAndSet(0);
                    currentBatchSize.getAndSet(0);
                    planAggregationPhase = true;
                }

                logicalPlanList.add(metaNode);
                queryIdList.add(stateMachine.getSession().getQueryId());

                if (logicalPlanList.size() >= SystemSessionProperties.getBatchSize(getSession())) {
                    List<Symbol> outputSymbols = new ArrayList<>();
                    outputSymbols.add(planSymbolAllocator.newSymbol("out", BIGINT));
                    ListMultimap<Symbol, Symbol> symbolMap = ArrayListMultimap.create();

                    typesCollector.put(outputSymbols.get(0), BIGINT);

                    List<List<Symbol>> symbolsAll = new ArrayList<>();

                    for (PlanNode node : logicalPlanList) {
                        List<Symbol> inputSymbols = new ArrayList<>();
                        inputSymbols.addAll(node.getOutputSymbols());
                        symbolsAll.add(inputSymbols);
                    }

                    ExchangeNode unionNode = new ExchangeNode(idAllocator.getNextId(), ExchangeNode.Type.GATHER, ExchangeNode.Scope.REMOTE, new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), outputSymbols), logicalPlanList, symbolsAll, Optional.empty());
                    PlanNode rootNode = new OutputNode(idAllocator.getNextId(), unionNode, ImmutableList.of("agg"), ImmutableList.of(unionNode.getOutputSymbols().get(0)));

                    try {
                        PlanExpressionCollector.PlanExpressionCollectorContext ctx = new PlanExpressionCollector.PlanExpressionCollectorContext();
                        PlanExpressionCollector collector = new PlanExpressionCollector();
                        collector.visitPlan(rootNode, ctx);
                        AddRouters addRouters = new AddRouters(ctx, typesCollector);
                        ctx.printAllExpressions();
                        rootNode = addRouters.optimize(rootNode, stateMachine.getSession(), new TypeProvider(typesCollector), planSymbolAllocator, idAllocator, stateMachine.getWarningCollector());

                        plan = new Plan(rootNode, new TypeProvider(typesCollector), new StatsAndCosts(statsCollector, costsCollector));
                        String plantxt = PlanPrinter.textLogicalPlan(rootNode, new TypeProvider(typesCollector), metadata, new StatsAndCosts(statsCollector, costsCollector), stateMachine.getSession(), 0, true);
                        System.out.println(plantxt);

                        AddExchangesAboveRouters addExchangesAboveRouters = new AddExchangesAboveRouters(ctx, typesCollector);
                        rootNode = addExchangesAboveRouters.optimize(rootNode, stateMachine.getSession(), new TypeProvider(typesCollector), planSymbolAllocator, idAllocator, stateMachine.getWarningCollector());

                        Harmonizer harmonizer = new Harmonizer(ctx, typesCollector);
                        rootNode = harmonizer.optimize(rootNode, stateMachine.getSession(), new TypeProvider(typesCollector), planSymbolAllocator, idAllocator, stateMachine.getWarningCollector());
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    plan = new Plan(rootNode, new TypeProvider(typesCollector), new StatsAndCosts(statsCollector, costsCollector));
                    String plantxt = PlanPrinter.textLogicalPlan(rootNode, new TypeProvider(typesCollector), metadata, new StatsAndCosts(statsCollector, costsCollector), stateMachine.getSession(), 0, true);
                    System.out.println(plantxt);

                    try {
                        cacheWorkloadProfiler.setToMaterialized();
                        cacheWorkloadProfiler.fireBatch(stateMachine.getSession());
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw e;
                    }

                    stateMachine.getSession().setBatchQueries(queryIdList);
                    logicalPlanList = new ArrayList<>();
                    queryIdList = new ArrayList<>();
                    globalPlaner.resetStaticVariables();
                } else {
                    plan = lowCostQuery;
                }

                currentlyOptimized.getAndAdd(-1);
            }
        }
        /*else if (SystemSessionProperties.isBatchEnabled(getSession()) && planAggregationPhase){
            synchronized (this.getClass()) {
                overlapOrderCount.getAndAdd(1);
                currentBatchSize.getAndAdd(1);
                topologicalOrderPlanList.put(overlapOrderCount.intValue(), plan);

                //get only the datapath
                JoinInformationExtractor.JoinInformation joinInformation = globalPlaner.getJoinInfoQueryOrder(plan.getRoot(), plan.getStatsAndCosts());
                JoinGraphMap.put(overlapOrderCount.intValue(), joinInformation);
                globalPlaner.GraphConstruction(JoinGraphMap, getSession() );

                //System.out.println("ACTUAL QUERY COUNT: " + allQueryCount);
                plan = lowCostQuery;
            }
        }*/
        else if (allQueryCountLocal > 1) {

        }


        //plan = createPlan(analysis, stateMachine.getSession(), planOptimizers, idAllocator, metadata, new TypeAnalyzer(sqlParser, metadata), statsCalculator, costCalculator, stateMachine.getWarningCollector());
        queryPlan.set(plan);


        // extract inputs
        List<Input> inputs = new InputExtractor(metadata, stateMachine.getSession()).extractInputs(plan.getRoot());
        stateMachine.setInputs(inputs);

        // extract output
        stateMachine.setOutput(analysis.getTarget());

        //System.out.println("before fragmenter");

        SubPlan fragmentedPlan;

        try {
            // fragment the plan
            fragmentedPlan = planFragmenter.createSubPlans(stateMachine.getSession(), plan, false, stateMachine.getWarningCollector());
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new NullPointerException("propagated");
        }

        //System.out.println("Fragmenter");

        //System.out.println("after fragmenter");

        // record analysis time
        stateMachine.endAnalysis();

        //System.out.println("PLAN PRODUCED2");

        boolean explainAnalyze = analysis.getStatement() instanceof Explain && ((Explain) analysis.getStatement()).isAnalyze();
        return new PlanRoot(fragmentedPlan, !explainAnalyze, extractConnectors(analysis));
    }

    // This method was introduced separate logical planning from query analyzing stage
    // and allow plans to be overwritten by CachedSqlQueryExecution
    protected Plan createPlan(Analysis analysis,
                              Session session,
                              List<PlanOptimizer> planOptimizers,
                              PlanNodeIdAllocator idAllocator,
                              Metadata metadata,
                              TypeAnalyzer typeAnalyzer,
                              StatsCalculator statsCalculator,
                              CostCalculator costCalculator,
                              WarningCollector warningCollector)
    {
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata, typeAnalyzer, statsCalculator, costCalculator, warningCollector);
        return logicalPlanner.plan(analysis);
    }

    // This method was introduced separate logical planning from query analyzing stage
    // and allow plans to be overwritten by CachedSqlQueryExecution
    protected Plan createPlan(Analysis analysis,
                              Session session,
                              List<PlanOptimizer> planOptimizers,
                              PlanNodeIdAllocator idAllocator,
                              Metadata metadata,
                              TypeAnalyzer typeAnalyzer,
                              StatsCalculator statsCalculator,
                              CostCalculator costCalculator,
                              WarningCollector warningCollector, CaptureLineage captureLineage)
    {
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata, typeAnalyzer, statsCalculator, costCalculator, warningCollector);
        return logicalPlanner.plan(analysis, captureLineage);
    }

    private static Set<CatalogName> extractConnectors(Analysis analysis)
    {
        ImmutableSet.Builder<CatalogName> connectors = ImmutableSet.builder();

        for (TableHandle tableHandle : analysis.getTables()) {
            connectors.add(tableHandle.getCatalogName());
        }

        if (analysis.getInsert().isPresent()) {
            TableHandle target = analysis.getInsert().get().getTarget();
            connectors.add(target.getCatalogName());
        }

        return connectors.build();
    }

    private void planDistribution(PlanRoot plan)
    {
        // time distribution planning
        stateMachine.beginDistributedPlanning();

        //System.out.println("before distribution");
        // plan the execution on the active nodes
        DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager, metadata);
        StageExecutionPlan outputStageExecutionPlan = distributedPlanner.plan(plan.getRoot(), stateMachine.getSession());
        stateMachine.endDistributedPlanning();

        //System.out.println("after distribution");

        // ensure split sources are closed
        stateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                closeSplitSources(outputStageExecutionPlan);
            }
        });

        // if query was canceled, skip creating scheduler
        if (stateMachine.isDone()) {
            return;
        }

        // record output field
        stateMachine.setColumns(outputStageExecutionPlan.getFieldNames(), outputStageExecutionPlan.getFragment().getTypes());

        PartitioningHandle partitioningHandle = plan.getRoot().getFragment().getPartitioningScheme().getPartitioning().getHandle();
        OutputBuffers rootOutputBuffers = createInitialEmptyOutputBuffers(partitioningHandle)
                .withBuffer(OUTPUT_BUFFER_ID, BROADCAST_PARTITION_ID)
                .withNoMoreBufferIds();

        // build the stage execution objects (this doesn't schedule execution)
        SqlQueryScheduler scheduler = createSqlQueryScheduler(
                stateMachine,
                locationFactory,
                outputStageExecutionPlan,
                nodePartitioningManager,
                nodeScheduler,
                remoteTaskFactory,
                stateMachine.getSession(),
                plan.isSummarizeTaskInfos(),
                scheduleSplitBatchSize,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                rootOutputBuffers,
                nodeTaskMap,
                executionPolicy,
                schedulerStats,
                dynamicFilterService,
                heuristicIndexerManager);

        queryScheduler.set(scheduler);

        // if query was canceled during scheduler creation, abort the scheduler
        // directly since the callback may have already fired
        if (stateMachine.isDone()) {
            scheduler.abort();
            queryScheduler.set(null);
        }
    }

    private static void closeSplitSources(StageExecutionPlan plan)
    {
        for (SplitSource source : plan.getSplitSources().values()) {
            try {
                source.close();
            }
            catch (Throwable t) {
                log.warn(t, "Error closing split source");
            }
        }

        for (StageExecutionPlan stage : plan.getSubStages()) {
            closeSplitSources(stage);
        }
    }

    @Override
    public void cancelQuery()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            SqlQueryScheduler scheduler = queryScheduler.get();
            if (scheduler != null) {
                scheduler.cancelStage(stageId);
            }
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        stateMachine.transitionToFailed(cause);
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        stateMachine.addOutputInfoListener(listener);
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        stateMachine.pruneQueryInfo();
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            // acquire reference to scheduler before checking finalQueryInfo, because
            // state change listener sets finalQueryInfo and then clears scheduler when
            // the query finishes.
            SqlQueryScheduler scheduler = queryScheduler.get();

            return stateMachine.getFinalQueryInfo().orElseGet(() -> buildQueryInfo(scheduler));
        }
    }

    @Override
    public boolean isRunningAsync()
    {
        return stateMachine.isRunningAsync();
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Plan getQueryPlan()
    {
        return queryPlan.get();
    }

    private QueryInfo buildQueryInfo(SqlQueryScheduler scheduler)
    {
        Optional<StageInfo> stageInfo = Optional.empty();
        if (scheduler != null) {
            stageInfo = Optional.ofNullable(scheduler.getStageInfo());
        }

        QueryInfo queryInfo = stateMachine.updateQueryInfo(stageInfo);
        if (queryInfo.isFinalQueryInfo()) {
            // capture the final query state and drop reference to the scheduler
            queryScheduler.set(null);
        }

        return queryInfo;
    }

    private static class PlanRoot
    {
        private final SubPlan root;
        private final boolean summarizeTaskInfos;
        private final Set<CatalogName> connectors;

        public PlanRoot(SubPlan root, boolean summarizeTaskInfos, Set<CatalogName> connectors)
        {
            this.root = requireNonNull(root, "root is null");
            this.summarizeTaskInfos = summarizeTaskInfos;
            this.connectors = ImmutableSet.copyOf(connectors);
        }

        public SubPlan getRoot()
        {
            return root;
        }

        public boolean isSummarizeTaskInfos()
        {
            return summarizeTaskInfos;
        }

        public Set<CatalogName> getConnectors()
        {
            return connectors;
        }
    }

    public static class SqlQueryExecutionFactory
            implements QueryExecutionFactory<QueryExecution>
    {
        private final SplitSchedulerStats schedulerStats;
        private final int scheduleSplitBatchSize;
        private final Metadata metadata;
        private final CubeManager cubeManager;
        private final AccessControl accessControl;
        private final SqlParser sqlParser;
        private final SplitManager splitManager;
        private final NodePartitioningManager nodePartitioningManager;
        private final NodeScheduler nodeScheduler;
        private final List<PlanOptimizer> planOptimizers;
        private final PlanFragmenter planFragmenter;
        private final RemoteTaskFactory remoteTaskFactory;
        private final QueryExplainer queryExplainer;
        private final LocationFactory locationFactory;
        private final ExecutorService queryExecutor;
        private final ScheduledExecutorService schedulerExecutor;
        private final FailureDetector failureDetector;
        private final NodeTaskMap nodeTaskMap;
        private final Map<String, ExecutionPolicy> executionPolicies;
        private final StatsCalculator statsCalculator;
        private final CostCalculator costCalculator;
        private final DynamicFilterService dynamicFilterService;
        private final Optional<Cache<Integer, CachedSqlQueryExecutionPlan>> cache;
        private final HeuristicIndexerManager heuristicIndexerManager;
        private final StateStoreProvider stateStoreProvider;

        @Inject
        SqlQueryExecutionFactory(QueryManagerConfig config,
                HetuConfig hetuConfig,
                Metadata metadata,
                CubeManager cubeManager,
                AccessControl accessControl,
                SqlParser sqlParser,
                LocationFactory locationFactory,
                SplitManager splitManager,
                NodePartitioningManager nodePartitioningManager,
                NodeScheduler nodeScheduler,
                PlanOptimizers planOptimizers,
                PlanFragmenter planFragmenter,
                RemoteTaskFactory remoteTaskFactory,
                @ForQueryExecution ExecutorService queryExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor,
                FailureDetector failureDetector,
                NodeTaskMap nodeTaskMap,
                QueryExplainer queryExplainer,
                Map<String, ExecutionPolicy> executionPolicies,
                SplitSchedulerStats schedulerStats,
                StatsCalculator statsCalculator,
                CostCalculator costCalculator,
                DynamicFilterService dynamicFilterService,
                HeuristicIndexerManager heuristicIndexerManager,
                StateStoreProvider stateStoreProvider)
        {
            requireNonNull(config, "config is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.scheduleSplitBatchSize = config.getScheduleSplitBatchSize();
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.executionPolicies = requireNonNull(executionPolicies, "schedulerPolicies is null");
            this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null").get();
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
            this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");
            this.loadConfigToService(hetuConfig);
            if (hetuConfig.isExecutionPlanCacheEnabled()) {
                this.cache = Optional.of(CacheBuilder.newBuilder()
                        .expireAfterAccess(java.time.Duration.ofMillis(hetuConfig.getExecutionPlanCacheTimeout()))
                        .maximumSize(hetuConfig.getExecutionPlanCacheMaxItems())
                        .build());
            }
            else {
                this.cache = Optional.empty();
            }
        }

        // Loading properties into PropertyService for later reference
        private void loadConfigToService(HetuConfig hetuConfig)
        {
            PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, hetuConfig.isSplitCacheMapEnabled());
            PropertyService.setProperty(HetuConstant.SPLIT_CACHE_STATE_UPDATE_INTERVAL, hetuConfig.getSplitCacheStateUpdateInterval());
        }

        @Override
        public QueryExecution createQueryExecution(
                PreparedQuery preparedQuery,
                QueryStateMachine stateMachine,
                String slug,
                WarningCollector warningCollector)
        {
            String executionPolicyName = SystemSessionProperties.getExecutionPolicy(stateMachine.getSession());
            ExecutionPolicy executionPolicy = executionPolicies.get(executionPolicyName);
            checkArgument(executionPolicy != null, "No execution policy %s", executionPolicy);

            return new CachedSqlQueryExecution(
                    preparedQuery,
                    stateMachine,
                    slug,
                    metadata,
                    cubeManager,
                    accessControl,
                    sqlParser,
                    splitManager,
                    nodePartitioningManager,
                    nodeScheduler,
                    planOptimizers,
                    planFragmenter,
                    remoteTaskFactory,
                    locationFactory,
                    scheduleSplitBatchSize,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    queryExplainer,
                    executionPolicy,
                    schedulerStats,
                    statsCalculator,
                    costCalculator,
                    warningCollector,
                    dynamicFilterService,
                    this.cache,
                    heuristicIndexerManager,
                    stateStoreProvider);
        }
    }
}
