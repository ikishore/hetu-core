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

package io.prestosql.sql.planner.iterative.rule;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.execution.SqlQueryExecution;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.CostProvider;
import io.prestosql.cost.PlanCostEstimate;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.*;
import io.prestosql.spi.plan.JoinNode.DistributionType;
import io.prestosql.spi.plan.JoinNode.EquiJoinClause;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.planner.EqualityInference;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.datapath.globalplanner.GlobalPlanPrinter;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.JoinNodeUtils;
import io.prestosql.sql.planner.plan.CaptureLineage;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.Cache;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.powerSet;
import static com.google.common.collect.Streams.stream;
import static io.prestosql.SystemSessionProperties.getJoinDistributionType;
import static io.prestosql.SystemSessionProperties.getJoinReorderingStrategy;
import static io.prestosql.SystemSessionProperties.getMaxReorderedJoins;
import static io.prestosql.spi.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.spi.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.ExpressionUtils.and;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ExpressionUtils.extractConjuncts;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.AUTOMATIC;
import static io.prestosql.sql.planner.EqualityInference.createEqualityInference;
import static io.prestosql.sql.planner.EqualityInference.nonInferrableConjuncts;
import static io.prestosql.sql.planner.ExpressionDeterminismEvaluator.isDeterministic;
import static io.prestosql.sql.planner.iterative.rule.DetermineJoinDistributionType.canReplicate;
import static io.prestosql.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.INFINITE_COST_RESULT;
import static io.prestosql.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.UNKNOWN_COST_RESULT;
import static io.prestosql.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode.toMultiJoinNode;
import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class ReorderJoins
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(ReorderJoins.class);

    // We check that join distribution type is absent because we only want
    // to do this transformation once (reordered joins will have distribution type already set).
    private static final Pattern<JoinNode> PATTERN = join().matching(
            joinNode -> !joinNode.getDistributionType().isPresent()
                    && joinNode.getType() == INNER
                    && isDeterministic(joinNode.getFilter().map(OriginalExpressionUtils::castToExpression).orElse(TRUE_LITERAL)));

    public static Map<String, String> NodeIdTableMap;

    private final CostComparator costComparator;

    public ReorderJoins(CostComparator costComparator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getJoinReorderingStrategy(session) == AUTOMATIC && SystemSessionProperties.getSkipReorderingThreshold(session) > 0;
    }

    public static boolean readJoinForcingValue(){

        try {
            FileInputStream fstream = new FileInputStream("./joinForcing.txt");
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
                System.out.println("VSK: Shared operation is true");
                return  true;
            }
            else {
                System.out.println("VSK: Shared operation is false");
                return false;
            }

        } catch (Exception e) {// Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
        System.out.println("VSK: It should not come here");
        return false;
    }



    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context) {
        return apply2(joinNode,captures, context, null);
    }

    public Result apply2(JoinNode joinNode, Captures captures, Context context, CaptureLineage captureLineage) {
        //String joinHintString = "((date_dim,(catalog_sales,customer_address)),call_center)";
        boolean joinHint = false;
        String joinHintString = null;

        if (captureLineage !=null) {
            System.out.println("In ReorderJoins: apply2 join Hint " + captureLineage.getJoinHintString());
            joinHintString = captureLineage.getJoinHintString();
            joinHint = true;
        }
        //GlobalPlanPrinter.joinString.toString();
        //System.out.println(" "+joinHintString);
//        try {
//            String file = "./joinOrder.txt";
//
//            BufferedReader reader = new BufferedReader(new FileReader(file));
//            joinHintString = reader.readLine();
//            reader.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        //System.out.println("VSK: ReorderJoins: apply()" + joinNode.getId());
        MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context.getLookup(), getMaxReorderedJoins(context.getSession()));

        //boolean joinHint = readJoinForcingValue();
        //boolean joinHint = SqlQueryExecution.forceJoinOrder;

        if (joinHint){
            //System.out.println("VSK: ReorderJoins: apply(): multiJoin getSouces() ");
            NodeIdTableMap = new HashMap<String, String>();
            int cnt = 0;
            for (PlanNode tempJoin : multiJoinNode.getSources()) {
                NodeIdTableMap.put(tempJoin.getId().toString(), multiJoinNode.sources_table_name.get(cnt));
                System.out.println(multiJoinNode.sources_table_name.get(cnt));
                cnt++;
            }
            System.out.println("Node id map is " + NodeIdTableMap);

            List<String> sourceOrderFromHint = OrderSoucestoHint(NodeIdTableMap, joinHintString);

            LinkedHashSet<PlanNode> sourcesReordered = new LinkedHashSet<>();

            for (String table : sourceOrderFromHint) {
                for (PlanNode tempJoin : multiJoinNode.getSources()) {
                    if (table.equals(NodeIdTableMap.get(tempJoin.getId().toString()))) {
                        sourcesReordered.add(tempJoin);
                        break;
                    }
                }
            }
            //reorder according to the hint string
            multiJoinNode = new MultiJoinNode(sourcesReordered, multiJoinNode.getFilter(), multiJoinNode.getOutputSymbols(), sourceOrderFromHint);
        }
        else{
            joinHintString = null;
        }


        JoinEnumerator joinEnumerator = new JoinEnumerator(
                costComparator,
                multiJoinNode.getFilter(),
                context, joinHintString);
        JoinEnumerationResult result = joinEnumerator.chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols());
        if (!result.getPlanNode().isPresent()) {
            return Result.empty();
        }
        return Result.ofPlanNode(result.getPlanNode().get());

    }

    List<String> OrderSoucestoHint(Map<String, String> NodeIdTableMap, String joinHint){
        List<String> sources = new ArrayList<>(NodeIdTableMap.values());

        System.out.println("Join Hint "+joinHint);
        Map<String, Integer> IndexMap = new HashMap<>();
        for (String table: sources){
            int index = joinHint.indexOf(table+",");
            if(index == -1)
                index = joinHint.indexOf(table+")");
            IndexMap.put(table, index);
        }

        for (int i=0; i< sources.size(); i++){
            for (int j=i+1; j< sources.size(); j++){
                if(IndexMap.get(sources.get(i)) > IndexMap.get(sources.get(j))){
                    String temp = sources.get(i);
                    sources.set(i, sources.get(j));
                    sources.set(j, temp);
                }
            }
        }
        //System.out.println(" ordered table or sources "+sources);
        return sources;
    }



    @VisibleForTesting
    static class JoinEnumerator
    {
        private final Session session;
        private final CostProvider costProvider;
        // Using Ordering to facilitate rule determinism
        private final Ordering<JoinEnumerationResult> resultComparator;
        private final PlanNodeIdAllocator idAllocator;
        private final Expression allFilter;
        private final EqualityInference allFilterInference;
        private final Lookup lookup;
        private final Context context;
        String joinHintString = null;
        List<String> allPossibleHintJoinClauses;
        private final Map<Set<PlanNode>, JoinEnumerationResult> memo = new HashMap<>();

        @VisibleForTesting
        JoinEnumerator(CostComparator costComparator, Expression filter, Context context)
        {
            this.context = requireNonNull(context);
            this.session = requireNonNull(context.getSession(), "session is null");
            this.costProvider = requireNonNull(context.getCostProvider(), "costProvider is null");
            this.resultComparator = costComparator.forSession(session).onResultOf(result -> result.cost);
            this.idAllocator = requireNonNull(context.getIdAllocator(), "idAllocator is null");
            this.allFilter = requireNonNull(filter, "filter is null");
            this.allFilterInference = createEqualityInference(filter);
            this.lookup = requireNonNull(context.getLookup(), "lookup is null");
        }

        @VisibleForTesting
        JoinEnumerator(CostComparator costComparator, Expression filter, Context context, String joinHintString)
        {
            this.context = requireNonNull(context);
            this.session = requireNonNull(context.getSession(), "session is null");
            this.costProvider = requireNonNull(context.getCostProvider(), "costProvider is null");
            this.resultComparator = costComparator.forSession(session).onResultOf(result -> result.cost);
            this.idAllocator = requireNonNull(context.getIdAllocator(), "idAllocator is null");
            this.allFilter = requireNonNull(filter, "filter is null");
            this.allFilterInference = createEqualityInference(filter);
            this.lookup = requireNonNull(context.getLookup(), "lookup is null");
            this.joinHintString = joinHintString;
            if (joinHintString != null)
                allPossibleHintJoinClauses = AllPossibileValidHintJoins(joinHintString);
            else
                allPossibleHintJoinClauses = null;
        }

        private JoinEnumerationResult chooseJoinOrder(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols)
        {
            context.checkTimeoutNotExhausted();
            System.out.println("VSK: ReorderJoinsRule: JoinEnumerator: ChooseJoinOrder()");
            Set<PlanNode> multiJoinKey = ImmutableSet.copyOf(sources);
            JoinEnumerationResult bestResult = memo.get(multiJoinKey);
            if (bestResult == null) {
                checkState(sources.size() > 1, "sources size is less than or equal to one");
                ImmutableList.Builder<JoinEnumerationResult> resultBuilder = ImmutableList.builder();

                Set<Set<Integer>> partitions = null;
                if (joinHintString!=null)
                    partitions = getPartitionsFromHint(sources);
                else
                    partitions = generatePartitions(sources.size());
                requireNonNull(partitions);

                System.out.println("VSK: total partitions are "+partitions);
                int cnt = 0;
                int inner_cnt = 0;
                int result_builder_cnt =0;
                for (Set<Integer> partition : partitions) {
                    //System.out.println("VSK choose Join Order(): total partitions are : "+partitions.size()+" with current partition choice "+partition);
                    JoinEnumerationResult result = createJoinAccordingToPartitioning(sources, outputSymbols, partition);
                    if(result.planNode.isPresent()){
                        inner_cnt ++;
                        StringBuilder builder = new StringBuilder();
                        HintedReorderJoinsShared.HintedReorderJoinsRule.TableNameExtractor extractor = new HintedReorderJoinsShared.HintedReorderJoinsRule.TableNameExtractor(lookup);
                        result.planNode.get().accept(extractor, builder);
                        //System.out.println("VSK choose Join Order(): builder String: "+builder.toString());
//                        if (HintedReorderJoins.HintedReorderJoinsRule.TableNameExtractor.startsWith(builder.toString(), new String("(date_dim,(customer_address,(customer,catalog_sales)))"))) {
//                            memo.put(multiJoinKey, result);
//                            return result;
//                        }
                    }

                    if (result.equals(UNKNOWN_COST_RESULT)) {
                        //System.out.println(" VSK: came inside unknown cost result ");
                        memo.put(multiJoinKey, result);
                        return result;
                    }
                    if (!result.equals(INFINITE_COST_RESULT)) {
                        result_builder_cnt ++;
                        resultBuilder.add(result);
                        //memo.put(multiJoinKey, result);
                        //System.out.println("VSK: JoinEnumerationResult : PlanNode "+result.getPlanNode().get());
                    }
                    cnt ++;
                }

                List<JoinEnumerationResult> results = resultBuilder.build();
                //System.out.println("VSK: ChooseJoinOrder: partition Cnt = "+cnt+" out of "+partitions.size()+ "for which plan node is present = "+inner_cnt+" result builder cnt "+result_builder_cnt+ " result size = "+"result.size() = "+results.size());

                if (results.isEmpty()) {
                    memo.put(multiJoinKey, INFINITE_COST_RESULT);
                    return INFINITE_COST_RESULT;
                }

                bestResult = resultComparator.min(results);
                memo.put(multiJoinKey, bestResult);
            }

            bestResult.planNode.ifPresent((planNode) -> log.debug("Least cost join was: %s", planNode));
            return bestResult;
        }

        private Set<Set<Integer>> getPartitionsFromHint(LinkedHashSet<PlanNode>  sources) {

            List<String> tableSources = new ArrayList<String>();
            for(PlanNode tempJoin: sources){
                tableSources.add(ReorderJoins.NodeIdTableMap.get(tempJoin.getId().toString()));
            }

            //get the sources matching string from Hint
            String subStringHint = null;
            for(String hintSubStr: allPossibleHintJoinClauses){
                boolean matching = true;
                if(hintSubStr.split(",").length == sources.size()) {  //TODO check this
                    for (String tableSource : tableSources) {
                        if (!hintSubStr.contains(tableSource))
                            matching = false;
                    }
                    if(matching)
                        subStringHint = hintSubStr;
                    else
                        System.out.println("Didn't find " + hintSubStr  + " " + tableSources.toString());
                }
            }

            requireNonNull(subStringHint);

            String leftChildSubString = getLeftChildAtRoot(subStringHint);
            int i =0;
            Set<Set<Integer>> partitions = new HashSet<>();
            Set<Integer> partition = new HashSet<>();
            for (String table: tableSources){
                if (leftChildSubString.contains(table)){
                    if(table.equals("customer") && (leftChildSubString.contains("customer_address")|| leftChildSubString.contains("customer_demographics")) ){
                        String removedString = leftChildSubString.replace("customer_address","");
                        removedString = removedString.replace("customer_demographics","");
                        if(removedString.contains(table))
                            partition.add(i);
                    }else if(table.equals("store") && (leftChildSubString.contains("store_sales")|| leftChildSubString.contains("store_returns")) ){
                        String removedString = leftChildSubString.replace("store_sales","");
                        removedString = removedString.replace("store_returns","");

                        if(removedString.contains(table))
                            partition.add(i);
                    }
                    else partition.add(i);
                }
                i++;
            }
            partitions.add(partition);

            return partitions;
        }


        List<String> AllPossibileValidHintJoins(String joinHint){

            Deque<Integer> stack
                    = new ArrayDeque<Integer>();
            List<String> hintJoinClauses = new ArrayList<>();
            // Traversing the Expression
            for (int i = 0; i < joinHint.length(); i++)
            {
                char x = joinHint.charAt(i);

                if (x == '(')
                {
                    // Push the element in the stack
                    stack.push(i+1);

                }

                else if (x == ')'){
                    int startingIndex  = stack.pop();
                    hintJoinClauses.add(joinHint.substring(startingIndex, i));

                }
            }
            requireNonNull(hintJoinClauses);
            return hintJoinClauses;
        }

        String getLeftChildAtRoot(String joinHint){

            Deque<Integer> stack
                    = new ArrayDeque<Integer>();
            String LeftChildAtRoot = null;

            if ((joinHint.charAt(0)!= '(') || (joinHint.split(",").length==2)){
                String left_table = joinHint.split(",")[0];
                return left_table;
            }

            boolean firstOpenbracket = true;
            int firstOpenbracketPosition = -1;
            for (int i = 0; i < joinHint.length(); i++)
            {
                char x = joinHint.charAt(i);

                if (x == '(')
                {
                    // Push the element in the stack
                    stack.push(i+1);
                    if (firstOpenbracket == true) {
                        firstOpenbracketPosition = i + 1;
                        firstOpenbracket = false;
                    }
                }
                else if (x == ')'){
                    int startingIndex  = stack.pop();
                    if(startingIndex == firstOpenbracketPosition) {
                        LeftChildAtRoot = new String(joinHint.substring(startingIndex, i));
                        break;
                    }
                }
            }

            //System.out.println(" getChildAtRootFromHint "+LeftChildAtRoot);

            return requireNonNull(LeftChildAtRoot);
        }
        /**
         * This method generates all the ways of dividing totalNodes into two sets
         * each containing at least one node. It will generate one set for each
         * possible partitioning. The other partition is implied in the absent values.
         * In order not to generate the inverse of any set, we always include the 0th
         * node in our sets.
         *
         * @return A set of sets each of which defines a partitioning of totalNodes
         */
        @VisibleForTesting
        static Set<Set<Integer>> generatePartitions(int totalNodes)
        {
            checkArgument(totalNodes > 1, "totalNodes must be greater than 1");
            Set<Integer> numbers = IntStream.range(0, totalNodes)
                    .boxed()
                    .collect(toImmutableSet());
            return powerSet(numbers).stream()
                    .filter(subSet -> subSet.contains(0))
                    .filter(subSet -> subSet.size() < numbers.size())
                    .collect(toImmutableSet());
        }

        @VisibleForTesting
        JoinEnumerationResult createJoinAccordingToPartitioning(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols, Set<Integer> partitioning)
        {
            List<PlanNode> sourceList = ImmutableList.copyOf(sources);
            //System.out.println("VSK: Reorder Joins: createJoinAccordingtoPartitioning: sourceList.size(): "+sourceList.size()+ "partitioning: "+partitioning);
            //for (PlanNode tempJoin: sourceList)
            //    System.out.println("VSK: ReorderJoins: createJoinAccordingtoPartitioning: sources "+tempJoin.getId());
            LinkedHashSet<PlanNode> leftSources = partitioning.stream()
                    .map(sourceList::get)
                    .collect(toCollection(LinkedHashSet::new));
            LinkedHashSet<PlanNode> rightSources = sources.stream()
                    .filter(source -> !leftSources.contains(source))
                    .collect(toCollection(LinkedHashSet::new));
            return createJoin(leftSources, rightSources, outputSymbols);
        }

        private JoinEnumerationResult createJoin(LinkedHashSet<PlanNode> leftSources, LinkedHashSet<PlanNode> rightSources, List<Symbol> outputSymbols)
        {
            /*System.out.println("VSK: ReorderJoinsRule: JoinEnumerator: CreateJoin()");
            for (PlanNode tempJoin: leftSources)
                System.out.println("VSK: ReorderJoins: createJoin: Leftsources"+tempJoin.getId());
            for (PlanNode tempJoin: rightSources)
                System.out.println("VSK: ReorderJoins: createJoin: RightSources: "+tempJoin.getId());*/

            Set<Symbol> leftSymbols = leftSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());
            Set<Symbol> rightSymbols = rightSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());


            List<Expression> joinPredicates = getJoinPredicates(leftSymbols, rightSymbols);
            List<EquiJoinClause> joinConditions = joinPredicates.stream()
                    .filter(JoinEnumerator::isJoinEqualityCondition)
                    .map(predicate -> toEquiJoinClause((ComparisonExpression) predicate, leftSymbols))
                    .collect(toImmutableList());

            if (joinConditions.isEmpty()) {
                //System.out.println("Join conditions are empty");
                return INFINITE_COST_RESULT;
            }

            for (EquiJoinClause temp: joinConditions)
                System.out.println("VSK: ReorderJoins: createJoin: Equijoin clauses: "+temp.toString());

            List<Expression> joinFilters = joinPredicates.stream()
                    .filter(predicate -> !isJoinEqualityCondition(predicate))
                    .collect(toImmutableList());

            Set<Symbol> requiredJoinSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(outputSymbols)
                    .addAll(SymbolsExtractor.extractUnique(joinPredicates))
                    .build();

            JoinEnumerationResult leftResult = getJoinSource(
                    leftSources,
                    requiredJoinSymbols.stream()
                            .filter(leftSymbols::contains)
                            .collect(toImmutableList()));
            if (leftResult.equals(UNKNOWN_COST_RESULT)) {
                //System.out.println("left is unknown cost results: ");
                return UNKNOWN_COST_RESULT;
            }
            if (leftResult.equals(INFINITE_COST_RESULT)) {
                //System.out.println("left is infinite cost results");
                return INFINITE_COST_RESULT;
            }

            PlanNode left = leftResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            JoinEnumerationResult rightResult = getJoinSource(
                    rightSources,
                    requiredJoinSymbols.stream()
                            .filter(rightSymbols::contains)
                            .collect(toImmutableList()));
            if (rightResult.equals(UNKNOWN_COST_RESULT)) {
                //System.out.println("right  is unknown cost results");
                return UNKNOWN_COST_RESULT;
            }
            if (rightResult.equals(INFINITE_COST_RESULT)) {
                System.out.println("right is infinite cost results");
                return INFINITE_COST_RESULT;
            }

            PlanNode right = rightResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            //System.out.println("VSK: ReorderJoins: createJoin: left Plan: "+left.getId());
            //System.out.println("VSK: ReorderJoins: createJoin: Right Plan: "+right.getId());

            // sort output symbols so that the left input symbols are first
            List<Symbol> sortedOutputSymbols = Stream.concat(left.getOutputSymbols().stream(), right.getOutputSymbols().stream())
        //            .filter(outputSymbols::contains)
                    .collect(toImmutableList());

            return setJoinNodeProperties(new JoinNode(
                    idAllocator.getNextId(),
                    INNER,
                    left,
                    right,
                    joinConditions,
                    sortedOutputSymbols,
                    joinFilters.isEmpty() ? Optional.empty() : Optional.of(and(joinFilters)).map(OriginalExpressionUtils::castToRowExpression),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of()));
        }

        private List<Expression> getJoinPredicates(Set<Symbol> leftSymbols, Set<Symbol> rightSymbols)
        {
            ImmutableList.Builder<Expression> joinPredicatesBuilder = ImmutableList.builder();

            // This takes all conjuncts that were part of allFilters that
            // could not be used for equality inference.
            // If they use both the left and right symbols, we add them to the list of joinPredicates
            stream(nonInferrableConjuncts(allFilter))
                    .map(conjunct -> allFilterInference.rewriteExpression(conjunct, symbol -> leftSymbols.contains(symbol) || rightSymbols.contains(symbol)))
                    .filter(Objects::nonNull)
                    // filter expressions that contain only left or right symbols
                    .filter(conjunct -> allFilterInference.rewriteExpression(conjunct, leftSymbols::contains) == null)
                    .filter(conjunct -> allFilterInference.rewriteExpression(conjunct, rightSymbols::contains) == null)
                    .forEach(joinPredicatesBuilder::add);

            // create equality inference on available symbols
            // TODO: make generateEqualitiesPartitionedBy take left and right scope
            List<Expression> joinEqualities = allFilterInference.generateEqualitiesPartitionedBy(symbol -> leftSymbols.contains(symbol) || rightSymbols.contains(symbol)).getScopeEqualities();
            EqualityInference joinInference = createEqualityInference(joinEqualities.toArray(new Expression[0]));
            joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(in(leftSymbols)).getScopeStraddlingEqualities());

            return joinPredicatesBuilder.build();
        }

        private JoinEnumerationResult getJoinSource(LinkedHashSet<PlanNode> nodes, List<Symbol> outputSymbols)
        {
            if (nodes.size() == 1) {
                PlanNode planNode = getOnlyElement(nodes);
                ImmutableList.Builder<Expression> predicates = ImmutableList.builder();
                predicates.addAll(allFilterInference.generateEqualitiesPartitionedBy(outputSymbols::contains).getScopeEqualities());
                stream(nonInferrableConjuncts(allFilter))
                        .map(conjunct -> allFilterInference.rewriteExpression(conjunct, outputSymbols::contains))
                        .filter(Objects::nonNull)
                        .forEach(predicates::add);
                Expression filter = combineConjuncts(predicates.build());
                if (!TRUE_LITERAL.equals(filter)) {
                    planNode = new FilterNode(idAllocator.getNextId(), planNode, castToRowExpression(filter));
                }
                return createJoinEnumerationResult(planNode);
            }
            return chooseJoinOrder(nodes, outputSymbols);
        }

        private static boolean isJoinEqualityCondition(Expression expression)
        {
            return expression instanceof ComparisonExpression
                    && ((ComparisonExpression) expression).getOperator() == EQUAL
                    && ((ComparisonExpression) expression).getLeft() instanceof SymbolReference
                    && ((ComparisonExpression) expression).getRight() instanceof SymbolReference;
        }

        private static EquiJoinClause toEquiJoinClause(ComparisonExpression equality, Set<Symbol> leftSymbols)
        {
            Symbol leftSymbol = SymbolUtils.from(equality.getLeft());
            Symbol rightSymbol = SymbolUtils.from(equality.getRight());
            EquiJoinClause equiJoinClause = new EquiJoinClause(leftSymbol, rightSymbol);
            return leftSymbols.contains(leftSymbol) ? equiJoinClause : equiJoinClause.flip();
        }

        private JoinEnumerationResult setJoinNodeProperties(JoinNode joinNode)
        {
            if (isAtMostScalar(joinNode.getRight(), lookup)) {
                return createJoinEnumerationResult(joinNode.withDistributionType(REPLICATED));
            }
            if (isAtMostScalar(joinNode.getLeft(), lookup)) {
                return createJoinEnumerationResult(joinNode.flipChildren().withDistributionType(REPLICATED));
            }
            List<JoinEnumerationResult> possibleJoinNodes = getPossibleJoinNodes(joinNode, getJoinDistributionType(session));
            verify(!possibleJoinNodes.isEmpty(), "possibleJoinNodes is empty");
            if (possibleJoinNodes.stream().anyMatch(UNKNOWN_COST_RESULT::equals)) {
                return UNKNOWN_COST_RESULT;
            }
            return resultComparator.min(possibleJoinNodes);
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, JoinDistributionType distributionType)
        {
            checkArgument(joinNode.getType() == INNER, "unexpected join node type: %s", joinNode.getType());

            if (joinNode.isCrossJoin()) {
                return getPossibleJoinNodes(joinNode, REPLICATED);
            }

            switch (distributionType) {
                case PARTITIONED:
                    return getPossibleJoinNodes(joinNode, PARTITIONED);
                case BROADCAST:
                    return getPossibleJoinNodes(joinNode, REPLICATED);
                case AUTOMATIC:
                    ImmutableList.Builder<JoinEnumerationResult> result = ImmutableList.builder();
                    result.addAll(getPossibleJoinNodes(joinNode, PARTITIONED));
                    result.addAll(getPossibleJoinNodes(joinNode, REPLICATED, node -> canReplicate(node, context)));

                    return result.build();
                default:
                    throw new IllegalArgumentException("unexpected join distribution type: " + distributionType);
            }
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, DistributionType distributionType)
        {
            return getPossibleJoinNodes(joinNode, distributionType, (node) -> true);
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, DistributionType distributionType, Predicate<JoinNode> isAllowed)
        {
            List<JoinNode> nodes = ImmutableList.of(
                    joinNode.withDistributionType(distributionType),
                    joinNode.flipChildren().withDistributionType(distributionType));
            return nodes.stream().filter(isAllowed).map(this::createJoinEnumerationResult).collect(toImmutableList());
        }

        private JoinEnumerationResult createJoinEnumerationResult(PlanNode planNode)
        {
            return JoinEnumerationResult.createJoinEnumerationResult(Optional.of(planNode), costProvider.getCost(planNode));
        }
    }

    /**
     * This class represents a set of inner joins that can be executed in any order.
     */
    @VisibleForTesting
    static class MultiJoinNode
    {
        // Use a linked hash set to ensure optimizer is deterministic
        private final LinkedHashSet<PlanNode> sources;
        private final Expression filter;
        private final List<Symbol> outputSymbols;
        public List<String> sources_table_name;

        public MultiJoinNode(LinkedHashSet<PlanNode> sources, Expression filter, List<Symbol> outputSymbols, List<String> sources_table_name)
        {
            requireNonNull(sources, "sources is null");
            checkArgument(sources.size() > 1, "sources size is <= 1");
            requireNonNull(filter, "filter is null");
            requireNonNull(outputSymbols, "outputSymbols is null");

            this.sources = sources;
            this.filter = filter;
            this.outputSymbols = ImmutableList.copyOf(outputSymbols);
            this.sources_table_name = sources_table_name;
            List<Symbol> inputSymbols = sources.stream().flatMap(source -> source.getOutputSymbols().stream()).collect(toImmutableList());
            checkArgument(inputSymbols.containsAll(outputSymbols), "inputs do not contain all output symbols");
        }

        public Expression getFilter()
        {
            return filter;
        }

        public LinkedHashSet<PlanNode> getSources()
        {
            return sources;
        }

        public List<Symbol> getOutputSymbols()
        {
            return outputSymbols;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sources, ImmutableSet.copyOf(extractConjuncts(filter)), outputSymbols);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof MultiJoinNode)) {
                return false;
            }

            MultiJoinNode other = (MultiJoinNode) obj;
            return this.sources.equals(other.sources)
                    && ImmutableSet.copyOf(extractConjuncts(this.filter)).equals(ImmutableSet.copyOf(extractConjuncts(other.filter)))
                    && this.outputSymbols.equals(other.outputSymbols);
        }

        static MultiJoinNode toMultiJoinNode(JoinNode joinNode, Lookup lookup, int joinLimit)
        {
            // the number of sources is the number of joins + 1
            return new JoinNodeFlattener(joinNode, lookup, joinLimit + 1).toMultiJoinNode();
        }

        private static class JoinNodeFlattener
        {
            private final LinkedHashSet<PlanNode> sources = new LinkedHashSet<>();
            private final List<Expression> filters = new ArrayList<>();
            private final List<Symbol> outputSymbols;
            private final Lookup lookup;
            public List<String> sources_table_name = new ArrayList<>();


            JoinNodeFlattener(JoinNode node, Lookup lookup, int sourceLimit)
            {
                requireNonNull(node, "node is null");
                checkState(node.getType() == INNER, "join type must be INNER");
                this.outputSymbols = node.getOutputSymbols();
                this.lookup = requireNonNull(lookup, "lookup is null");
                flattenNode(node, sourceLimit);
            }

            private String getTableNameForPrefix(String prefix)
            {
                if (prefix.startsWith("cs_")) {
                    return new String("catalog_sales");
                }
                else if (prefix.startsWith("c_")) {
                    return new String("customer");
                }
                else if (prefix.startsWith("d_")) {
                    return new String("date_dim");
                }
                else if (prefix.startsWith("ca_")) {
                    return new String("customer_address");
                }
                else if (prefix.startsWith("cc_")) {
                    return new String("call_center");
                }
                else if (prefix.startsWith("i_")) {
                    return new String("item");
                }
                else if (prefix.startsWith("p_")) {
                    return new String("promotion");
                }
                else if (prefix.startsWith("hd_")) {
                    return new String("household_demographics");
                }
                else if (prefix.startsWith("cd_")) {
                    return new String("customer_demographics");
                }
                else if (prefix.startsWith("sm_")) {
                    return new String("ship_mode");
                }
                else if (prefix.startsWith("w_")) {
                    return new String("warehouse");
                }
                else if (prefix.startsWith("ss_")) {
                    return new String("store_sales");
                }
                else if (prefix.startsWith("inv_")) {
                    return new String("inventory");
                }
                else if (prefix.startsWith("s_")) {
                    return new String("store");
                }
                else if (prefix.startsWith("ws_")) {
                    return new String("web_sales");
                }
                else if (prefix.startsWith("ib_")) {
                    return new String("income_band");
                }                else if (prefix.startsWith("t_")) {
                    return new String("time_dim");
                }
                else if (prefix.startsWith("sr_")) {
                    return new String("store_returns");
                }
                else if (prefix.startsWith("cr_")) {
                    return new String("catalog_returns");
                }
                else {
                    new UnsupportedOperationException("Need to handle this situation in ReorderJoins ");;
                    return null;
                }
            }




            private void flattenNode(PlanNode node, int limit)
            {
                PlanNode resolved = lookup.resolve(node);

                // (limit - 2) because you need to account for adding left and right side
                if (!(resolved instanceof JoinNode) || (sources.size() > (limit - 2))) {

                    if(resolved instanceof  TableScanNode) {
                        System.out.println(" TableScan flatten node id = "+resolved.getId()+" class name is "+resolved.getClass().toString());
                        System.out.println(((TableScanNode) resolved).getTable().getConnectorHandle().getTableName().toString());
                        sources_table_name.add(((TableScanNode) resolved).getTable().getConnectorHandle().getTableName().toString());
                    }
                    else if(resolved instanceof  FilterNode) {
                        /*
                        //System.out.println(" TableScan flatten node id = "+resolved.getId()+" class name is "+resolved.getClass().toString());
                        PlanNode sourceNode = ((FilterNode) resolved).getSource();
                        if (sourceNode instanceof  TableScanNode)
                            sources_table_name.add(((TableScanNode) sourceNode).getTable().getConnectorHandle().getTableName().toString());
                        else {
                            Symbol firstColumn = sourceNode.getOutputSymbols().get(0);
                            if (firstColumn == null)
                                throw new UnsupportedOperationException("Need to handle this situation in ReorderJoins ");
                            String tableName = getTableNameForPrefix(firstColumn.getName());
                            if (tableName == null)
                                throw new UnsupportedOperationException("Need to handle this situation in ReorderJoins ");
                            sources_table_name.add(tableName);
                        }
                        */
                        System.out.println("Filter recursion");
                        flattenNode(((FilterNode) resolved).getSource(), limit);
                        sources.remove(((FilterNode) resolved).getSource());
                    }
                    else if(resolved instanceof  ProjectNode){
                        System.out.println("Project recursion");
                        flattenNode(((ProjectNode) resolved).getSource(), limit);
                        sources.remove(((ProjectNode) resolved).getSource());
                        //ProjectNode tempNode = (ProjectNode) resolved;
                        //throw new UnsupportedOperationException("Need to handle this situation in ReorderJoins ");
                        //System.out.println(" Project node flatten node id = "+resolved.getId()+" class name is "+((ProjectNode) resolved).getSource().getClass().toString());
                        //sources_table_name.add(((ProjectNode) resolved.getSource()).);
                    }
                    else
                        new UnsupportedOperationException("Need to handle this situation in ReorderJoins ");

                    sources.add(node);
                    return;
                }

                JoinNode joinNode = (JoinNode) resolved;
                if (joinNode.getType() != INNER
                        || !isDeterministic(joinNode.getFilter().map(OriginalExpressionUtils::castToExpression).orElse(TRUE_LITERAL))
                        || joinNode.getDistributionType().isPresent()) {
                    //System.out.println(" flatten node id = "+resolved.getId()+" class name is "+resolved.getClass().toString());
                    if(resolved instanceof  TableScanNode)
                        sources_table_name.add(((TableScanNode) resolved).getTable().getConnectorHandle().getTableName().toString());
                    sources.add(node);
                    return;
                }

                // we set the left limit to limit - 1 to account for the node on the right
                flattenNode(joinNode.getLeft(), limit - 1);
                flattenNode(joinNode.getRight(), limit);
                joinNode.getCriteria().stream()
                        .map(JoinNodeUtils::toExpression)
                        .forEach(filters::add);
                joinNode.getFilter().map(OriginalExpressionUtils::castToExpression).ifPresent(filters::add);
            }

            MultiJoinNode toMultiJoinNode()
            {
                return new MultiJoinNode(sources, and(filters), outputSymbols, sources_table_name);
            }
        }

        static class Builder
        {
            private List<PlanNode> sources;
            private Expression filter;
            private List<Symbol> outputSymbols;

            public Builder setSources(PlanNode... sources)
            {
                this.sources = ImmutableList.copyOf(sources);
                return this;
            }

            public Builder setFilter(Expression filter)
            {
                this.filter = filter;
                return this;
            }

            public Builder setOutputSymbols(Symbol... outputSymbols)
            {
                this.outputSymbols = ImmutableList.copyOf(outputSymbols);
                return this;
            }

            public MultiJoinNode build()
            {
                return new MultiJoinNode(new LinkedHashSet<>(sources), filter, outputSymbols, null);
            }
        }
    }

    @VisibleForTesting
    static class JoinEnumerationResult
    {
        public static final JoinEnumerationResult UNKNOWN_COST_RESULT = new JoinEnumerationResult(Optional.empty(), PlanCostEstimate.unknown());
        public static final JoinEnumerationResult INFINITE_COST_RESULT = new JoinEnumerationResult(Optional.empty(), PlanCostEstimate.infinite());

        private final Optional<PlanNode> planNode;
        private final PlanCostEstimate cost;

        private JoinEnumerationResult(Optional<PlanNode> planNode, PlanCostEstimate cost)
        {
            this.planNode = requireNonNull(planNode, "planNode is null");
            this.cost = requireNonNull(cost, "cost is null");
            checkArgument((cost.hasUnknownComponents() || cost.equals(PlanCostEstimate.infinite())) && !planNode.isPresent()
                            || (!cost.hasUnknownComponents() || !cost.equals(PlanCostEstimate.infinite())) && planNode.isPresent(),
                    "planNode should be present if and only if cost is known");
        }

        public Optional<PlanNode> getPlanNode()
        {
            return planNode;
        }

        public PlanCostEstimate getCost()
        {
            return cost;
        }

        static JoinEnumerationResult createJoinEnumerationResult(Optional<PlanNode> planNode, PlanCostEstimate cost)
        {
            if (cost.hasUnknownComponents()) {
                return UNKNOWN_COST_RESULT;
            }
            if (cost.equals(PlanCostEstimate.infinite())) {
                return INFINITE_COST_RESULT;
            }
            return new JoinEnumerationResult(planNode, cost);
        }
    }


    private class TableNameMapExtractor
            extends InternalPlanVisitor<Void, StringBuilder>
    {
        private String pattern = "";


        @Override
        public Void visitTableScan(TableScanNode node, StringBuilder context)
        {
            context.append(node.getTable().getConnectorHandle().getTableName().toString());
            //System.out.println(" Node id "+node.getId()+" table name is "+node.getTable().getConnectorHandle().getTableName().toString());
            return null;
        }

        @Override
        public Void visitPlan(PlanNode node, StringBuilder context)
        {
            //node = node.getSources();
            for (PlanNode source : node.getSources()) {
                source.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, StringBuilder context)
        {
            PlanNode left = node.getLeft();
            PlanNode right = node.getRight();
            context.append('(');
            left.accept(this, context);
            context.append(',');
            right.accept(this, context);
            context.append(')');
            return null;
        }

        public  boolean startsWith(String actualPattern, String expectedPattern)
        {
            return expectedPattern.contains(actualPattern);
        }
    }
}
