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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.SetOperationNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.optimizations.SetOperationNodeUtils.sourceSymbolMap;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;

public class SetOperationNodeTranslator
{
    private static final String MARKER = "marker";
    private static final Signature COUNT_AGGREGATION = new Signature("count", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BOOLEAN));
    private static final Literal GENERIC_LITERAL = new GenericLiteral("BIGINT", "1");
    private final PlanSymbolAllocator planSymbolAllocator;
    private final PlanNodeIdAllocator idAllocator;

    public SetOperationNodeTranslator(PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        this.planSymbolAllocator = requireNonNull(planSymbolAllocator, "SymbolAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "PlanNodeIdAllocator is null");
    }

    public TranslationResult makeSetContainmentPlan(SetOperationNode node)
    {
        checkArgument(!(node instanceof UnionNode), "Cannot simplify a UnionNode");
        List<Symbol> markers = allocateSymbols(node.getSources().size(), MARKER, BOOLEAN);
        // identity projection for all the fields in each of the sources plus marker columns
        List<PlanNode> withMarkers = appendMarkers(markers, node.getSources(), node);

        // add a union over all the rewritten sources. The outputs of the union have the same name as the
        // original intersect node
        List<Symbol> outputs = node.getOutputSymbols();
        UnionNode union = union(withMarkers, ImmutableList.copyOf(concat(outputs, markers)));

        // add count aggregations and filter rows where any of the counts is >= 1
        List<Symbol> aggregationOutputs = allocateSymbols(markers.size(), "count", BIGINT);
        AggregationNode aggregation = computeCounts(union, outputs, markers, aggregationOutputs);
        List<Expression> presentExpression = aggregationOutputs.stream()
                .map(symbol -> new ComparisonExpression(GREATER_THAN_OR_EQUAL, toSymbolReference(symbol), GENERIC_LITERAL))
                .collect(toImmutableList());
        return new TranslationResult(aggregation, presentExpression);
    }

    private List<Symbol> allocateSymbols(int count, String nameHint, Type type)
    {
        ImmutableList.Builder<Symbol> symbolsBuilder = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            symbolsBuilder.add(planSymbolAllocator.newSymbol(nameHint, type));
        }
        return symbolsBuilder.build();
    }

    private List<PlanNode> appendMarkers(List<Symbol> markers, List<PlanNode> nodes, SetOperationNode node)
    {
        ImmutableList.Builder<PlanNode> result = ImmutableList.builder();
        for (int i = 0; i < nodes.size(); i++) {
            result.add(appendMarkers(idAllocator, planSymbolAllocator, nodes.get(i), i, markers, sourceSymbolMap(node, i)));
        }
        return result.build();
    }

    private static PlanNode appendMarkers(PlanNodeIdAllocator idAllocator, PlanSymbolAllocator planSymbolAllocator, PlanNode source, int markerIndex, List<Symbol> markers, Map<Symbol, SymbolReference> projections)
    {
        Assignments.Builder assignments = Assignments.builder();
        // add existing intersect symbols to projection
        for (Map.Entry<Symbol, SymbolReference> entry : projections.entrySet()) {
            Symbol symbol = planSymbolAllocator.newSymbol(entry.getKey().getName(), planSymbolAllocator.getTypes().get(entry.getKey()));
            assignments.put(symbol, castToRowExpression(entry.getValue()));
        }

        // add extra marker fields to the projection
        for (int i = 0; i < markers.size(); ++i) {
            Expression expression = (i == markerIndex) ? TRUE_LITERAL : new Cast(new NullLiteral(), StandardTypes.BOOLEAN);
            assignments.put(planSymbolAllocator.newSymbol(markers.get(i).getName(), BOOLEAN), castToRowExpression(expression));
        }

        return new ProjectNode(idAllocator.getNextId(), source, assignments.build());
    }

    private UnionNode union(List<PlanNode> nodes, List<Symbol> outputs)
    {
        ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputs = ImmutableListMultimap.builder();
        for (PlanNode source : nodes) {
            for (int i = 0; i < source.getOutputSymbols().size(); i++) {
                outputsToInputs.put(outputs.get(i), source.getOutputSymbols().get(i));
            }
        }

        return new UnionNode(idAllocator.getNextId(), nodes, outputsToInputs.build(), outputs);
    }

    private AggregationNode computeCounts(UnionNode sourceNode, List<Symbol> originalColumns, List<Symbol> markers, List<Symbol> aggregationOutputs)
    {
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();

        for (int i = 0; i < markers.size(); i++) {
            Symbol output = aggregationOutputs.get(i);
            aggregations.put(output, new AggregationNode.Aggregation(
                    COUNT_AGGREGATION,
                    ImmutableList.of(castToRowExpression(toSymbolReference(markers.get(i)))),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }

        return new AggregationNode(idAllocator.getNextId(),
                sourceNode,
                aggregations.build(),
                singleGroupingSet(originalColumns),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
    }

    public static class TranslationResult
    {
        private final PlanNode planNode;
        private final List<Expression> presentExpressions;

        public TranslationResult(PlanNode planNode, List<Expression> presentExpressions)
        {
            this.planNode = requireNonNull(planNode, "AggregationNode is null");
            this.presentExpressions = ImmutableList.copyOf(requireNonNull(presentExpressions, "AggregationOutputs is null"));
        }

        public PlanNode getPlanNode()
        {
            return this.planNode;
        }

        public List<Expression> getPresentExpressions()
        {
            return presentExpressions;
        }
    }
}
