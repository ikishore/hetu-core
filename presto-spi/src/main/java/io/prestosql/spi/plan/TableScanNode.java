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
package io.prestosql.spi.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.RowExpression;

import javax.annotation.concurrent.Immutable;

import java.util.*;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableScanNode
        extends PlanNode
{
    private final Long querySet;

    private final TableHandle table;
    private final List<Symbol> outputSymbols;
    private final Map<Symbol, ColumnHandle> assignments; // symbol -> column
    private final Map<Integer, String> predicateCollector;

    private final TupleDomain<ColumnHandle> enforcedConstraint;
    private final Optional<RowExpression> predicate;
    private final boolean forDelete;

    private ReuseExchangeOperator.STRATEGY strategy;
    private UUID reuseTableScanMappingId;
    private RowExpression filterExpr;
    private Integer consumerTableScanNodeCount;

    // We need this factory method to disambiguate with the constructor used for deserializing
    // from a json object. The deserializer sets some fields which are never transported
    // to null
    public static TableScanNode newInstance(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            ReuseExchangeOperator.STRATEGY strategy,
            UUID reuseTableScanMappingId, Integer consumerTableScanNodeCount, boolean forDelete)
    {
        return new TableScanNode(id, table, outputs, assignments, TupleDomain.all(), Optional.empty(), strategy, reuseTableScanMappingId, consumerTableScanNodeCount, forDelete);
    }

    @JsonCreator
    public TableScanNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("querySet") Long querySet,
            @JsonProperty("predicateCollector") Map<Integer, String> predicateCollector,
            @JsonProperty("outputSymbols") List<Symbol> outputs,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments,
            @JsonProperty("predicate") Optional<RowExpression> predicate,
            @JsonProperty("strategy") ReuseExchangeOperator.STRATEGY strategy,
            @JsonProperty("reuseTableScanMappingId") UUID reuseTableScanMappingId,
            @JsonProperty("consumerTableScanNodeCount") Integer consumerTableScanNodeCount,
            @JsonProperty("forDelete") boolean forDelete)
    {
        // This constructor is for JSON deserialization only. Do not use.
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.querySet = querySet;
        this.predicateCollector = predicateCollector;
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs) || outputs.get(outputs.size()-1).getName().startsWith("query_set"), "assignments does not cover all of outputs");
        this.enforcedConstraint = null;
        this.predicate = predicate;
        this.strategy = strategy;
        this.reuseTableScanMappingId = reuseTableScanMappingId;
        this.filterExpr = null;
        this.consumerTableScanNodeCount = consumerTableScanNodeCount;
        this.forDelete = forDelete;
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            Optional<RowExpression> predicate,
            ReuseExchangeOperator.STRATEGY strategy,
            UUID reuseTableScanMappingId,
            Integer consumerTableScanNodeCount,
            boolean forDelete)
    {
        // This constructor is for JSON deserialization only. Do not use.
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.querySet = 1L;
        this.predicateCollector = new HashMap<>();
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs) || outputs.get(outputs.size()-1).getName().startsWith("query_set"), "assignments does not cover all of outputs");
        this.enforcedConstraint = null;
        this.predicate = predicate;
        this.strategy = strategy;
        this.reuseTableScanMappingId = reuseTableScanMappingId;
        this.filterExpr = null;
        this.consumerTableScanNodeCount = consumerTableScanNodeCount;
        this.forDelete = forDelete;
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<RowExpression> predicate,
            ReuseExchangeOperator.STRATEGY strategy,
            UUID reuseTableScanMappingId,
            Integer consumerTableScanNodeCount,
            boolean forDelete)
    {
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.querySet = 1L;
        this.predicateCollector = new HashMap<>();
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs) || outputs.get(outputs.size()-1).getName().startsWith("query_set"), "assignments does not cover all of outputs");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        this.predicate = requireNonNull(predicate, "predicate expression cannot be empty");
        this.strategy = strategy;
        this.reuseTableScanMappingId = reuseTableScanMappingId;
        this.filterExpr = null;
        this.consumerTableScanNodeCount = consumerTableScanNodeCount;
        this.forDelete = forDelete;
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            Long querySet,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<RowExpression> predicate,
            ReuseExchangeOperator.STRATEGY strategy,
            UUID reuseTableScanMappingId,
            Integer consumerTableScanNodeCount,
            boolean forDelete)
    {
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.querySet = querySet;
        this.predicateCollector = new HashMap<>();
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs) || outputs.get(outputs.size()-1).getName().startsWith("query_set"), "assignments does not cover all of outputs");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        this.predicate = requireNonNull(predicate, "predicate expression cannot be empty");
        this.strategy = strategy;
        this.reuseTableScanMappingId = reuseTableScanMappingId;
        this.filterExpr = null;
        this.consumerTableScanNodeCount = consumerTableScanNodeCount;
        this.forDelete = forDelete;
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            Long querySet,
            Map<Integer, String> predicateCollector,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<RowExpression> predicate,
            ReuseExchangeOperator.STRATEGY strategy,
            UUID reuseTableScanMappingId,
            Integer consumerTableScanNodeCount,
            boolean forDelete)
    {
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.querySet = querySet;
        this.predicateCollector = predicateCollector;
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs) || outputs.get(outputs.size()-1).getName().startsWith("query_set"), "assignments does not cover all of outputs");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        this.predicate = requireNonNull(predicate, "predicate expression cannot be empty");
        this.strategy = strategy;
        this.reuseTableScanMappingId = reuseTableScanMappingId;
        this.filterExpr = null;
        this.consumerTableScanNodeCount = consumerTableScanNodeCount;
        this.forDelete = forDelete;
    }

    public RowExpression getFilterExpr()
    {
        return filterExpr;
    }

    public void setFilterExpr(RowExpression filterExpr)
    {
        this.filterExpr = filterExpr;
    }

    public void setStrategy(ReuseExchangeOperator.STRATEGY strategy)
    {
        this.strategy = strategy;
    }

    public void setReuseTableScanMappingId(UUID reuseTableScanMappingId)
    {
        this.reuseTableScanMappingId = reuseTableScanMappingId;
    }

    public void addPredicate(Integer queryId, String predicate)
    {
        predicateCollector.put(queryId, predicate);
    }

    @JsonProperty("predicateCollector")
    public Map<Integer, String> getPredicateCollector()
    {
        return predicateCollector;
    }

    @JsonProperty("querySet")
    public Long getQuerySet()
    {
        return querySet;
    }

    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    @JsonProperty("forDelete")
    public boolean isForDelete()
    {
        return forDelete;
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("assignments")
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @JsonProperty("strategy")
    public ReuseExchangeOperator.STRATEGY getStrategy()
    {
        return strategy;
    }

    @JsonProperty("reuseTableScanMappingId")
    public UUID getReuseTableScanMappingId()
    {
        return reuseTableScanMappingId;
    }

    public void setConsumerTableScanNodeCount(Integer consumerTableScanNodeCount)
    {
        this.consumerTableScanNodeCount = consumerTableScanNodeCount;
    }

    @JsonProperty("consumerTableScanNodeCount")
    public Integer getConsumerTableScanNodeCount()
    {
        return consumerTableScanNodeCount;
    }

    /**
     * A TupleDomain that represents a predicate that has been successfully pushed into
     * this TableScan node. In other words, predicates that were removed from filters
     * above the TableScan node because the TableScan node can guarantee it.
     * <p>
     * This field is used to make sure that predicates which were previously pushed down
     * do not get lost in subsequent refinements of the table layout.
     */
    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        // enforcedConstraint can be pretty complex. As a result, it may incur a significant cost to serialize, store, and transport.
        checkState(enforcedConstraint != null, "enforcedConstraint should only be used in planner. It is not transported to workers.");
        return enforcedConstraint;
    }

    @JsonProperty("predicate")
    public Optional<RowExpression> getPredicate()
    {
        return predicate;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("outputSymbols", outputSymbols)
                .add("assignments", assignments)
                .add("enforcedConstraint", enforcedConstraint)
                .add("strategy", strategy)
                .add("reuseTableScanMappingId", reuseTableScanMappingId)
                .add("consumerTableScanNodeCount", consumerTableScanNodeCount)
                .toString();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }

    public boolean isSourcesEqual(List<PlanNode> n1, List<PlanNode> n2)
    {
        if (n1.size() != n2.size()) {
            return false;
        }
        int count = 0;
        for (PlanNode p1 : n1) {
            for (PlanNode p2 : n2) {
                if (p1.equals(p2)) {
                    count++;
                    break;
                }
            }
        }
        if (count == n1.size()) {
            return true;
        }
        return false;
    }

    public boolean isSymbolsEqual(List<Symbol> s1, List<Symbol> s2)
    {
        if (s1 == null && s2 == null) {
            return true;
        }
        if (null == s1 || null == s2 || s1.size() != s2.size()) {
            return false;
        }
        // Convert RegularImmutableList to list
        ArrayList<Symbol> ar1 = new ArrayList<>(s1);
        ArrayList<Symbol> ar2 = new ArrayList<>(s2);
        Collections.sort(ar1);
        Collections.sort(ar2);

        for (int i = 0; i < ar1.size(); i++) {
            String st1 = getActualColName(ar1.get(i).getName());
            String st2 = getActualColName(ar2.get(i).getName());
            if (!st1.equalsIgnoreCase(st2)) {
                return false;
            }
        }
        return true;
    }

    public static String getActualColName(String var)
    {
        // TODO: Instead of stripping off _, we can get corresponding name from assigments column mapping.
        int index = var.lastIndexOf("_");
        if (index == -1 || isInteger(var.substring(index + 1)) == false) {
            return var;
        }
        else {
            return var.substring(0, index);
        }
    }

    private static boolean isInteger(String st)
    {
        try {
            Integer.parseInt(st);
        }
        catch (NumberFormatException ex) {
            return false;
        }

        return true;
    }

    public boolean isPredicateSame(TableScanNode curr)
    {
        boolean returnValue = false;
        if (filterExpr != null) {
            returnValue = filterExpr.absEquals(curr.getFilterExpr());
        }
        else if (curr.getFilterExpr() == null) {
            returnValue = true;
        }

        if (returnValue) {
            if (predicate.isPresent()) {
                return predicate.get().absEquals(curr.getPredicate().get());
            }
            return true;
        }
        return false;
    }
}
