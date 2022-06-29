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
package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class TableDeleteNode
        extends InternalPlanNode
{
    private final TableHandle target;
    private final Symbol output;
    private final PlanNode source;
    private final Optional<RowExpression> filter;
    private final Map<Symbol, ColumnHandle> assignments;

    public TableDeleteNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("target") TableHandle target,
            @JsonProperty("output") Symbol output)
    {
        this(id, null, Optional.empty(), target, ImmutableMap.of(), output);
    }

    @JsonCreator
    public TableDeleteNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("filter") Optional<RowExpression> filter,
            @JsonProperty("target") TableHandle target,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments,
            @JsonProperty("output") Symbol output)
    {
        super(id);
        this.source = source;
        this.filter = requireNonNull(filter, "filter is null");
        this.target = requireNonNull(target, "target is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
        this.output = requireNonNull(output, "output is null");
    }

    @JsonProperty
    public TableHandle getTarget()
    {
        return target;
    }

    @JsonProperty
    public Symbol getOutput()
    {
        return output;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.of(output);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Optional<RowExpression> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @Override
    public List<PlanNode> getSources()
    {
        if (source == null) {
            return ImmutableList.of();
        }
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableDelete(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        if (newChildren.isEmpty()) {
            return new TableDeleteNode(getId(), target, output);
        }
        return new TableDeleteNode(getId(), Iterables.getOnlyElement(newChildren), filter, target, assignments, output);
    }
}
