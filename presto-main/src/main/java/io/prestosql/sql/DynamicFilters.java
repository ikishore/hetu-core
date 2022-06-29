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
package io.prestosql.sql;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.planner.FunctionCallBuilder;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static io.prestosql.expressions.LogicalRowExpressions.extractAllPredicates;
import static io.prestosql.expressions.LogicalRowExpressions.extractConjuncts;
import static io.prestosql.expressions.LogicalRowExpressions.extractDisjuncts;
import static io.prestosql.expressions.LogicalRowExpressions.extractPredicates;
import static io.prestosql.spi.relation.SpecialForm.Form.AND;
import static io.prestosql.spi.relation.SpecialForm.Form.OR;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.relational.Expressions.call;
import static java.util.Objects.requireNonNull;

public final class DynamicFilters
{
    private DynamicFilters() {}

    public static Expression createDynamicFilterExpression(Metadata metadata, String id, Type inputType, SymbolReference input)
    {
        return createDynamicFilterExpression(metadata, id, inputType, input, Optional.empty());
    }

    public static Expression createDynamicFilterExpression(Metadata metadata, String id, Type inputType, SymbolReference input, Optional<Expression> filter)
    {
        return new FunctionCallBuilder(metadata)
                .setName(QualifiedName.of(Function.NAME))
                .addArgument(VarcharType.VARCHAR, new StringLiteral(id))
                .addArgument(inputType, input)
                .setFilter(filter)
                .build();
    }

    public static RowExpression createDynamicFilterRowExpression(Session session, Metadata metadata, TypeManager typeManager, String id, Type inputType, SymbolReference input, Optional<RowExpression> filter)
    {
        ConstantExpression string = new ConstantExpression(utf8Slice(id), VarcharType.VARCHAR);
        VariableReferenceExpression expression = new VariableReferenceExpression(input.getName(), inputType);
        FunctionHandle handle = metadata.getFunctionAndTypeManager().resolveFunction(session.getTransactionId(), QualifiedObjectName.valueOfDefaultFunction(Function.NAME), fromTypes(VarcharType.VARCHAR, inputType));
        return call(Function.NAME, handle, string.getType(), Arrays.asList(string, expression), filter);
    }

    public static ExtractResult extractDynamicFilters(RowExpression expression)
    {
        List<RowExpression> conjuncts = extractConjuncts(expression);

        ImmutableList.Builder<RowExpression> staticConjuncts = ImmutableList.builder();
        ImmutableList.Builder<Descriptor> dynamicConjuncts = ImmutableList.builder();

        for (RowExpression conjunct : conjuncts) {
            Optional<Descriptor> descriptor = getDescriptor(conjunct);
            if (descriptor.isPresent()) {
                dynamicConjuncts.add(descriptor.get());
            }
            else {
                staticConjuncts.add(conjunct);
            }
        }

        return new ExtractResult(staticConjuncts.build(), dynamicConjuncts.build());
    }

    public static RowExpression extractDynamicFilterExpression(RowExpression expression, Metadata metadata)
    {
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata),
                new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
        if (expression instanceof SpecialForm) {
            switch (((SpecialForm) expression).getForm()) {
                case AND:
                    return logicalRowExpressions.combineConjuncts((extractPredicates(AND, expression)
                            .stream()
                            .map(exp -> extractDynamicFilterExpression(exp, metadata))
                            .collect(Collectors.toList())));
                case OR:
                    return logicalRowExpressions.combineDisjuncts((extractPredicates(OR, expression)
                            .stream()
                            .map(exp -> extractDynamicFilterExpression(exp, metadata))
                            .collect(Collectors.toList())));
                default:
                    return TRUE_CONSTANT;
            }
        }
        else if (expression instanceof CallExpression) {
            if (getDescriptor(expression).isPresent()) {
                return expression;
            }
        }
        return TRUE_CONSTANT;
    }

    public static Optional<RowExpression> extractStaticFilters(Optional<RowExpression> expression, Metadata metadata)
    {
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata),
                new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
        if (expression.isPresent()) {
            List<RowExpression> filters = extractConjuncts(expression.get());
            RowExpression staticFilters = TRUE_CONSTANT;
            for (RowExpression filter : filters) {
                List<RowExpression> predicates = extractAllPredicates(filter);
                for (RowExpression predicate : predicates) {
                    if (!getDescriptor(predicate).isPresent()) {
                        staticFilters = logicalRowExpressions.combineConjuncts(staticFilters, filter);
                    }
                }
            }
            return staticFilters.equals(TRUE_CONSTANT) ? Optional.empty() : Optional.of(staticFilters);
        }
        else {
            return Optional.empty();
        }
    }

    public static List<List<Descriptor>> extractDynamicFiltersAsUnion(Optional<RowExpression> filterExpression, Metadata metadata)
    {
        if (!filterExpression.isPresent()) {
            return ImmutableList.of();
        }
        RowExpression expression = extractDynamicFilterExpression(filterExpression.get(), metadata);
        List<List<Descriptor>> dynamicFilterList = new ArrayList<>();
        List<RowExpression> predicates = extractDisjuncts(expression);

        for (RowExpression predicate : predicates) {
            List<Descriptor> dynamicFilters = new ArrayList<>();
            List<RowExpression> conjuncts = extractConjuncts(predicate);
            for (RowExpression conjunct : conjuncts) {
                Optional<Descriptor> descriptor = getDescriptor(conjunct);
                if (descriptor.isPresent()) {
                    dynamicFilters.add(descriptor.get());
                }
            }
            if (!dynamicFilters.isEmpty()) {
                dynamicFilterList.add(dynamicFilters);
            }
        }
        return dynamicFilterList;
    }

    public static boolean isDynamicFilter(RowExpression expression)
    {
        return getDescriptor(expression).isPresent();
    }

    public static Optional<Descriptor> getDescriptor(RowExpression expression)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression callExpression = (CallExpression) expression;

        if (!callExpression.getDisplayName().equals(Function.NAME)) {
            return Optional.empty();
        }

        List<RowExpression> arguments = callExpression.getArguments();
        checkArgument(arguments.size() == 2, "invalid arguments count: %s", arguments.size());

        RowExpression firstArgument = arguments.get(0);
        checkArgument(firstArgument instanceof ConstantExpression, "firstArgument is expected to be an instance of ConstantExpression: %s", firstArgument.getClass().getSimpleName());
        Object firstArgumentValue = ((ConstantExpression) firstArgument).getValue();
        String id = (firstArgumentValue instanceof String) ? (String) (firstArgumentValue) : ((Slice) (firstArgumentValue)).toStringUtf8();
        return Optional.of(new Descriptor(id, arguments.get(1), callExpression.getFilter())); /* Fixme(Nitin): Resolve the filter expression from the dynamic filter */
    }

    public static Optional<Predicate<List>> createDynamicFilterPredicate(Optional<RowExpression> filter)
    {
        if (filter.isPresent()) {
            if (filter.get() instanceof CallExpression) {
                CallExpression call = (CallExpression) filter.get();
                BuiltInFunctionHandle builtInFunctionHandle = (BuiltInFunctionHandle) call.getFunctionHandle();
                String name = builtInFunctionHandle.getSignature().getNameSuffix();
                if (name.contains("$operator$") && Signature.unmangleOperator(name).isComparisonOperator()) {
                    if (call.getArguments().get(1) instanceof VariableReferenceExpression &&
                            call.getArguments().get(0) instanceof VariableReferenceExpression) {
                        switch (Signature.unmangleOperator(name)) {
                            case LESS_THAN:
                                return Optional.of((values) -> {
                                    Object probeValue = values.get(0);
                                    Object buildValue = values.get(1);
                                    if (!(probeValue instanceof Long) || !(buildValue instanceof Long)) {
                                        return true;
                                    }
                                    Long probeLiteral = (Long) probeValue;
                                    Long buildLiteral = (Long) buildValue;
                                    return probeLiteral.compareTo(buildLiteral) < 0;
                                });
                            case LESS_THAN_OR_EQUAL:
                                return Optional.of((values) -> {
                                    Object probeValue = values.get(0);
                                    Object buildValue = values.get(1);
                                    if (!(probeValue instanceof Long) || !(buildValue instanceof Long)) {
                                        return true;
                                    }
                                    Long probeLiteral = (Long) probeValue;
                                    Long buildLiteral = (Long) buildValue;
                                    return probeLiteral.compareTo(buildLiteral) <= 0;
                                });
                            case GREATER_THAN:
                                return Optional.of((values) -> {
                                    Object probeValue = values.get(0);
                                    Object buildValue = values.get(1);
                                    if (!(probeValue instanceof Long) || !(buildValue instanceof Long)) {
                                        return true;
                                    }
                                    Long probeLiteral = (Long) probeValue;
                                    Long buildLiteral = (Long) buildValue;
                                    return probeLiteral.compareTo(buildLiteral) > 0;
                                });
                            case GREATER_THAN_OR_EQUAL:
                                return Optional.of((values) -> {
                                    Object probeValue = values.get(0);
                                    Object buildValue = values.get(1);
                                    if (!(probeValue instanceof Long) || !(buildValue instanceof Long)) {
                                        return true;
                                    }
                                    Long probeLiteral = (Long) probeValue;
                                    Long buildLiteral = (Long) buildValue;
                                    return probeLiteral.compareTo(buildLiteral) >= 0;
                                });
                            default:
                                return Optional.empty();
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }

    public static class ExtractResult
    {
        private final List<RowExpression> staticConjuncts;
        private final List<Descriptor> dynamicConjuncts;

        public ExtractResult(List<RowExpression> staticConjuncts, List<Descriptor> dynamicConjuncts)
        {
            this.staticConjuncts = ImmutableList.copyOf(requireNonNull(staticConjuncts, "staticConjuncts is null"));
            this.dynamicConjuncts = ImmutableList.copyOf(requireNonNull(dynamicConjuncts, "dynamicConjuncts is null"));
        }

        public List<RowExpression> getStaticConjuncts()
        {
            return staticConjuncts;
        }

        public List<Descriptor> getDynamicConjuncts()
        {
            return dynamicConjuncts;
        }
    }

    public static final class Descriptor
    {
        private final String id;
        private final RowExpression input;
        private final Optional<RowExpression> filter;

        public Descriptor(String id, RowExpression input)
        {
            this(id, input, Optional.empty());
        }

        public Descriptor(String id, RowExpression input, Optional<RowExpression> filter)
        {
            this.id = requireNonNull(id, "id is null");
            this.input = requireNonNull(input, "input is null");
            this.filter = requireNonNull(filter, "filter is null");
        }

        public String getId()
        {
            return id;
        }

        public RowExpression getInput()
        {
            return input;
        }

        public Optional<RowExpression> getFilter()
        {
            return filter;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Descriptor that = (Descriptor) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(input, that.input) &&
                    Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, input, filter);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .add("input", input)
                    .add("filter", filter)
                    .toString();
        }
    }

    @ScalarFunction(value = Function.NAME, hidden = true, deterministic = true)
    public static final class Function
    {
        private Function() {}

        public static final String NAME = "$internal$dynamic_filter_function";

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") Block input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") Slice input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") long input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") boolean input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") double input)
        {
            throw new UnsupportedOperationException();
        }
    }
}
