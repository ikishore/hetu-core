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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.prestosql.Session;
import io.prestosql.metadata.AbstractMockMetadata;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableProperties;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.plan.AggregationNode.globalAggregation;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.ExpressionUtils.and;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ExpressionUtils.or;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestEffectivePredicateExtractor
{
    private static final Symbol A = new Symbol("a");
    private static final Symbol B = new Symbol("b");
    private static final Symbol C = new Symbol("c");
    private static final Symbol D = new Symbol("d");
    private static final Symbol E = new Symbol("e");
    private static final Symbol F = new Symbol("f");
    private static final Symbol G = new Symbol("g");
    private static final Expression AE = toSymbolReference(A);
    private static final Expression BE = toSymbolReference(B);
    private static final Expression CE = toSymbolReference(C);
    private static final Expression DE = toSymbolReference(D);
    private static final Expression EE = toSymbolReference(E);
    private static final Expression FE = toSymbolReference(F);
    private static final Expression GE = toSymbolReference(G);
    private static final Session SESSION = TestingSession.testSessionBuilder().build();

    private final Metadata metadata = new AbstractMockMetadata()
    {
        private final Metadata delegate = createTestMetadataManager();

        @Override
        public Type getType(TypeSignature signature)
        {
            return delegate.getType(signature);
        }

        @Override
        public Signature resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
        {
            return ((BuiltInFunctionHandle) delegate.getFunctionAndTypeManager().resolveFunction(Optional.empty(), QualifiedObjectName.valueOf(name.toString()), parameterTypes)).getSignature();
        }

        public FunctionAndTypeManager getFunctionAndTypeManager()
        {
            return delegate.getFunctionAndTypeManager();
        }

        @Override
        public TableProperties getTableProperties(Session session, TableHandle handle)
        {
            return new TableProperties(new CatalogName("test"), TestingConnectorTransactionHandle.INSTANCE, new ConnectorTableProperties(
                    ((PredicatedTableHandle) handle.getConnectorHandle()).getPredicate(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableList.of()));
        }
    };

    private final TypeAnalyzer typeAnalyzer = new TypeAnalyzer(new SqlParser(), metadata);
    private final EffectivePredicateExtractor effectivePredicateExtractor = new EffectivePredicateExtractor(new ExpressionDomainTranslator(new LiteralEncoder(metadata)), metadata, true);
    private final EffectivePredicateExtractor effectivePredicateExtractorWithoutTableProperties = new EffectivePredicateExtractor(new ExpressionDomainTranslator(new LiteralEncoder(metadata)), metadata, false);

    private Map<Symbol, ColumnHandle> scanAssignments;
    private TableScanNode baseTableScan;
    private ExpressionIdentityNormalizer expressionNormalizer;

    @BeforeMethod
    public void setUp()
    {
        scanAssignments = ImmutableMap.<Symbol, ColumnHandle>builder()
                .put(A, new TestingColumnHandle("a"))
                .put(B, new TestingColumnHandle("b"))
                .put(C, new TestingColumnHandle("c"))
                .put(D, new TestingColumnHandle("d"))
                .put(E, new TestingColumnHandle("e"))
                .put(F, new TestingColumnHandle("f"))
                .build();

        Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C, D, E, F)));
        baseTableScan = TableScanNode.newInstance(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0),
                0,
                false);

        expressionNormalizer = new ExpressionIdentityNormalizer();
    }

    @Test
    public void testAggregation()
    {
        PlanNode node = new AggregationNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, DE),
                                equals(BE, EE),
                                equals(CE, FE),
                                lessThan(DE, bigintLiteral(10)),
                                lessThan(CE, DE),
                                greaterThan(AE, bigintLiteral(2)),
                                equals(EE, FE))),
                ImmutableMap.of(
                        C, new Aggregation(
                                new CallExpression("test",
                                        new FunctionResolution(metadata.getFunctionAndTypeManager()).countFunction(),
                                        BIGINT,
                                        ImmutableList.of()),
                                ImmutableList.of(),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        D, new Aggregation(
                                new CallExpression("test",
                                        new FunctionResolution(metadata.getFunctionAndTypeManager()).countFunction(),
                                        BIGINT,
                                        ImmutableList.of()),
                                ImmutableList.of(),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty())),
                singleGroupingSet(ImmutableList.of(A, B, C)),
                ImmutableList.of(),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty(),
                AggregationNode.AggregationType.HASH,
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Rewrite in terms of group by symbols
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(
                        lessThan(AE, bigintLiteral(10)),
                        lessThan(BE, AE),
                        greaterThan(AE, bigintLiteral(2)),
                        equals(BE, CE)));
    }

    @Test
    public void testGroupByEmpty()
    {
        PlanNode node = new AggregationNode(
                newId(),
                filter(baseTableScan, FALSE_LITERAL),
                ImmutableMap.of(),
                globalAggregation(),
                ImmutableList.of(),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty(),
                AggregationNode.AggregationType.HASH,
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        assertEquals(effectivePredicate, TRUE_LITERAL);
    }

    @Test
    public void testFilter()
    {
        PlanNode node = filter(baseTableScan,
                and(
                        greaterThan(AE, new FunctionCallBuilder(metadata)
                                .setName(QualifiedName.of("rand"))
                                .build()),
                        lessThan(BE, bigintLiteral(10))));

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Non-deterministic functions should be purged
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, bigintLiteral(10))));
    }

    @Test
    public void testProject()
    {
        PlanNode node = new ProjectNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                assignment(D, AE, E, CE));

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Rewrite in terms of project output symbols
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(
                        lessThan(DE, bigintLiteral(10)),
                        equals(DE, EE)));
    }

    @Test
    public void testTopN()
    {
        PlanNode node = new TopNNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                1, new OrderingScheme(ImmutableList.of(A), ImmutableMap.of(A, SortOrder.ASC_NULLS_LAST)), TopNNode.Step.PARTIAL);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testLimit()
    {
        PlanNode node = new LimitNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                1,
                false);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testSort()
    {
        PlanNode node = new SortNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                new OrderingScheme(ImmutableList.of(A), ImmutableMap.of(A, SortOrder.ASC_NULLS_LAST)),
                false);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testWindow()
    {
        PlanNode node = new WindowNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                new WindowNode.Specification(
                        ImmutableList.of(A),
                        Optional.of(new OrderingScheme(
                                ImmutableList.of(A),
                                ImmutableMap.of(A, SortOrder.ASC_NULLS_LAST)))),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testTableScan()
    {
        // Effective predicate is True if there is no effective predicate
        Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C, D)));
        PlanNode node = TableScanNode.newInstance(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments, ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);
        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);
        node = new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.none()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.none(), Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(effectivePredicate, FALSE_LITERAL);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(scanAssignments.get(A), Domain.singleValue(BIGINT, 1L)));
        node = new TableScanNode(
                newId(),
                makeTableHandle(predicate),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                predicate, Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(bigintLiteral(1L), AE)));
        predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                scanAssignments.get(A), Domain.singleValue(BIGINT, 1L),
                scanAssignments.get(B), Domain.singleValue(BIGINT, 2L)));
        node = new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.withColumnDomains(ImmutableMap.of(scanAssignments.get(A), Domain.singleValue(BIGINT, 1L)))),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                predicate, Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);
        effectivePredicate = effectivePredicateExtractorWithoutTableProperties.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjunctsSet(equals(bigintLiteral(2L), BE), equals(bigintLiteral(1L), AE)));

        node = new TableScanNode(
                newId(),
                makeTableHandle(predicate),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0), 0, false);
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(effectivePredicate, and(equals(AE, bigintLiteral(1)), equals(BE, bigintLiteral(2))));

        node = new TableScanNode(
                newId(),
                makeTableHandle(predicate),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        scanAssignments.get(A), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)),
                        scanAssignments.get(B), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)))),
                Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT,
                new UUID(0, 0), 0, false);
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjunctsSet(equals(bigintLiteral(2L), BE), equals(bigintLiteral(1L), AE)));

        node = new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(), Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);
    }

    @Test
    public void testValues()
    {
        TypeProvider types = TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
                .put(A, BIGINT)
                .put(B, BIGINT)
                .build());

        // one column
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteralRowExpression(1)),
                                ImmutableList.of(bigintLiteralRowExpression(2)))),
                types,
                typeAnalyzer),
                new InPredicate(AE, new InListExpression(ImmutableList.of(bigintLiteral(1), bigintLiteral(2)))));

        // one column with null
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteralRowExpression(1)),
                                ImmutableList.of(bigintLiteralRowExpression(2)),
                                ImmutableList.of(castToRowExpression(new Cast(new NullLiteral(), BIGINT.toString()))))),
                types,
                typeAnalyzer),
                or(
                        new InPredicate(AE, new InListExpression(ImmutableList.of(bigintLiteral(1), bigintLiteral(2)))),
                        new IsNullPredicate(AE)));

        // all nulls
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A),
                        ImmutableList.of(
                                ImmutableList.of(castToRowExpression(new Cast(new NullLiteral(), BIGINT.toString()))))),
                types,
                typeAnalyzer),
                new IsNullPredicate(AE));

        // many rows
        List<List<RowExpression>> rows = IntStream.range(0, 500)
                .mapToObj(TestEffectivePredicateExtractor::bigintLiteralRowExpression)
                .map(ImmutableList::of)
                .collect(toImmutableList());
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A),
                        rows),
                types,
                typeAnalyzer),
                new BetweenPredicate(AE, bigintLiteral(0), bigintLiteral(499)));

        // multiple columns
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A, B),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteralRowExpression(1), bigintLiteralRowExpression(100)),
                                ImmutableList.of(bigintLiteralRowExpression(2), bigintLiteralRowExpression(200)))),
                types,
                typeAnalyzer),
                and(
                        new InPredicate(AE, new InListExpression(ImmutableList.of(bigintLiteral(1), bigintLiteral(2)))),
                        new InPredicate(BE, new InListExpression(ImmutableList.of(bigintLiteral(100), bigintLiteral(200))))));

        // multiple columns with null
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A, B),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteralRowExpression(1), castToRowExpression(new Cast(new NullLiteral(), BIGINT.toString()))),
                                ImmutableList.of(castToRowExpression(new Cast(new NullLiteral(), BIGINT.toString())), bigintLiteralRowExpression(200)))),
                types,
                typeAnalyzer),
                and(
                        or(new ComparisonExpression(EQUAL, AE, bigintLiteral(1)), new IsNullPredicate(AE)),
                        or(new ComparisonExpression(EQUAL, BE, bigintLiteral(200)), new IsNullPredicate(BE))));

        // non-deterministic
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A, B),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteralRowExpression(1), castToRowExpression(new FunctionCall(QualifiedName.of("rand"), ImmutableList.of()))))),
                types,
                typeAnalyzer),
                new ComparisonExpression(EQUAL, AE, bigintLiteral(1)));

        // non-constant
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteralRowExpression(1)),
                                ImmutableList.of(castToRowExpression(BE)))),
                types,
                typeAnalyzer),
                TRUE_LITERAL);
    }

    @Test
    public void testUnion()
    {
        ImmutableListMultimap<Symbol, Symbol> symbolMapping = ImmutableListMultimap.of(A, B, A, C, A, E);
        PlanNode node = new UnionNode(newId(),
                ImmutableList.of(
                        filter(baseTableScan, greaterThan(AE, bigintLiteral(10))),
                        filter(baseTableScan, and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100)))),
                        filter(baseTableScan, and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100))))),
                symbolMapping,
                ImmutableList.copyOf(symbolMapping.keySet()));

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Only the common conjuncts can be inferred through a Union
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(greaterThan(AE, bigintLiteral(10))));
    }

    @Test
    public void testInnerJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));

        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.INNER,
                left,
                right,
                criteria,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.of(castToRowExpression(lessThanOrEqual(BE, EE))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // All predicates having output symbol should be carried through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100)),
                        equals(AE, DE),
                        equals(BE, EE),
                        lessThanOrEqual(BE, EE)));
    }

    @Test
    public void testInnerJoinPropagatesPredicatesViaEquiConditions()
    {
        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan, equals(AE, bigintLiteral(10)));

        // predicates on "a" column should be propagated to output symbols via join equi conditions
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.INNER,
                left,
                rightScan,
                ImmutableList.of(new JoinNode.EquiJoinClause(A, D)),
                ImmutableList.<Symbol>builder()
                        .addAll(rightScan.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        assertEquals(
                normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(equals(DE, bigintLiteral(10))));
    }

    @Test
    public void testInnerJoinWithFalseFilter()
    {
        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new JoinNode.EquiJoinClause(A, D)),
                ImmutableList.<Symbol>builder()
                        .addAll(leftScan.getOutputSymbols())
                        .addAll(rightScan.getOutputSymbols())
                        .build(),
                Optional.of(castToRowExpression(FALSE_LITERAL)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        assertEquals(effectivePredicate, FALSE_LITERAL);
    }

    @Test
    public void testLeftJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.LEFT,
                left,
                right,
                criteria,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // All right side symbols having output symbols should be checked against NULL
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        or(equals(DE, EE), and(isNull(DE), isNull(EE))),
                        or(lessThan(FE, bigintLiteral(100)), isNull(FE)),
                        or(equals(AE, DE), isNull(DE)),
                        or(equals(BE, EE), isNull(EE))));
    }

    @Test
    public void testLeftJoinWithFalseInner()
    {
        List<JoinNode.EquiJoinClause> criteria = ImmutableList.of(new JoinNode.EquiJoinClause(A, D));

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan, FALSE_LITERAL);
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.LEFT,
                left,
                right,
                criteria,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // False literal on the right side should be ignored
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        or(equals(AE, DE), isNull(DE))));
    }

    @Test
    public void testRightJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.RIGHT,
                left,
                right,
                criteria,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // All left side symbols should be checked against NULL
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(or(lessThan(BE, AE), and(isNull(BE), isNull(AE))),
                        or(lessThan(CE, bigintLiteral(10)), isNull(CE)),
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100)),
                        or(equals(AE, DE), isNull(AE)),
                        or(equals(BE, EE), isNull(BE))));
    }

    @Test
    public void testRightJoinWithFalseInner()
    {
        List<JoinNode.EquiJoinClause> criteria = ImmutableList.of(new JoinNode.EquiJoinClause(A, D));

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan, FALSE_LITERAL);
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.RIGHT,
                left,
                right,
                criteria,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // False literal on the left side should be ignored
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjunctsSet(equals(DE, EE),
                        lessThan(FE, bigintLiteral(100)),
                        or(equals(AE, DE), isNull(AE))));
    }

    @Test
    public void testSemiJoin()
    {
        PlanNode node = new SemiJoinNode(newId(),
                filter(baseTableScan, and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100)))),
                filter(baseTableScan, greaterThan(AE, bigintLiteral(5))),
                A, B, C,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Currently, only pull predicates through the source plan
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100)))));
    }

    private static TableScanNode tableScanNode(Map<Symbol, ColumnHandle> scanAssignments)
    {
        return new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(scanAssignments.keySet()),
                scanAssignments,
                TupleDomain.all(), Optional.empty(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, new UUID(0, 0), 0, false);
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }

    private static FilterNode filter(PlanNode source, Expression predicate)
    {
        return new FilterNode(newId(), source, castToRowExpression(predicate));
    }

    private static Expression bigintLiteral(long number)
    {
        if (number < Integer.MAX_VALUE && number > Integer.MIN_VALUE) {
            return new GenericLiteral("BIGINT", String.valueOf(number));
        }
        return new LongLiteral(String.valueOf(number));
    }

    private static RowExpression bigintLiteralRowExpression(long number)
    {
        if (number < Integer.MAX_VALUE && number > Integer.MIN_VALUE) {
            return castToRowExpression(new GenericLiteral("BIGINT", String.valueOf(number)));
        }
        return castToRowExpression(new LongLiteral(String.valueOf(number)));
    }

    private static ComparisonExpression equals(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(EQUAL, expression1, expression2);
    }

    private static ComparisonExpression lessThan(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, expression1, expression2);
    }

    private static ComparisonExpression lessThanOrEqual(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, expression1, expression2);
    }

    private RowExpression lessThanOrEqual(RowExpression expression1, RowExpression expression2)
    {
        return PlanBuilder.comparison(OperatorType.LESS_THAN_OR_EQUAL, expression1, expression2);
    }

    private static ComparisonExpression greaterThan(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, expression1, expression2);
    }

    private static IsNullPredicate isNull(Expression expression)
    {
        return new IsNullPredicate(expression);
    }

    private Set<Expression> normalizeConjunctsSet(Expression... conjuncts)
    {
        return normalizeConjunctsCollection(Arrays.asList(conjuncts));
    }

    private Set<Expression> normalizeConjunctsCollection(Collection<Expression> conjuncts)
    {
        return normalizeConjuncts(combineConjuncts(conjuncts));
    }

    private Set<Expression> normalizeConjuncts(Expression predicate)
    {
        // Normalize the predicate by identity so that the EqualityInference will produce stable rewrites in this test
        // and thereby produce comparable Sets of conjuncts from this method.
        Expression tmpPredicate = expressionNormalizer.normalize(predicate);

        // Equality inference rewrites and equality generation will always be stable across multiple runs in the same JVM
        EqualityInference inference = EqualityInference.createEqualityInference(tmpPredicate);

        Set<Expression> rewrittenSet = new HashSet<>();
        for (Expression expression : EqualityInference.nonInferrableConjuncts(tmpPredicate)) {
            Expression rewritten = inference.rewriteExpression(expression, Predicates.alwaysTrue());
            Preconditions.checkState(rewritten != null, "Rewrite with full symbol scope should always be possible");
            rewrittenSet.add(rewritten);
        }
        rewrittenSet.addAll(inference.generateEqualitiesPartitionedBy(Predicates.alwaysTrue()).getScopeEqualities());

        return rewrittenSet;
    }

    private static TableHandle makeTableHandle(TupleDomain<ColumnHandle> predicate)
    {
        return new TableHandle(
                new CatalogName("test"),
                new PredicatedTableHandle(predicate),
                TestingTransactionHandle.create(),
                Optional.empty());
    }

    /**
     * Normalizes Expression nodes (and all sub-expressions) by identity.
     * <p>
     * Identity equality of Expression nodes is necessary for EqualityInference to generate stable rewrites
     * (as specified by Ordering.arbitrary())
     */
    private static class ExpressionIdentityNormalizer
    {
        private final Map<Expression, Expression> expressionCache = new HashMap<>();

        private Expression normalize(Expression expression)
        {
            Expression identityNormalizedExpression = expressionCache.get(expression);
            if (identityNormalizedExpression == null) {
                // Make sure all sub-expressions are normalized first
                for (Expression subExpression : Iterables.filter(SubExpressionExtractor.extract(expression), Predicates.not(Predicates.equalTo(expression)))) {
                    normalize(subExpression);
                }

                // Since we have not seen this expression before, rewrite it entirely in terms of the normalized sub-expressions
                identityNormalizedExpression = ExpressionTreeRewriter.rewriteWith(new ExpressionNodeInliner(expressionCache), expression);
                expressionCache.put(identityNormalizedExpression, identityNormalizedExpression);
            }
            return identityNormalizedExpression;
        }
    }

    private static class PredicatedTableHandle
            implements ConnectorTableHandle
    {
        private final TupleDomain<ColumnHandle> predicate;

        public PredicatedTableHandle(TupleDomain<ColumnHandle> predicate)
        {
            this.predicate = predicate;
        }

        public TupleDomain<ColumnHandle> getPredicate()
        {
            return predicate;
        }
    }
}
