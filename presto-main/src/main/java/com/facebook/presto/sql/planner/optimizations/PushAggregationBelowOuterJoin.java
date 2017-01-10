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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.ExpressionSymbolInliner;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NullLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.DistinctOutputQueryUtil.isDistinct;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.Predicates.isInstanceOfAny;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer pushes aggregations below outer joins when: the aggregation
 * is on top of the outer join, it groups by all columns in the outer table, and
 * the outer rows are guaranteed to be distinct.
 * <p>
 * When the aggregation is pushed down, we still need to perform aggregations
 * on the null values that come out of the absent values in an outer
 * join. We add a cross join with a row of aggregations on null literals,
 * and coalesce the aggregation that results from the left outer join with
 * the result of the aggregation over nulls.
 * <p>
 * Example:
 * <pre>
 * - Filter ("nationkey" > "avg")
 *  - Aggregate(Group by: all columns from the left talbe, aggregation:
 *    avg("n2.nationkey"))
 *      - LeftJoin("regionkey" = "regionkey")
 *          - AssignUniqueId (nation)
 *              - Tablescan (nation)
 *          - Tablescan (nation)
 * </pre>
 * </p>
 * Is rewritten to:
 * <pre>
 * - Filter ("nationkey" > "avg")
 *  - project(regionkey, coalesce("avg", "avg_over_null")
 *      - CrossJoin
 *          - LeftJoin("regionkey" = "regionkey")
 *              - AssignUniqueId (nation)
 *                  - Tablescan (nation)
 *              - Aggregate(Group by: regionkey, aggregation:
 *                avg(nationkey))
 *                  - Tablescan (nation)
 *          - Aggregate
 *            avg(null_literal)
 *              - Values (null_literal)
 * </pre>
 */
public class PushAggregationBelowOuterJoin
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, symbolAllocator), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllcoator;

        public Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = idAllocator;
            this.symbolAllcoator = symbolAllocator;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            Optional<JoinNode> joinNodeOptional = getFirstJoinNode(node.getSource());
            if (!joinNodeOptional.isPresent()) {
                return context.defaultRewrite(node, context.get());
            }

            JoinNode joinSource = joinNodeOptional.get();
            if (joinSource.getType() == JoinNode.Type.LEFT
                    && !joinSource.getFilter().isPresent()
                    && groupsOnAllOuterTableColumns(node, joinSource)
                    && isDistinct(joinSource.getLeft())) {
                List<Symbol> groupingKeys = joinSource.getCriteria().stream()
                        .map(JoinNode.EquiJoinClause::getRight)
                        .collect(toImmutableList());
                AggregationNode aggregationNode = new AggregationNode(
                        node.getId(),
                        context.defaultRewrite(joinSource.getRight()),
                        node.getAggregations(),
                        node.getFunctions(),
                        node.getMasks(),
                        ImmutableList.of(groupingKeys),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol());
                PlanNode rewrittenLeft = context.defaultRewrite(joinSource.getLeft());
                PlanNode joinNode = new JoinNode(
                        joinSource.getId(),
                        joinSource.getType(),
                        rewrittenLeft,
                        aggregationNode,
                        joinSource.getCriteria(),
                        ImmutableList.<Symbol>builder().addAll(rewrittenLeft.getOutputSymbols()).addAll(aggregationNode.getOutputSymbols()).build(),
                        joinSource.getFilter(),
                        joinSource.getLeftHashSymbol(),
                        joinSource.getRightHashSymbol(),
                        Optional.empty());

                return coalesceWithNullAggregation(aggregationNode, joinNode);
            }

            if (joinSource.getType() == JoinNode.Type.RIGHT
                    && !joinSource.getFilter().isPresent()
                    && groupsOnAllOuterTableColumns(node, joinSource)
                    && isDistinct(joinSource.getRight())) {
                List<Symbol> groupingKeys = joinSource.getCriteria().stream()
                        .map(JoinNode.EquiJoinClause::getLeft)
                        .collect(toImmutableList());
                AggregationNode aggregationNode = new AggregationNode(
                        node.getId(),
                        context.defaultRewrite(joinSource.getLeft()),
                        node.getAggregations(),
                        node.getFunctions(),
                        node.getMasks(),
                        ImmutableList.of(groupingKeys),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol());
                PlanNode rewrittenRight = context.defaultRewrite(joinSource.getRight());
                JoinNode joinNode = new JoinNode(
                        joinSource.getId(),
                        joinSource.getType(),
                        aggregationNode,
                        rewrittenRight,
                        joinSource.getCriteria(),
                        ImmutableList.<Symbol>builder().addAll(aggregationNode.getOutputSymbols()).addAll(rewrittenRight.getOutputSymbols()).build(),
                        joinSource.getFilter(),
                        joinSource.getLeftHashSymbol(),
                        joinSource.getRightHashSymbol(),
                        Optional.empty());
                return coalesceWithNullAggregation(aggregationNode, joinNode);
            }
            return context.defaultRewrite(node, context.get());
        }

        // When the aggregation is done after the join, there will be a null value that gets aggregated over
        // where rows did not exist in the inner table.  For some aggregate functions, such as count, the result
        // of an aggregation over a single null row is one or zero rather than null. In order to ensure correct results,
        // we add a coalesce function with the output of the new outer join and the agggregation performed over a single
        // null row.
        private PlanNode coalesceWithNullAggregation(AggregationNode aggregationNode, PlanNode outerJoin)
        {
            NullLiteral nullLiteral = new NullLiteral();
            ImmutableList.Builder<Symbol> nullSymbols = ImmutableList.builder();
            ImmutableList.Builder<Expression> nullLiterals = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, Expression> aggregationSourceToValuesMapping = ImmutableMap.builder();
            for (Symbol sourceSymbol : aggregationNode.getSource().getOutputSymbols()) {
                nullLiterals.add(nullLiteral);
                Symbol nullSymbol = symbolAllcoator.newSymbol(nullLiteral, symbolAllcoator.getTypes().get(sourceSymbol));
                nullSymbols.add(nullSymbol);
                aggregationSourceToValuesMapping.put(sourceSymbol, nullSymbol.toSymbolReference());
            }
            ValuesNode values = new ValuesNode(
                    idAllocator.getNextId(),
                    nullSymbols.build(),
                    ImmutableList.of(nullLiterals.build()));

            ImmutableMap.Builder<Symbol, Symbol> basicAggregationToOverNullMappingBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, FunctionCall> aggregationsOverNullBuilder = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : aggregationNode.getAggregations().entrySet()) {
                FunctionCall overNullFunction = ExpressionTreeRewriter.rewriteWith(new ExpressionSymbolInliner(aggregationSourceToValuesMapping.build()), entry.getValue());
                Symbol overNullSymbol = symbolAllcoator.newSymbol(overNullFunction, symbolAllcoator.getTypes().get(entry.getKey()));
                aggregationsOverNullBuilder.put(overNullSymbol, overNullFunction);
                basicAggregationToOverNullMappingBuilder.put(entry.getKey(), overNullSymbol);
            }

            ImmutableMap<Symbol, Symbol> basicAggregationToOverNullMapping = basicAggregationToOverNullMappingBuilder.build();
            AggregationNode aggregationOverNullNode = new AggregationNode(
                    idAllocator.getNextId(),
                    values,
                    aggregationsOverNullBuilder.build(),
                    aggregationNode.getFunctions().entrySet().stream()
                            .collect(toImmutableMap(entry -> basicAggregationToOverNullMapping.get(entry.getKey()), Map.Entry::getValue)),
                    aggregationNode.getMasks(),
                    ImmutableList.of(ImmutableList.of()),
                    aggregationNode.getStep(),
                    aggregationNode.getHashSymbol(),
                    aggregationNode.getGroupIdSymbol()
            );

            JoinNode crossJoin = new JoinNode(
                    idAllocator.getNextId(),
                    JoinNode.Type.INNER,
                    outerJoin,
                    aggregationOverNullNode,
                    ImmutableList.of(),
                    ImmutableList.<Symbol>builder().addAll(outerJoin.getOutputSymbols()).addAll(aggregationOverNullNode.getOutputSymbols()).build(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            Assignments.Builder assignmentsBuilder = Assignments.builder();
            for (Symbol symbol : outerJoin.getOutputSymbols()) {
                if (aggregationNode.getAggregations().containsKey(symbol)) {
                    assignmentsBuilder.put(symbol, new CoalesceExpression(symbol.toSymbolReference(), basicAggregationToOverNullMapping.get(symbol).toSymbolReference()));
                }
                else {
                    assignmentsBuilder.put(symbol, symbol.toSymbolReference());
                }
            }
            return new ProjectNode(idAllocator.getNextId(), crossJoin, assignmentsBuilder.build());
        }

        private static boolean groupsOnAllOuterTableColumns(AggregationNode node, JoinNode joinSource)
        {
            checkState(joinSource.getType() == JoinNode.Type.LEFT || joinSource.getType() == JoinNode.Type.RIGHT, "expected LEFT or RIGHT JOIN");
            PlanNode outerNode;
            if (joinSource.getType().equals(JoinNode.Type.LEFT)) {
                outerNode = joinSource.getLeft();
            }
            else {
                outerNode = joinSource.getRight();
            }
            return new HashSet<>(node.getGroupingKeys()).equals(new HashSet<>(outerNode.getOutputSymbols()));
        }

        private static Optional<JoinNode> getFirstJoinNode(PlanNode node)
        {
            return searchFrom(node)
                    .skipOnlyWhen(Rewriter::isIdentityProject)
                    .where(isInstanceOfAny(JoinNode.class))
                    .findFirst();
        }

        private static boolean isIdentityProject(PlanNode planNode)
        {
            return (planNode instanceof ProjectNode) && ((ProjectNode) planNode).isIdentity();
        }
    }
}
