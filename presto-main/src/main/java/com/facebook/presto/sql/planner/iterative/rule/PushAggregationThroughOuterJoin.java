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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.sql.planner.ExpressionSymbolInliner;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.SymbolReferenceExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.DistinctOutputQueryUtil.isDistinct;
import static com.google.common.base.Preconditions.checkState;

public class PushAggregationThroughOuterJoin
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode aggregation = (AggregationNode) node;
        PlanNode source = lookup.resolve(aggregation.getSource());
        if (!(source instanceof JoinNode)) {
            return Optional.empty();
        }
        JoinNode join = (JoinNode) source;
        if (join.getFilter().isPresent()
                || !(join.getType() == JoinNode.Type.LEFT || join.getType() == JoinNode.Type.RIGHT)
                || !groupsOnAllOuterTableColumns(aggregation, getOuterTable(join))
                || !isDistinct(getOuterTable(join))) {
            return Optional.empty();
        }

        AggregationNode rewrittenAggregation = new AggregationNode(
                node.getId(),
                getInnerTable(join),
                aggregation.getAssignments(),
                ImmutableList.of(ImmutableList.of()),
                aggregation.getStep(),
                aggregation.getHashSymbol(),
                aggregation.getGroupIdSymbol());

        JoinNode rewrittenJoin;
        if (join.getType() == JoinNode.Type.LEFT) {
            ImmutableList.Builder<JoinNode.EquiJoinClause> equiJoinClauses = ImmutableList.builder();
            for (JoinNode.EquiJoinClause equiJoinClause : join.getCriteria()) {
                equiJoinClauses.add(new JoinNode.EquiJoinClause(equiJoinClause.getLeft(), getParentExpression(rewrittenAggregation, equiJoinClause.getRight())));
            }
            rewrittenJoin = new JoinNode(
                    join.getId(),
                    join.getType(),
                    join.getLeft(),
                    rewrittenAggregation,
                    equiJoinClauses.build(),
                    ImmutableList.<Symbol>builder()
                            .addAll(join.getLeft().getOutputSymbols())
                            .addAll(rewrittenAggregation.getOutputSymbols())
                            .build(),
                    join.getFilter(),
                    join.getLeftHashSymbol(),
                    join.getRightHashSymbol());
        }
        else {
            ImmutableList.Builder<JoinNode.EquiJoinClause> equiJoinClauses = ImmutableList.builder();
            for (JoinNode.EquiJoinClause equiJoinClause : join.getCriteria()) {
                equiJoinClauses.add(new JoinNode.EquiJoinClause(getParentExpression(rewrittenAggregation, equiJoinClause.getRight()), equiJoinClause.getRight()));
            }
            rewrittenJoin = new JoinNode(
                    join.getId(),
                    join.getType(),
                    rewrittenAggregation,
                    join.getRight(),
                    equiJoinClauses.build(),
                    ImmutableList.<Symbol>builder()
                            .addAll(rewrittenAggregation.getOutputSymbols())
                            .addAll(join.getRight().getOutputSymbols())
                            .build(),
                    join.getFilter(),
                    join.getLeftHashSymbol(),
                    join.getRightHashSymbol());
        }

        return Optional.of(coalesceWithNullAggregation(rewrittenAggregation, rewrittenJoin, symbolAllocator, idAllocator));
    }

    private Symbol getParentExpression(AggregationNode aggregationNode, Symbol symbol)
    {
        for (Map.Entry<Symbol, AggregationNode.Aggregation> assignment : aggregationNode.getAssignments().entrySet()) {
            if (SymbolReferenceExtractor.extract(assignment.getValue().getCall()).contains(symbol.toSymbolReference())) {
                return assignment.getKey();
            }
        }
        throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, String.format("Symbol %s not found in function inputs %s", symbol, aggregationNode.getAssignments().values()));
    }

    private PlanNode getInnerTable(JoinNode join)
    {
        checkState(join.getType() == JoinNode.Type.LEFT || join.getType() == JoinNode.Type.RIGHT, "expected LEFT or RIGHT JOIN");
        PlanNode innerNode;
        if (join.getType().equals(JoinNode.Type.LEFT)) {
            innerNode = join.getRight();
        }
        else {
            innerNode = join.getLeft();
        }
        return innerNode;
    }

    private static PlanNode getOuterTable(JoinNode join)
    {
        checkState(join.getType() == JoinNode.Type.LEFT || join.getType() == JoinNode.Type.RIGHT, "expected LEFT or RIGHT JOIN");
        PlanNode outerNode;
        if (join.getType().equals(JoinNode.Type.LEFT)) {
            outerNode = join.getLeft();
        }
        else {
            outerNode = join.getRight();
        }
        return outerNode;
    }

    private static boolean groupsOnAllOuterTableColumns(AggregationNode node, PlanNode outerTable)
    {
        return new HashSet<>(node.getGroupingKeys()).equals(new HashSet<>(outerTable.getOutputSymbols()));
    }

    // When the aggregation is done after the join, there will be a null value that gets aggregated over
    // where rows did not exist in the inner table.  For some aggregate functions, such as count, the result
    // of an aggregation over a single null row is one or zero rather than null. In order to ensure correct results,
    // we add a coalesce function with the output of the new outer join and the agggregation performed over a single
    // null row.
    private PlanNode coalesceWithNullAggregation(AggregationNode aggregationNode, PlanNode outerJoin, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        // Create an aggregation node over a row of nulls.
        MappedAggregationInfo aggregationOverNullInfo = createAggregationOverNull(aggregationNode, symbolAllocator, idAllocator);
        AggregationNode aggregationOverNull = aggregationOverNullInfo.getAggregation();
        Map<Symbol, Symbol> sourceAggregationToOverNullMapping = aggregationOverNullInfo.getSymbolMapping();

        // Do a cross join with the aggregation over null
        JoinNode crossJoin = new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                outerJoin,
                aggregationOverNull,
                ImmutableList.of(),
                ImmutableList.<Symbol>builder()
                        .addAll(outerJoin.getOutputSymbols())
                        .addAll(aggregationOverNull.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // Add coalesce expressions for all aggregation functions
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        for (Symbol symbol : outerJoin.getOutputSymbols()) {
            if (aggregationNode.getAssignments().containsKey(symbol)) {
                assignmentsBuilder.put(symbol, new CoalesceExpression(symbol.toSymbolReference(), sourceAggregationToOverNullMapping.get(symbol).toSymbolReference()));
            }
            else {
                assignmentsBuilder.put(symbol, symbol.toSymbolReference());
            }
        }
        return new ProjectNode(idAllocator.getNextId(), crossJoin, assignmentsBuilder.build());
    }

    private MappedAggregationInfo createAggregationOverNull(AggregationNode referenceAggregation, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        // Create a values node that consists of a single row of nulls.
        // Map the output symbols from the referenceAggregation's source
        // to symbol references for the new values node.
        NullLiteral nullLiteral = new NullLiteral();
        ImmutableList.Builder<Symbol> nullSymbols = ImmutableList.builder();
        ImmutableList.Builder<Expression> nullLiterals = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, SymbolReference> sourcesSymbolMappingBuilder = ImmutableMap.builder();
        for (Symbol sourceSymbol : referenceAggregation.getSource().getOutputSymbols()) {
            nullLiterals.add(nullLiteral);
            Symbol nullSymbol = symbolAllocator.newSymbol(nullLiteral, symbolAllocator.getTypes().get(sourceSymbol));
            nullSymbols.add(nullSymbol);
            sourcesSymbolMappingBuilder.put(sourceSymbol, nullSymbol.toSymbolReference());
        }
        ValuesNode nullRow = new ValuesNode(
                idAllocator.getNextId(),
                nullSymbols.build(),
                ImmutableList.of(nullLiterals.build()));
        Map<Symbol, SymbolReference> sourcesSymbolMapping = sourcesSymbolMappingBuilder.build();

        // For each aggregation function in the reference node, create a corresponding aggregation function
        // that points to the nullRow. Map the symbols from the aggregations in referenceAggregation to the
        // symbols in these new aggregations.
        ImmutableMap.Builder<Symbol, Symbol> aggregationsSymbolMappingBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregationsOverNullBuilder = ImmutableMap.builder();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : referenceAggregation.getAssignments().entrySet()) {
            AggregationNode.Aggregation overNullAggregation = new AggregationNode.Aggregation(
                    ExpressionTreeRewriter.rewriteWith(new ExpressionSymbolInliner(sourcesSymbolMapping), entry.getValue().getCall()),
                    entry.getValue().getSignature(),
                    entry.getValue().getMask().map(x -> Symbol.from(sourcesSymbolMapping.get(x))));
            Symbol overNullSymbol = symbolAllocator.newSymbol(overNullAggregation.getCall(), symbolAllocator.getTypes().get(entry.getKey()));
            aggregationsOverNullBuilder.put(overNullSymbol, overNullAggregation);
            aggregationsSymbolMappingBuilder.put(entry.getKey(), overNullSymbol);
        }
        Map<Symbol, Symbol> aggregationsSymbolMapping = aggregationsSymbolMappingBuilder.build();

        // create an aggregation node whose source is the null row.
        AggregationNode aggregationOverNullRow = new AggregationNode(
                idAllocator.getNextId(),
                nullRow,
                aggregationsOverNullBuilder.build(),
                ImmutableList.of(ImmutableList.of()),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty()
        );
        return new MappedAggregationInfo(aggregationOverNullRow, aggregationsSymbolMapping);
    }

    private static class MappedAggregationInfo
    {
        private final AggregationNode aggregationNode;
        private final Map<Symbol, Symbol> symbolMapping;

        public MappedAggregationInfo(AggregationNode aggregationNode, Map<Symbol, Symbol> symbolMapping)
        {
            this.aggregationNode = aggregationNode;
            this.symbolMapping = symbolMapping;
        }

        public Map<Symbol, Symbol> getSymbolMapping()
        {
            return symbolMapping;
        }

        public AggregationNode getAggregation()
        {
            return aggregationNode;
        }
    }
}
