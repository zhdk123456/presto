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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.metadata.GlobalProperties;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.sql.planner.optimizations.ScalarQueryUtil.isScalar;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DetermineJoinDistributionType
        implements PlanOptimizer
{
    private final CostCalculator costCalculator;
    private final GlobalProperties globalProperties;
    private final int nodeCount;

    public DetermineJoinDistributionType(CostCalculator costCalculator, GlobalProperties globalProperties, int nodeCount)
    {
        this.costCalculator = requireNonNull(costCalculator, "statisticsCalculator is null");
        this.globalProperties = requireNonNull(globalProperties, "globalProperties is null");
        this.nodeCount = nodeCount;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(session, globalProperties, costCalculator, types, nodeCount), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        // arbitrary estimate of a "typical" row size in bytes for when you don't have stats on the size of your data
        private static final int ROW_SIZE_ESTIMATE = 50;

        private final Session session;
        private final GlobalProperties globalProperties;
        private final CostCalculator costCalculator;
        private final Map<Symbol, Type> types;
        private final int nodeCount;
        private boolean isDeleteQuery;

        public Rewriter(Session session, GlobalProperties globalProperties, CostCalculator costCalculator, Map<Symbol, Type> types, int nodeCount)
        {
            this.session = session;
            this.globalProperties = globalProperties;
            this.costCalculator = costCalculator;
            this.types = types;
            this.nodeCount = nodeCount;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode leftRewritten = context.rewrite(node.getLeft(), context.get());
            PlanNode rightRewritten = context.rewrite(node.getRight(), context.get());
            JoinNode.DistributionType targetJoinDistributionType = getTargetJoinDistributionType(node);

            JoinNode rewritten = new JoinNode(
                    node.getId(),
                    node.getType(),
                    leftRewritten,
                    rightRewritten,
                    node.getCriteria(),
                    node.getOutputSymbols(),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    Optional.of(targetJoinDistributionType));

            // Check if flipping the join allows you to replicate
            JoinNode flipped = flipJoin(rewritten);
            if (getJoinDistributionType(session).equals(FeaturesConfig.JoinDistributionType.AUTOMATIC)
                    && targetJoinDistributionType == JoinNode.DistributionType.PARTITIONED
                    && getTargetJoinDistributionType(flipped) == JoinNode.DistributionType.REPLICATED) {
                rewritten = new JoinNode(
                        flipped.getId(),
                        flipped.getType(),
                        flipped.getLeft(),
                        flipped.getRight(),
                        flipped.getCriteria(),
                        flipped.getOutputSymbols(),
                        flipped.getFilter(),
                        flipped.getLeftHashSymbol(),
                        flipped.getRightHashSymbol(),
                        Optional.of(getTargetJoinDistributionType(flipped)));
            }

            return rewritten;
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Void> context)
        {
            PlanNode sourceRewritten = context.rewrite(node.getSource(), context.get());
            PlanNode filteringSourceRewritten = context.rewrite(node.getFilteringSource(), context.get());
            SemiJoinNode.DistributionType targetJoinDistributionType = getTargetSemiJoinDistributionType(node, isDeleteQuery);
            return new SemiJoinNode(
                    node.getId(),
                    sourceRewritten,
                    filteringSourceRewritten,
                    node.getSourceJoinSymbol(),
                    node.getFilteringSourceJoinSymbol(),
                    node.getSemiJoinOutput(),
                    node.getSourceHashSymbol(),
                    node.getFilteringSourceHashSymbol(),
                    Optional.of(targetJoinDistributionType));
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Void> context)
        {
            // For delete queries, the TableScan node that corresponds to the table being deleted must be collocated with the Delete node,
            // so you can't do a distributed semi-join
            isDeleteQuery = true;
            PlanNode rewrittenSource = context.rewrite(node.getSource());
            return new DeleteNode(
                    node.getId(),
                    rewrittenSource,
                    node.getTarget(),
                    node.getRowId(),
                    node.getOutputSymbols());
        }

        private JoinNode.DistributionType getTargetJoinDistributionType(JoinNode node)
        {
            // join distribution type forced by limitation of implementation
            if (mustPartitionJoin(node)) {
                return JoinNode.DistributionType.PARTITIONED;
            }
            if (mustReplicateJoin(node)) {
                return JoinNode.DistributionType.REPLICATED;
            }

            // join distribution type forced by session variable
            if (getJoinDistributionType(session).equals(FeaturesConfig.JoinDistributionType.REPARTITIONED)) {
                return JoinNode.DistributionType.PARTITIONED;
            }
            if (!mustPartitionJoin(node) && getJoinDistributionType(session).equals(FeaturesConfig.JoinDistributionType.REPLICATED)) {
                return JoinNode.DistributionType.REPLICATED;
            }

            // Choose based on stats.
            if (isSmall(node.getRight()) && isMuchSmaller(node.getRight(), node.getLeft())) {
                return JoinNode.DistributionType.REPLICATED;
            }

            return JoinNode.DistributionType.PARTITIONED;
        }

        private static boolean mustPartitionJoin(JoinNode node)
        {
            // The implementation of full outer join only works if the data is hash partitioned. See LookupJoinOperators#buildSideOuterJoinUnvisitedPositions
            return node.getType() == RIGHT || node.getType() == FULL;
        }

        private static boolean mustReplicateJoin(JoinNode node)
        {
            return isScalar(node.getRight()) || isCrossJoin(node);
        }

        private static boolean isCrossJoin(JoinNode node)
        {
            return node.getType() == INNER && node.getCriteria().isEmpty();
        }

        private SemiJoinNode.DistributionType getTargetSemiJoinDistributionType(SemiJoinNode semiJoinNode, boolean isDeleteQuery)
        {
            // For delete queries, the TableScan node that corresponds to the table being deleted must be collocated with the Delete node,
            // so you can't do a distributed semi-join
            if (isDeleteQuery) {
                return SemiJoinNode.DistributionType.REPLICATED;
            }

            // join distribution type forced by session variable
            if (getJoinDistributionType(session).equals(FeaturesConfig.JoinDistributionType.REPLICATED)) {
                return SemiJoinNode.DistributionType.REPLICATED;
            }
            if (getJoinDistributionType(session).equals(FeaturesConfig.JoinDistributionType.REPARTITIONED)) {
                return SemiJoinNode.DistributionType.PARTITIONED;
            }

            // choose based on stats
            if (isSmall(semiJoinNode.getFilteringSource())) {
                return SemiJoinNode.DistributionType.REPLICATED;
            }
            return SemiJoinNode.DistributionType.PARTITIONED;
        }

        private boolean isSmall(PlanNode node)
        {
            double smallSizeLimit = SystemSessionProperties.getSmallTableCoefficient(session) * globalProperties.getMaxMemoryPerNode().toBytes();
            Estimate dataSize = getOutputSizeEstimate(node);
            return !dataSize.isValueUnknown() && dataSize.getValue() < smallSizeLimit;
        }

        private boolean isMuchSmaller(PlanNode node, PlanNode other)
        {
            Estimate nodeDataSize = getOutputSizeEstimate(node);
            Estimate otherDataSize = getOutputSizeEstimate(other);
            return !nodeDataSize.isValueUnknown() && !otherDataSize.isValueUnknown() && nodeDataSize.getValue() * nodeCount < otherDataSize.getValue();
        }

        private Estimate getOutputSizeEstimate(PlanNode node)
        {
            Estimate dataSize = costCalculator.calculateCostForNode(session, types, node).getOutputSizeInBytes();
            if (!dataSize.isValueUnknown()) {
                return dataSize;
            }

            dataSize = costCalculator.calculateCostForNode(session, types, node).getOutputRowCount();
            if (!dataSize.isValueUnknown()) {
                dataSize = new Estimate(dataSize.getValue() * ROW_SIZE_ESTIMATE);
            }
            return dataSize;
        }

        private JoinNode flipJoin(JoinNode node)
        {
            ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
            outputSymbolsBuilder.addAll(node.getRight().getOutputSymbols());
            outputSymbolsBuilder.addAll(node.getLeft().getOutputSymbols());

            List<Symbol> outputSymbols = outputSymbolsBuilder.build().stream().filter(symbol -> node.getOutputSymbols().contains(symbol)).collect(toImmutableList());
            return new JoinNode(
                    node.getId(),
                    flipJoinType(node.getType()),
                    node.getRight(),
                    node.getLeft(),
                    flipJoinCriteria(node.getCriteria()),
                    outputSymbols,
                    node.getFilter(),
                    node.getRightHashSymbol(),
                    node.getLeftHashSymbol(),
                    node.getDistributionType()
            );
        }

        private JoinNode.Type flipJoinType(JoinNode.Type joinType)
        {
            switch (joinType) {
                case LEFT:
                    return JoinNode.Type.RIGHT;
                case RIGHT:
                    return JoinNode.Type.LEFT;
                case INNER:
                case FULL:
                    return joinType;
                default:
                    throw new IllegalArgumentException("unknown joinType: " + joinType);
            }
        }

        private List<JoinNode.EquiJoinClause> flipJoinCriteria(List<JoinNode.EquiJoinClause> criteria)
        {
            return criteria.stream()
                    .map(clause -> new JoinNode.EquiJoinClause(clause.getRight(), clause.getLeft()))
                    .collect(toImmutableList());
        }
    }
}
