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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.ScalarQueryUtil.isResolvedScalarSubqueryOf;
import static com.facebook.presto.util.Optionals.cast;

public class TransformUncorrelatedScalarToJoin
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        return cast(node, ApplyNode.class)
                .flatMap(applyNode -> {
                    PlanNode input = lookup.resolve(applyNode.getInput());
                    PlanNode subquery = lookup.resolve(applyNode.getSubquery());
                    return replaceIfPossible(idAllocator, applyNode, input, subquery);
                });
    }

    private Optional<PlanNode> replaceIfPossible(PlanNodeIdAllocator idAllocator, ApplyNode applyNode, PlanNode input, PlanNode subquery)
    {
        if (canBeReplacedByScalarJoin(applyNode, subquery)) {
            return Optional.of(createReplacementJoinNode(idAllocator, input, subquery));
        }
        else {
            return Optional.empty();
        }
    }

    private boolean canBeReplacedByScalarJoin(ApplyNode applyNode, PlanNode subquery)
    {
        return applyNode.getCorrelation().isEmpty() && isResolvedScalarSubqueryOf(applyNode, subquery);
    }

    private PlanNode createReplacementJoinNode(PlanNodeIdAllocator idAllocator, PlanNode input, PlanNode subquery)
    {
        return new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                input,
                subquery,
                ImmutableList.of(),
                ImmutableList.<Symbol>builder()
                        .addAll(input.getOutputSymbols())
                        .addAll(subquery.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
