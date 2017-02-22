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
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.ScalarQueryUtil.isResolvedScalarSubquery;
import static com.facebook.presto.sql.planner.optimizations.ScalarQueryUtil.isScalar;
import static com.facebook.presto.util.Types.tryCast;

/**
 * Remove resolved ApplyNodes with unreferenced scalar input, e.g: "SELECT (SELECT 1)".
 */
public class RemoveUnreferencedScalarInputApplyNodes
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode planNode, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        return tryCast(planNode, ApplyNode.class)
                .filter(applyNode -> canBeReplacedBySubquery(applyNode, lookup))
                .map(applyNode -> lookup.resolve(applyNode.getSubquery()));
    }

    public boolean canBeReplacedBySubquery(ApplyNode applyNode, Lookup lookup)
    {
        PlanNode input = lookup.resolve(applyNode.getInput());
        return input.getOutputSymbols().isEmpty() && isScalar(input) && isResolvedScalarSubquery(applyNode, lookup::resolve);
    }
}
