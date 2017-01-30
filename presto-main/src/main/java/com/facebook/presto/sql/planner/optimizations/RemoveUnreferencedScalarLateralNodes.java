
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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.Map;

import static com.facebook.presto.sql.planner.optimizations.ScalarQueryUtil.isScalar;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;

/**
 * Remove LateralJoinNodes with unreferenced scalar input, e.g: "SELECT (SELECT 1)".
 */
public class RemoveUnreferencedScalarLateralNodes
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        @Override
        public PlanNode visitLateralJoin(LateralJoinNode node, RewriteContext<PlanNode> context)
        {
            if (node.getInput().getOutputSymbols().isEmpty() && isScalar(node.getInput())) {
                return context.rewrite(node.getSubquery());
            }

            if (node.getSubquery().getOutputSymbols().isEmpty() && isScalar(node.getSubquery())) {
                return context.rewrite(node.getInput());
            }

            return context.defaultRewrite(node);
        }
    }
}
