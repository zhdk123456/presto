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
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer can rewrite correlated single row subquery to projection in a way described here:
 * From:
 * <pre>
 * - Apply (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B]
 *   - (subquery)
 *     - Project (A + C)
 *       - single row VALUES()
 * </pre>
 * to:
 * <pre>
 *   - Project(A + C)
 *       - (input) plan which produces symbols: [A, B]
 * </pre>
 */
public class TransformCorrelatedSingleRowSubqueryToJoin
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            Map<Symbol, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitApply(ApplyNode apply, RewriteContext<PlanNode> context)
        {
            ApplyNode rewrittenApply = (ApplyNode) context.defaultRewrite(apply, context.get());
            if (!rewrittenApply.getCorrelation().isEmpty()) {
                Optional<ValuesNode> values = PlanNodeSearcher.searchFrom(apply.getSubquery())
                        .skipOnlyWhen(ProjectNode.class::isInstance)
                        .where(ValuesNode.class::isInstance)
                        .findSingle();

                if (values.isPresent() && isNoColumnSingleRowValues(values.get())) {
                    List<ProjectNode> projections = PlanNodeSearcher.searchFrom(apply.getSubquery())
                            .where(ProjectNode.class::isInstance)
                            .findAll();

                    if (projections.size() == 0) {
                        return rewrittenApply.getInput();
                    }
                    else {
                        if (projections.size() == 1) {
                            // todo merge projections if size > 1
                            Map<Symbol, Expression> assignments = ImmutableMap.<Symbol, Expression>builder()
                                    .putAll(toAssignments(rewrittenApply.getInput().getOutputSymbols()))
                                    .putAll(projections.get(0).getAssignments())
                                    .build();
                            return new ProjectNode(
                                    idAllocator.getNextId(),
                                    rewrittenApply.getInput(),
                                    assignments);
                        }
                    }
                }
            }
            return rewrittenApply;
        }

        private static boolean isNoColumnSingleRowValues(ValuesNode values)
        {
            return values.getRows().size() == 1 && values.getRows().get(0).size() == 0;
        }

        private static Map<Symbol, Expression> toAssignments(Collection<Symbol> symbols)
        {
            return symbols.stream()
                    .collect(toImmutableMap(s -> s, Symbol::toSymbolReference));
        }
    }
}
