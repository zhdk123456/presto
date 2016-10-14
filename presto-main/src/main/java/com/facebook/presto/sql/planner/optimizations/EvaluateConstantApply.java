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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Evaluates a constant Apply expression. For example:
 * <p>
 * apply(values(a, b), r -> values(x, y))
 * <p>
 * into
 * <p>
 * values((a, x), (a, y), (b, x), (b, y))
 */
public class EvaluateConstantApply
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Void> context)
        {
            Optional<ValuesNode> outerValues = searchFrom(node.getInput())
                    .skipOnlyWhen(ProjectNode.class::isInstance)
                    .where(ValuesNode.class::isInstance)
                    .findFirst();

            List<ProjectNode> outerProjection = searchFrom(node.getInput())
                    .where(ProjectNode.class::isInstance)
                    .findAll();

            Optional<ValuesNode> innerValues = searchFrom(node.getSubquery())
                    .skipOnlyWhen(ProjectNode.class::isInstance)
                    .where(ValuesNode.class::isInstance)
                    .findFirst();

            List<ProjectNode> innerProjection = searchFrom(node.getSubquery())
                    .where(ProjectNode.class::isInstance)
                    .findAll();

            if (!outerValues.isPresent() || !innerValues.isPresent()) {
                return context.defaultRewrite(node);
            }

            checkState(
                    outerProjection.size() < 2 && innerProjection.size() < 2,
                    "This rule should be run after MergeProjections");

            // semantics of apply are similar to a cross join
            PlanNode result = crossJoin(outerValues.get(), innerValues.get());

            if (outerProjection.size() == 1) {
                result = new ProjectNode(
                        idAllocator.getNextId(),
                        result,
                        mergeAssignments(outerProjection.get(0).getAssignments(), toAssignments(result.getOutputSymbols())));
            }

            if (innerProjection.size() == 1) {
                result = new ProjectNode(
                        idAllocator.getNextId(),
                        result,
                        mergeAssignments(innerProjection.get(0).getAssignments(), toAssignments(result.getOutputSymbols())));
            }

            return result;
        }

        private ValuesNode crossJoin(ValuesNode outer, ValuesNode inner)
        {
            ImmutableList.Builder<List<Expression>> resultValuesExpressions = ImmutableList.builder();

            for (List<Expression> outerRow : outer.getRows()) {
                for (List<Expression> innerRow : inner.getRows()) {
                    resultValuesExpressions.add(ImmutableList.<Expression>builder()
                            .addAll(outerRow)
                            .addAll(innerRow)
                            .build());
                }
            }

            return new ValuesNode(
                    idAllocator.getNextId(),
                    ImmutableList.<Symbol>builder()
                            .addAll(outer.getOutputSymbols())
                            .addAll(inner.getOutputSymbols())
                            .build(),
                    resultValuesExpressions.build());
        }

        private static Map<Symbol, Expression> toAssignments(Collection<Symbol> symbols)
        {
            return symbols.stream()
                    .collect(toImmutableMap(s -> s, Symbol::toSymbolReference));
        }

        private static Map<Symbol, Expression> mergeAssignments(
                Map<Symbol, Expression> first,
                Map<Symbol, Expression> second)
        {
            Map<Symbol, Expression> result = new HashMap<>();
            result.putAll(first);
            second.entrySet().forEach(entry -> {
                Symbol key = entry.getKey();
                Expression value = entry.getValue();
                if (result.containsKey(key)) {
                    checkState(
                            result.get(key).equals(value),
                            "Cannot merge assignments as the same symbol %s points to different expressions: %s, %s",
                            key,
                            result.get(key),
                            value);
                }
                else {
                    result.put(key, value);
                }
            });
            return ImmutableMap.copyOf(result);
        }
    }
}
