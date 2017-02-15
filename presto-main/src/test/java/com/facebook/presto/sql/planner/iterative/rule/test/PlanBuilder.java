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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static java.lang.String.format;

public class PlanBuilder
{
    private final PlanNodeIdAllocator idAllocator;
    private final Map<Symbol, Type> symbols = new HashMap<>();

    public PlanBuilder(PlanNodeIdAllocator idAllocator)
    {
        this.idAllocator = idAllocator;
    }

    public ValuesNode values(Symbol... columns)
    {
        return new ValuesNode(
                idAllocator.getNextId(),
                ImmutableList.copyOf(columns),
                ImmutableList.of());
    }

    public ValuesNode values(List<Symbol> columns, List<List<Expression>> rows)
    {
        return new ValuesNode(idAllocator.getNextId(), columns, rows);
    }

    public LimitNode limit(long limit, PlanNode source)
    {
        return new LimitNode(idAllocator.getNextId(), source, limit, false);
    }

    public SampleNode sample(double sampleRatio, SampleNode.Type type, PlanNode source)
    {
        return new SampleNode(idAllocator.getNextId(), source, sampleRatio, type);
    }

    public SortNode sort(List<Symbol> orderBy, PlanNode source)
    {
        return new SortNode(
                idAllocator.getNextId(),
                source,
                orderBy,
                orderBy.stream().collect(Collectors.toMap(Function.identity(), symbol -> ASC_NULLS_LAST)));
    }

    public SortNode sort(List<Symbol> orderBy, Map<Symbol, SortOrder> orderings, PlanNode source)
    {
        return new SortNode(idAllocator.getNextId(), source, orderBy, orderings);
    }

    public ProjectNode project(Assignments assignments, PlanNode source)
    {
        return new ProjectNode(idAllocator.getNextId(), source, assignments);
    }

    public FilterNode filter(Expression predicate, PlanNode source)
    {
        return new FilterNode(idAllocator.getNextId(), source, predicate);
    }

    public ApplyNode apply(Assignments subqueryAssignments, List<Symbol> correlation, PlanNode input, PlanNode subquery)
    {
        return new ApplyNode(idAllocator.getNextId(), input, subquery, subqueryAssignments, correlation);
    }

    public AssignUniqueId assignUniqueId(Symbol uniqueId, PlanNode source)
    {
        return new AssignUniqueId(idAllocator.getNextId(), source, uniqueId);
    }

    public Symbol symbol(String name, Type type)
    {
        Symbol symbol = new Symbol(name);

        Type old = symbols.get(symbol);
        if (old != null && !old.equals(type)) {
            throw new IllegalArgumentException(format("Symbol '%s' already registered with type '%s'", name, old));
        }

        if (old == null) {
            symbols.put(symbol, type);
        }

        return symbol;
    }

    public static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql));
    }

    public static List<Expression> expressions(String... expressions)
    {
        return Stream.of(expressions)
                .map(PlanBuilder::expression)
                .collect(ImmutableCollectors.toImmutableList());
    }

    public Map<Symbol, Type> getSymbols()
    {
        return Collections.unmodifiableMap(symbols);
    }
}
