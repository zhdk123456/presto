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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.optimizations.joins.JoinGraph;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestCrossJoinElimination
{
    PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testJoinOrder()
    {
        PlanNode plan =
                join(
                        join(
                                new AnyPlanNode(new PlanNodeId("A"), symbol("a")),
                                new AnyPlanNode(new PlanNodeId("B"), symbol("b"))),
                        new AnyPlanNode(new PlanNodeId("C"), symbol("c")),
                        symbol("a"), symbol("c"),
                        symbol("c"), symbol("b"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                CrossJoinsElimination.getJoinOrder(joinGraph),
                Optional.of(ImmutableList.of(0, 2, 1)));
    }

    @Test
    public void testJoinOrderWithMultipleEdgesBetweenNodes()
    {
        PlanNode plan =
                join(
                        join(
                                new AnyPlanNode(new PlanNodeId("A"), symbol("a")),
                                new AnyPlanNode(new PlanNodeId("B"), symbol("b1"), symbol("b2"))),
                        new AnyPlanNode(new PlanNodeId("C"), symbol("c1"), symbol("c2")),
                        symbol("a"), symbol("c1"),
                        symbol("c1"), symbol("b1"),
                        symbol("c2"), symbol("b2"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                CrossJoinsElimination.getJoinOrder(joinGraph),
                Optional.of(ImmutableList.of(0, 2, 1)));
    }

    @Test
    public void testDonNotChangeOrderWithoutCrossJoin()
    {
        PlanNode plan =
                join(
                        join(
                                new AnyPlanNode(new PlanNodeId("A"), symbol("a")),
                                new AnyPlanNode(new PlanNodeId("B"), symbol("b")),
                                symbol("a"), symbol("b")),
                        new AnyPlanNode(new PlanNodeId("C"), symbol("c")),
                        symbol("c"), symbol("b"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                CrossJoinsElimination.getJoinOrder(joinGraph),
                Optional.of(ImmutableList.of(0, 1, 2)));
    }

    @Test
    public void testGiveUpOnCrossJoin()
    {
        PlanNode plan =
                join(
                        join(
                                new AnyPlanNode(new PlanNodeId("A"), symbol("a")),
                                new AnyPlanNode(new PlanNodeId("B"), symbol("b"))),
                        new AnyPlanNode(new PlanNodeId("C"), symbol("c")),
                        symbol("c"), symbol("b"));

        JoinGraph joinGraph = getOnlyElement(JoinGraph.buildFrom(plan));

        assertEquals(
                CrossJoinsElimination.getJoinOrder(joinGraph),
                Optional.empty());
    }

    @Test
    public void testGiveUpOnNonIdentityProjections()
    {
        PlanNode plan =
                join(
                        project(
                                join(
                                        new AnyPlanNode(new PlanNodeId("A"), symbol("a1")),
                                        new AnyPlanNode(new PlanNodeId("B"), symbol("b"))),
                                symbol("a2"),
                                new ArithmeticUnaryExpression(MINUS, new SymbolReference("a1"))),
                        new AnyPlanNode(new PlanNodeId("C"), symbol("c")),
                        symbol("a2"), symbol("c"),
                        symbol("c"), symbol("b"));

        assertEquals(JoinGraph.buildFrom(plan).size(), 2);
    }

    private PlanNode project(PlanNode source, Symbol symbol, Expression expression)
    {
        return new ProjectNode(
                idAllocator.getNextId(),
                source,
                ImmutableMap.of(symbol, expression));
    }

    private Symbol symbol(String name)
    {
        return new Symbol(name);
    }

    private JoinNode join(PlanNode left, PlanNode right, Symbol... symbols)
    {
        checkArgument(symbols.length % 2 == 0);
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

        for (int i = 0; i < symbols.length; i += 2) {
            criteria.add(new JoinNode.EquiJoinClause(symbols[i], symbols[i + 1]));
        }

        return new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                left,
                right,
                criteria.build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static class AnyPlanNode
            extends PlanNode
    {
        List<Symbol> symbols;

        public AnyPlanNode(PlanNodeId planId, Symbol... symbols)
        {
            super(planId);
            this.symbols = ImmutableList.copyOf(symbols);
        }

        @Override
        public List<PlanNode> getSources()
        {
            return ImmutableList.of();
        }

        @Override
        public List<Symbol> getOutputSymbols()
        {
            return symbols;
        }

        @Override
        public String toString()
        {
            return getId().toString();
        }
    }
}
