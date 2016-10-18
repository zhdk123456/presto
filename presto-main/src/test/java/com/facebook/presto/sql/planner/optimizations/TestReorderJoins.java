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
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aliasPair;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.fail;

public class TestReorderJoins
{
    private final LocalQueryRunner queryRunner;

    public TestReorderJoins()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty(SystemSessionProperties.REORDER_JOINS, "true")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testEliminateSimpleCrossJoin()
    {
        assertPlan("SELECT o.orderkey FROM part p, orders o, lineitem l WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(aliasPair("orderkey_2", "orderkey")),
                                anyTree(
                                        join(INNER, ImmutableList.of(aliasPair("partkey", "partkey_3")),
                                                anyTree(tableScan("part")),
                                                anyTree(tableScan("lineitem")))),
                                anyTree(tableScan("orders")))));
    }

    @Test
    public void testGiveUpOnCrossJoin()
    {
        assertPlan("SELECT o.orderkey FROM part p, orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(aliasPair("orderkey", "orderkey_2")),
                                anyTree(
                                        join(INNER, ImmutableList.of(),
                                                tableScan("part"),
                                                anyTree(tableScan("orders")))),
                                anyTree(tableScan("lineitem")))));
    }

    @Test
    public void testEliminateCrossJoinFromTwoJoinBlocks()
    {
        String joinToReorder = "(SELECT l.suppkey suppkey FROM part p, orders o, lineitem l WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey)";
        assertPlan(format("SELECT * FROM (SELECT max(suppkey) suppkey FROM (%s)) subquery, supplier s WHERE subquery.suppkey = s.suppkey", joinToReorder),
                anyTree(
                        join(INNER, ImmutableList.of(aliasPair("max", "suppkey")),
                                anyTree(join(INNER, ImmutableList.of(aliasPair("orderkey_2", "orderkey")),
                                        anyTree(
                                                join(INNER, ImmutableList.of(aliasPair("partkey", "partkey_3")),
                                                        anyTree(tableScan("part")),
                                                        anyTree(tableScan("lineitem")))),
                                        anyTree(tableScan("orders")))),
                                anyTree())));
    }

    @Test
    public void testEliminateCrossJoinWithNonEqualityCondition()
    {
        assertPlan("SELECT o.orderkey FROM part p, orders o, lineitem l WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey AND p.partkey <> o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(aliasPair("orderkey_2", "orderkey")),
                                anyTree(
                                        join(INNER, ImmutableList.of(aliasPair("partkey", "partkey_3")),
                                                anyTree(tableScan("part")),
                                                anyTree(filter("partkey_3 <> orderkey_2", tableScan("lineitem"))))),
                                anyTree(tableScan("orders")))));
    }

    @Test
    public void testEliminateCrossJoinPreserveFilters()
    {
        assertPlan("SELECT o.orderkey FROM part p, orders o, lineitem l " +
                        "WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey AND l.returnflag = 'R' AND shippriority >= 10",
                anyTree(join(INNER, ImmutableList.of(aliasPair("orderkey_2", "orderkey")),
                        anyTree(
                                join(INNER, ImmutableList.of(aliasPair("partkey", "partkey_3")),
                                        anyTree(tableScan("part")),
                                        anyTree(filter("returnflag = 'R'", tableScan("lineitem"))))),
                        anyTree(filter("shippriority >= 10", tableScan("orders"))))));
    }

    private void assertPlan(String sql, PlanMatchPattern pattern)
    {
        //Plan actualPlan = plan(sql);
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = plan(transactionSession, sql);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private Plan plan(Session transactionSession, String sql)
    {
        try {
            return queryRunner.createPlan(transactionSession, sql);
        }
        catch (RuntimeException ex) {
            fail("Invalid SQL: " + sql, ex);
            return null; // make compiler happy
        }
    }
}
