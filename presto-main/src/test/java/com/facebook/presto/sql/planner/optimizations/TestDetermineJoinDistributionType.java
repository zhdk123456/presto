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
import com.facebook.presto.cost.CoefficientBasedCostCalculator;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.metadata.GlobalProperties;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestDetermineJoinDistributionType
{
    private final LocalQueryRunner queryRunner;
    private final Session automaticJoinDistributionSession;
    private final Session repartitionedJoinSession;
    private final CostCalculator costCalculator;

    public TestDetermineJoinDistributionType()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("sf10")
                .build());

        automaticJoinDistributionSession = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(SystemSessionProperties.JOIN_DISTRIBUTION_TYPE, "automatic")
                .build();

        repartitionedJoinSession = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(SystemSessionProperties.JOIN_DISTRIBUTION_TYPE, "repartitioned")
                .build();

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());
        costCalculator = new CoefficientBasedCostCalculator(queryRunner.getMetadata());
    }

    @Test
    public void testReplicatesSmallJoin()
    {
        @Language("SQL") String sql = "select * from nation join region on nation.regionkey = region.regionkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(equiJoinClause("n_regionkey", "r_regionkey")),
                                JoinNode.DistributionType.REPLICATED,
                                tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey")),
                                tableScan("region", ImmutableMap.of("r_regionkey", "regionkey"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testRepartitionsSameSizeTables()
    {
        @Language("SQL") String sql = "select * from nation n1 join nation n2 on n1.nationkey = n2.nationkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(equiJoinClause("n1_nationkey", "n2_nationkey")),
                                JoinNode.DistributionType.PARTITIONED,
                                tableScan("nation", ImmutableMap.of("n1_nationkey", "nationkey")),
                                tableScan("nation", ImmutableMap.of("n2_nationkey", "nationkey"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testReplicatesAndFlipsSmallJoin()
    {
        @Language("SQL") String sql = "select * from nation join lineitem on nation.regionkey = lineitem.orderkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(equiJoinClause("l_orderkey", "n_regionkey")),
                                JoinNode.DistributionType.REPLICATED,
                                tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey")),
                                tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testDoesNotFlipWhenIllegal()
    {
        @Language("SQL") String sql = "select * from nation left outer join lineitem on nation.regionkey = lineitem.orderkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.LEFT,
                                ImmutableList.of(equiJoinClause("n_regionkey", "l_orderkey")),
                                JoinNode.DistributionType.PARTITIONED,
                                tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey")),
                                tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);

        sql = "select * from lineitem left outer join nation on nation.regionkey = lineitem.orderkey";
        pattern =
                anyTree(
                        join(JoinNode.Type.LEFT,
                                ImmutableList.of(equiJoinClause("l_orderkey", "n_regionkey")),
                                JoinNode.DistributionType.REPLICATED,
                                tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey")),
                                tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testPartitionsLargeJoin()
    {
        @Language("SQL") String sql = "select * from  lineitem l1 join lineitem l2 on l1.orderkey = l2.orderkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(equiJoinClause("l1_orderkey", "l2_orderkey")),
                                JoinNode.DistributionType.PARTITIONED,
                                tableScan("lineitem", ImmutableMap.of("l1_orderkey", "orderkey")),
                                tableScan("lineitem", ImmutableMap.of("l2_orderkey", "orderkey"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testObeysSessionPropertyForJoin()
    {
        @Language("SQL") String sql = "select * from region join nation on nation.regionkey = region.regionkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(equiJoinClause("r_regionkey", "n_regionkey")),
                                JoinNode.DistributionType.PARTITIONED,
                                tableScan("region", ImmutableMap.of("r_regionkey", "regionkey")),
                                tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey"))));

        assertPlan(repartitionedJoinSession, sql, pattern);
    }

    @Test
    public void testReplicatesSmallSemiJoin()
    {
        @Language("SQL") String sql = "SELECT * from nation where nationkey in (SELECT regionkey FROM region)";

        PlanMatchPattern pattern =
                anyTree(
                        semiJoin(
                                "X", "Y", "S",
                                SemiJoinNode.DistributionType.REPLICATED,
                                tableScan("nation", ImmutableMap.of("X", "nationkey")),
                                tableScan("region", ImmutableMap.of("Y", "regionkey"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testPartitionsLargeSemiJoin()
    {
        @Language("SQL") String sql = "SELECT * from nation where nationkey in (SELECT orderkey FROM orders)";

        PlanMatchPattern pattern =
                anyTree(
                        semiJoin(
                                "X", "Y", "S",
                                SemiJoinNode.DistributionType.PARTITIONED,
                                tableScan("nation", ImmutableMap.of("X", "nationkey")),
                                tableScan("orders", ImmutableMap.of("Y", "orderkey"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testObeysSessionPropertySemiJoin()
    {
        @Language("SQL") String sql = "SELECT * from nation where nationkey in (SELECT regionkey FROM region)";

        PlanMatchPattern pattern =
                anyTree(
                        semiJoin(
                                "X", "Y", "S",
                                SemiJoinNode.DistributionType.PARTITIONED,
                                tableScan("nation", ImmutableMap.of("X", "nationkey")),
                                tableScan("region", ImmutableMap.of("Y", "regionkey"))));

        assertPlan(repartitionedJoinSession, sql, pattern);
    }

    private void assertPlan(Session session, @Language("SQL") String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneUnreferencedOutputs(),
                new PruneIdentityProjections(),
                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                new DetermineJoinDistributionType(new CoefficientBasedCostCalculator(queryRunner.getMetadata()), new GlobalProperties(), 1));
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), costCalculator, actualPlan, pattern);
            return null;
        });
    }
}
