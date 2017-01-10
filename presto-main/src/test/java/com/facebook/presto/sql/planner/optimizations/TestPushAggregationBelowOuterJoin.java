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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolMatcher;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.except;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.intersect;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushAggregationBelowOuterJoin
        extends BasePlanTest
{
    @Test
    public void testPushesAggregationWhenLeftSideHasAssignUniqueId()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> average = ImmutableMap.of(
                Optional.of("AVG"), functionCall("avg", ImmutableList.of("nationkey_2")));

        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> averageNull = ImmutableMap.of(
                Optional.of("AVG_NULL"), functionCall("avg", ImmutableList.of("null_literal")));

        assertPlanForSubquery(
                "SELECT regionkey " +
                        "FROM nation n1 " +
                        "WHERE (n1.nationkey > ( " +
                        "SELECT avg(nationkey) " +
                        "FROM nation n2 " +
                        "WHERE n1.regionkey=n2.regionkey))",
                anyTree(
                        project(ImmutableMap.of(
                                "regionkey_1", expression("regionkey_1"),
                                "COALESCE", expression("coalesce(AVG, AVG_NULL)")),
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("regionkey_1", "regionkey_2")),
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("regionkey_1", "regionkey"))),
                                                aggregation(ImmutableList.of(ImmutableList.of("regionkey_2")), average, ImmutableMap.of(), Optional.empty(),
                                                        anyTree(
                                                                tableScan("nation", ImmutableMap.of("regionkey_2", "regionkey", "nationkey_2", "nationkey"))))),
                                        aggregation(ImmutableList.of(ImmutableList.of()), averageNull, ImmutableMap.of(), Optional.empty(),
                                                values(ImmutableMap.of("null_literal", 0)))))));
    }

    @Test
    public void testPushesAggregationWhenLeftSideHasDistinct()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> max = ImmutableMap.of(
                Optional.of("MAX"), functionCall("max", ImmutableList.of("n_nationkey")));
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> maxNull = ImmutableMap.of(
                Optional.of("MAX_NULL"), functionCall("max", ImmutableList.of("null_literal")));

        assertPlan(
                "SELECT max(nationkey) " +
                        "FROM (SELECT DISTINCT regionkey FROM region) as r1 " +
                        "LEFT JOIN " +
                        "nation " +
                        "ON r1.regionkey = nation.regionkey " +
                        "GROUP BY r1.regionkey",
                anyTree(
                        project(ImmutableMap.of("COALESCE", expression("coalesce(MAX, MAX_NULL)")),
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("r_regionkey", "n_regionkey")),
                                                aggregation(ImmutableList.of(ImmutableList.of("r_regionkey")), ImmutableMap.of(), ImmutableMap.of(), Optional.empty(),
                                                        tableScan("region", ImmutableMap.of("r_regionkey", "regionkey"))),
                                                aggregation(ImmutableList.of(ImmutableList.of("n_regionkey")), max, ImmutableMap.of(), Optional.empty(),
                                                        tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey", "n_nationkey", "nationkey")))),
                                        aggregation(ImmutableList.of(ImmutableList.of()), maxNull, ImmutableMap.of(), Optional.empty(),
                                                values(ImmutableMap.of("null_literal", 0)))))));
    }

    @Test
    public void testPushesAggregationWhenLeftSideHasIntersect()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> max = ImmutableMap.of(
                Optional.of("MAX"), functionCall("max", ImmutableList.of("n_nationkey")));
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> maxNull = ImmutableMap.of(
                Optional.of("MAX_NULL"), functionCall("max", ImmutableList.of("null_literal")));

        assertPlan(
                "SELECT max(nationkey) " +
                        "FROM (SELECT regionkey FROM region INTERSECT SELECT regionkey FROM region where regionkey < 4) as r1 " +
                        "LEFT JOIN " +
                        "nation " +
                        "ON r1.regionkey = nation.regionkey " +
                        "GROUP BY r1.regionkey",
                anyTree(
                        project(ImmutableMap.of("COALESCE", expression("coalesce(MAX, MAX_NULL)")),
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("o_regionkey", "n_regionkey")),
                                                intersect(
                                                        tableScan("region", ImmutableMap.of("r1_regionkey", "regionkey")),
                                                        anyTree(
                                                                tableScan("region", ImmutableMap.of("r2_regionkey", "regionkey"))))
                                                        .withAlias("o_regionkey", new SymbolMatcher(0)),
                                                aggregation(ImmutableList.of(ImmutableList.of("n_regionkey")), max, ImmutableMap.of(), Optional.empty(),
                                                        tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey", "n_nationkey", "nationkey")))),
                                        aggregation(ImmutableList.of(ImmutableList.of()), maxNull, ImmutableMap.of(), Optional.empty(),
                                                values(ImmutableMap.of("null_literal", 0)))))));
    }

    @Test
    public void testPushesAggregationWhenLeftSideHasExcept()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> max = ImmutableMap.of(
                Optional.of("MAX"), functionCall("max", ImmutableList.of("n_nationkey")));
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> maxNull = ImmutableMap.of(
                Optional.of("MAX_NULL"), functionCall("max", ImmutableList.of("null_literal")));

        assertPlan(
                "SELECT max(nationkey) " +
                        "FROM (SELECT regionkey FROM region EXCEPT SELECT regionkey FROM region where regionkey < 4) as r1 " +
                        "LEFT JOIN " +
                        "nation " +
                        "ON r1.regionkey = nation.regionkey " +
                        "GROUP BY r1.regionkey",
                anyTree(
                        project(ImmutableMap.of("COALESCE", expression("coalesce(MAX, MAX_NULL)")),
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("o_regionkey", "n_regionkey")),
                                                except(
                                                        tableScan("region", ImmutableMap.of("r1_regionkey", "regionkey")),
                                                        anyTree(
                                                                tableScan("region", ImmutableMap.of("r2_regionkey", "regionkey"))))
                                                        .withAlias("o_regionkey", new SymbolMatcher(0)),
                                                aggregation(ImmutableList.of(ImmutableList.of("n_regionkey")), max, ImmutableMap.of(), Optional.empty(),
                                                        tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey", "n_nationkey", "nationkey")))),
                                        aggregation(ImmutableList.of(ImmutableList.of()), maxNull, ImmutableMap.of(), Optional.empty(),
                                                values(ImmutableMap.of("null_literal", 0)))))));
    }

    @Test
    public void testPushesAggregationWhenLeftSideHasOneRow()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> max = ImmutableMap.of(
                Optional.of("MAX"), functionCall("max", ImmutableList.of("n_nationkey")));
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> maxNull = ImmutableMap.of(
                Optional.of("MAX_NULL"), functionCall("max", ImmutableList.of("null_literal")));

        assertPlan(
                "SELECT max(nationkey) " +
                        "FROM (SELECT * FROM (VALUES(CAST(1 AS BIGINT)))) as v1(col1) " +
                        "LEFT JOIN " +
                        "nation " +
                        "ON v1.col1 = nation.regionkey " +
                        "GROUP BY v1.col1",
                anyTree(
                        project(ImmutableMap.of("COALESCE", expression("coalesce(MAX, MAX_NULL)")),
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("v1_col1", "n_regionkey")),
                                                values(ImmutableMap.of("v1_col1", 0)),
                                                aggregation(ImmutableList.of(ImmutableList.of("n_regionkey")), max, ImmutableMap.of(), Optional.empty(),
                                                        tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey", "n_nationkey", "nationkey")))),
                                        aggregation(ImmutableList.of(ImmutableList.of()), maxNull, ImmutableMap.of(), Optional.empty(),
                                                values(ImmutableMap.of("null_literal", 0)))))));
    }

    @Test
    public void testDoesNotPushWhenLeftNotUnique()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> max = ImmutableMap.of(
                Optional.of("MAX"), functionCall("max", ImmutableList.of("n2_nationkey")));

        assertPlan(
                "SELECT max(nationkey) " +
                        "FROM (SELECT regionkey FROM nation) n1 " +
                        "LEFT JOIN " +
                        "nation n2 " +
                        "ON n1.regionkey = n2.regionkey " +
                        "GROUP BY n1.regionkey",
                anyTree(
                        aggregation(ImmutableList.of(ImmutableList.of("n1_regionkey")), max, ImmutableMap.of(), Optional.empty(),
                                anyTree(
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("n1_regionkey", "n2_regionkey")),
                                                tableScan("nation", ImmutableMap.of("n1_regionkey", "regionkey")),
                                                tableScan("nation", ImmutableMap.of("n2_regionkey", "regionkey", "n2_nationkey", "nationkey")))))));
    }

    @Test
    public void testDoesNotPushWhenGroupingOnRightColumn()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> max = ImmutableMap.of(
                Optional.of("MAX"), functionCall("max", ImmutableList.of("n2_nationkey")));

        assertPlan(
                "SELECT max(nationkey) " +
                        "FROM (SELECT DISTINCT regionkey FROM nation) n1 " +
                        "LEFT JOIN " +
                        "nation n2 " +
                        "ON n1.regionkey = n2.regionkey " +
                        "GROUP BY n1.regionkey, n2.regionkey",
                anyTree(
                        aggregation(ImmutableList.of(ImmutableList.of("n1_regionkey", "n2_regionkey")), max, ImmutableMap.of(), Optional.empty(),
                                anyTree(
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("n1_regionkey", "n2_regionkey")),
                                                aggregation(ImmutableList.of(ImmutableList.of("n1_regionkey")), ImmutableMap.of(), ImmutableMap.of(), Optional.empty(),
                                                        tableScan("nation", ImmutableMap.of("n1_regionkey", "regionkey"))),
                                                tableScan("nation", ImmutableMap.of("n2_regionkey", "regionkey", "n2_nationkey", "nationkey")))))));
    }

    @Test
    public void testDoesNotPushWhenAggregationOnLeftColumn()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregations = ImmutableMap.of(
                Optional.of("MAX"), functionCall("max", ImmutableList.of("n1_regionkey")),
                Optional.of("AVG"), functionCall("avg", ImmutableList.of("n2_nationkey")));

        assertPlan(
                "SELECT max(n1.regionkey), avg(n2.nationkey) " +
                        "FROM (SELECT DISTINCT regionkey FROM nation) n1 " +
                        "LEFT JOIN " +
                        "nation n2 " +
                        "ON n1.regionkey = n2.regionkey",
                anyTree(
                        aggregation(ImmutableList.of(ImmutableList.of()), aggregations, ImmutableMap.of(), Optional.empty(),
                                join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("n1_regionkey", "n2_regionkey")),
                                        aggregation(ImmutableList.of(ImmutableList.of("n1_regionkey")), ImmutableMap.of(), ImmutableMap.of(), Optional.empty(),
                                                tableScan("nation", ImmutableMap.of("n1_regionkey", "regionkey"))),
                                        tableScan("nation", ImmutableMap.of("n2_regionkey", "regionkey", "n2_nationkey", "nationkey"))))));
    }

    @Test
    public void testDoesNotPushWhenJoinConditionHasNonEquality()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> max = ImmutableMap.of(
                Optional.of("MAX"), functionCall("max", ImmutableList.of("n2_nationkey")));

        assertPlan(
                "SELECT max(nationkey) " +
                        "FROM (SELECT DISTINCT regionkey FROM nation) n1 " +
                        "LEFT JOIN " +
                        "nation n2 " +
                        "ON n1.regionkey = n2.regionkey AND n1.regionkey < 4 " +
                        "GROUP BY n1.regionkey",
                anyTree(
                        aggregation(ImmutableList.of(ImmutableList.of("n1_regionkey")), max, ImmutableMap.of(), Optional.empty(),
                                anyTree(
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("n1_regionkey", "n2_regionkey")), Optional.of("n1_regionkey < CAST(4 AS bigint)"),
                                                aggregation(ImmutableList.of(ImmutableList.of("n1_regionkey")), ImmutableMap.of(), ImmutableMap.of(), Optional.empty(),
                                                        tableScan("nation", ImmutableMap.of("n1_regionkey", "regionkey"))),
                                                tableScan("nation", ImmutableMap.of("n2_regionkey", "regionkey", "n2_nationkey", "nationkey")))))));
    }

    @Test
    public void testPushesAggregationForRightJoin()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> max = ImmutableMap.of(
                Optional.of("MAX"), functionCall("max", ImmutableList.of("n_nationkey")));
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> maxNull = ImmutableMap.of(
                Optional.of("MAX_NULL"), functionCall("max", ImmutableList.of("null_literal")));

        assertPlan(
                "SELECT max(nationkey) " +
                        "FROM nation " +
                        "RIGHT JOIN " +
                        "(SELECT DISTINCT regionkey FROM region) as r1 " +
                        "ON nation.regionkey = r1.regionkey " +
                        "GROUP BY r1.regionkey",
                anyTree(
                        project(ImmutableMap.of("COALESCE", expression("coalesce(MAX, MAX_NULL)")),
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        join(JoinNode.Type.RIGHT, ImmutableList.of(equiJoinClause("n_regionkey", "r_regionkey")),
                                                aggregation(ImmutableList.of(ImmutableList.of("n_regionkey")), max, ImmutableMap.of(), Optional.empty(),
                                                        tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey", "n_nationkey", "nationkey"))),
                                                aggregation(ImmutableList.of(ImmutableList.of("r_regionkey")), ImmutableMap.of(), ImmutableMap.of(), Optional.empty(),
                                                        tableScan("region", ImmutableMap.of("r_regionkey", "regionkey")))),
                                        aggregation(ImmutableList.of(ImmutableList.of()), maxNull, ImmutableMap.of(), Optional.empty(),
                                                values(ImmutableMap.of("null_literal", 0)))))));
    }

    @Test
    public void testExistQuery()
    {
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> count = ImmutableMap.of(
                Optional.of("COUNT"), functionCall("count", ImmutableList.of("NON_NULL")));
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> countNull = ImmutableMap.of(
                Optional.of("COUNT_NULL"), functionCall("count", ImmutableList.of("null_literal")));

        assertPlanForSubquery(
                "SELECT nationkey " +
                        "FROM nation " +
                        "WHERE EXISTS (SELECT regionkey FROM region WHERE nation.regionkey = region.regionkey)",
                anyTree(
                        project(ImmutableMap.of("COALESCE", expression("coalesce(COUNT, COUNT_NULL)")),
                                join(JoinNode.Type.INNER, ImmutableList.of(),
                                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("n_regionkey", "r_regionkey")),
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey", "n_nationkey", "nationkey"))),
                                                aggregation(ImmutableList.of(ImmutableList.of("r_regionkey")), count, ImmutableMap.of(), Optional.empty(),
                                                        anyTree(
                                                                project(ImmutableMap.of("r_regionkey", expression("r_regionkey"),
                                                                        "NON_NULL", expression("true")),
                                                                        anyTree(
                                                                                tableScan("region", ImmutableMap.of("r_regionkey", "regionkey"))))))),
                                        aggregation(ImmutableList.of(ImmutableList.of()), countNull, ImmutableMap.of(), Optional.empty(),
                                                values(ImmutableMap.of("null_literal", 0)))))));
    }

    public void assertPlan(String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneUnreferencedOutputs(),
                new PruneIdentityProjections(),
                new PushAggregationBelowOuterJoin());
        assertPlanWithOptimizers(sql, pattern, optimizers);
    }

    public void assertPlanForSubquery(String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneUnreferencedOutputs(),
                new PruneIdentityProjections(),
                new TransformExistsApplyToScalarApply(getQueryRunner().getMetadata()),
                new TransformCorrelatedScalarAggregationToJoin(getQueryRunner().getMetadata()),
                new PredicatePushDown(getQueryRunner().getMetadata(), new SqlParser()),
                new PushAggregationBelowOuterJoin(),
                new PruneUnreferencedOutputs());
        assertPlanWithOptimizers(sql, pattern, optimizers);
    }
}
