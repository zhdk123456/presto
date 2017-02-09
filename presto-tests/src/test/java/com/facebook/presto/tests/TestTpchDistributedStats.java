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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.execution.QueryPlan;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.planPrinter.PlanNodeStats;
import com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer;
import com.facebook.presto.tpch.ColumnNaming;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.tests.TestTpchDistributedStats.MetricComparison.Result.DIFFER;
import static com.facebook.presto.tests.TestTpchDistributedStats.MetricComparison.Result.MATCH;
import static com.facebook.presto.tests.TestTpchDistributedStats.MetricComparison.Result.NO_BASELINE;
import static com.facebook.presto.tests.TestTpchDistributedStats.MetricComparison.Result.NO_ESTIMATE;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunnerWithoutCatalogs;
import static com.facebook.presto.util.MoreMaps.mergeMaps;
import static com.google.common.collect.Maps.transformValues;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

public class TestTpchDistributedStats
{
    public static final ImmutableMap<String, Function<PlanNodeCost, Estimate>> METRICS = ImmutableMap.of(
            "output row count", PlanNodeCost::getOutputRowCount,
            "output size bytes", PlanNodeCost::getOutputSizeInBytes
    );

    public static final int NUMBER_OF_TPCH_QUERIES = 22;

    DistributedQueryRunner runner;

    public TestTpchDistributedStats()
            throws Exception
    {
        runner = createQueryRunnerWithoutCatalogs(emptyMap(), emptyMap());
        runner.createCatalog("tpch", "tpch", ImmutableMap.of(
                "tpch.column-naming", ColumnNaming.STANDARD.name()
        ));
    }

    @Test
    void testCostEstimatesVsRealityDifferences()
    {
        IntStream.rangeClosed(1, NUMBER_OF_TPCH_QUERIES)
                .filter(i -> !asList(15).contains(i)) //query 15 creates a view, which TPCH connector does not support.
                .forEach(i -> summarizeQuery(i, getTpchQuery(i)));
    }

    private String getTpchQuery(int i)
    {
        try {
            String queryClassPath = "/io/airlift/tpch/queries/q" + i + ".sql";
            return Resources.toString(getClass().getResource(queryClassPath), Charset.defaultCharset());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void summarizeQuery(int queryNumber, String query)
    {
        Session session = runner.getDefaultSession();
        String queryId = runner.executeWithQueryId(session, query).getQueryId();
        QueryPlan queryPlan = runner.getQueryPlan(new QueryId(queryId));
        StageInfo outputStageInfo = runner.getQueryInfo(new QueryId(queryId)).getOutputStage().get();

        List<PlanNode> allPlanNodes = new PlanNodeSearcher(queryPlan.getPlan().getRoot()).findAll();

        System.out.println(format("Query TPCH [%s] produces [%s] plan nodes.\n", queryNumber, allPlanNodes.size()));

        List<MetricComparison> comparisons = getMetricComparisons(queryPlan, outputStageInfo);

        Map<String, Map<MetricComparison.Result, List<MetricComparison>>> metricSummaries =
                comparisons.stream()
                        .collect(groupingBy(metricComparison -> metricComparison.metricName, groupingBy(MetricComparison::result)));

        metricSummaries.forEach((metricName, resultSummaries) -> {
            System.out.println(format("Summary for metric [%s]", metricName));
            outputSummary(resultSummaries, NO_ESTIMATE);
            outputSummary(resultSummaries, NO_BASELINE);
            outputSummary(resultSummaries, DIFFER);
            outputSummary(resultSummaries, MATCH);
            System.out.println();
        });

        System.out.println("Detailed results:\n");

        comparisons.forEach(System.out::println);
    }

    private List<MetricComparison> getMetricComparisons(QueryPlan queryPlan, StageInfo outputStageInfo)
    {
        return METRICS.entrySet().stream().flatMap(metricEntry -> {
            String metricName = metricEntry.getKey();
            Function<PlanNodeCost, Estimate> metricFunction = metricEntry.getValue();
            Map<PlanNode, PlanNodeCost> estimatesByPlanNode = getCostEstimatesByPlanNode(queryPlan);
            return estimatesByPlanNode.entrySet().stream().map(nodeCostEntry -> {
                PlanNode node = nodeCostEntry.getKey();
                PlanNodeCost estimate = nodeCostEntry.getValue();
                Map<PlanNodeId, PlanNodeCost> executionCostsByPlanId = getExecutionCostsById(outputStageInfo);
                Optional<PlanNodeCost> execution = Optional.ofNullable(executionCostsByPlanId.get(node.getId()));
                Optional<Double> estimatedCost = asOptional(metricFunction.apply(estimate));
                Optional<Double> executionCost = execution.flatMap(e -> asOptional(metricFunction.apply(e)));
                return new MetricComparison(node, metricName, estimatedCost, executionCost);
            });
        }).collect(Collectors.toList());
    }

    private Map<PlanNode, PlanNodeCost> getCostEstimatesByPlanNode(QueryPlan queryPlan)
    {
        return queryPlan.getPlanNodeCosts().entrySet().stream()
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<PlanNodeId, PlanNodeCost> getExecutionCostsById(StageInfo outputStageInfo)
    {
        List<StageInfo> allStagesInfos = getAllStages(Optional.of(outputStageInfo));
        Stream<Map<PlanNodeId, PlanNodeStats>> aggregatedPlanNodeStatsByStages =
                allStagesInfos.stream().map(PlanNodeStatsSummarizer::aggregatePlanNodeStats);
        BinaryOperator<PlanNodeStats> allowNoDuplicates = (a, b) -> {
            throw new IllegalArgumentException("PlanNodeIds must be unique");
        };
        Map<PlanNodeId, PlanNodeStats> planNodeIdPlanNodeStatsMap =
                mergeMaps(aggregatedPlanNodeStatsByStages, allowNoDuplicates);
        return transformValues(planNodeIdPlanNodeStatsMap, this::toPlanNodeCost);
    }

    private PlanNodeCost toPlanNodeCost(PlanNodeStats operatorStats)
    {
        return PlanNodeCost.builder()
                .setOutputRowCount(new Estimate(operatorStats.getPlanNodeOutputPositions()))
                .setOutputSizeInBytes(new Estimate(operatorStats.getPlanNodeOutputDataSize().toBytes()))
                .build();
    }

    private Optional<Double> asOptional(Estimate estimate)
    {
        return estimate.isValueUnknown() ? Optional.empty() : Optional.of(estimate.getValue());
    }

    private void outputSummary(Map<MetricComparison.Result, List<MetricComparison>> resultSummaries, MetricComparison.Result result)
    {
        System.out.println(format("[%s]\t-\t[%s]", result, resultSummaries.getOrDefault(result, emptyList()).size()));
    }

    static class MetricComparison
    {
        final PlanNode planNode;
        final String metricName;
        final Optional<Double> estimatedCost;
        final Optional<Double> executionCost;

        public MetricComparison(PlanNode planNode, String metricName, Optional<Double> estimatedCost, Optional<Double> executionCost)
        {
            this.planNode = planNode;
            this.metricName = metricName;
            this.estimatedCost = estimatedCost;
            this.executionCost = executionCost;
        }

        @Override
        public String toString()
        {
            return format("Metric [%s] - [%s] - estimated: [%s], real: [%s] - plan node: [%s]",
                    metricName, result(), print(estimatedCost), print(executionCost), planNode);
        }

        Result result()
        {
            return estimatedCost
                    .map(estimate -> executionCost
                            .map(execution -> estimateMatchesReality(estimate, execution) ? MATCH : DIFFER)
                            .orElse(NO_BASELINE))
                    .orElse(NO_ESTIMATE);
        }

        private String print(Optional<Double> cost)
        {
            return cost.map(Object::toString).orElse("UNKNOWN");
        }

        private boolean estimateMatchesReality(Double estimate, Double execution)
        {
            return Math.abs(execution - estimate) / execution < 0.1;
        }

        enum Result
        {
            NO_ESTIMATE,
            NO_BASELINE,
            DIFFER,
            MATCH
        }
    }
}
