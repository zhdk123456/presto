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
package com.facebook.presto.operator;

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Threads.checkNotSameThreadExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.testng.AssertJUnit.assertEquals;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {
        //"-XX:MaxInlineSize=10",
        "-XX:CompileCommand=print,*PageProcessor*.process*"} )
@Warmup(iterations = 10)
@Measurement(iterations = 1000)
public class BenchmarkMultiJoin
{
    private static final int HASH_BUILD_OPERATOR_ID = 1;
    private static final int HASH_JOIN_OPERATOR_ID = 2;
    private static final PlanNodeId TEST_PLAN_NODE_ID = new PlanNodeId("test");

    @State(Thread)
    public static class BuildContext
    {
        protected static final int ROWS_PER_PAGE = 1024;
        protected static final int BUILD_ROWS_NUMBER = 700_000;

        //@Param({"varchar", "bigint", "all"})
        protected String hashColumns = "bigint";

        //@Param({"false", "true"})
        protected boolean buildHashEnabled = false;

        protected ExecutorService executor;
        protected Optional<Integer> hashChannel;
        protected List<Page> buildPages1;
        protected List<Page> buildPages2;
        protected List<Type> types;
        protected List<Integer> hashChannels;

        @Setup
        public void setup()
        {
            switch (hashColumns) {
                case "varchar":
                    hashChannels = Ints.asList(0);
                    break;
                case "bigint":
                    hashChannels = Ints.asList(1);
                    break;
                case "all":
                    hashChannels = Ints.asList(0, 1, 2);
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unknown hashColumns value [%s]", hashColumns));
            }
            executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

            RowPagesBuilder buildPagesBuilder1 = initializeBuildPages();

            buildPages1 = buildPagesBuilder1.build();
            hashChannel = buildPagesBuilder1.getHashChannel();

            RowPagesBuilder buildPagesBuilder2 = initializeBuildPages();

            buildPages2 = buildPagesBuilder2.build();
        }

        public HashBuilderOperatorFactory getHashBuilderOperatorFactory(List<Type> types)
        {
            return new HashBuilderOperatorFactory(
                    HASH_BUILD_OPERATOR_ID,
                    TEST_PLAN_NODE_ID,
                    types,
                    ImmutableMap.of(),
                    hashChannels,
                    hashChannel,
                    false,
                    Optional.empty(),
                    10_000);
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(
                    checkNotSameThreadExecutor(executor, "executor is null"),
                    TEST_SESSION,
                    new DataSize(2, GIGABYTE));
        }

        public Optional<Integer> getHashChannel()
        {
            return hashChannel;
        }

        public List<Integer> getHashChannels()
        {
            return hashChannels;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        protected RowPagesBuilder initializeBuildPages()
        {
            RowPagesBuilder buildPagesBuilder = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT));

            int rows = 0;
            while (rows < BUILD_ROWS_NUMBER) {
                int newRows = Math.min(BUILD_ROWS_NUMBER - rows, ROWS_PER_PAGE);
                buildPagesBuilder.addSequencePage(newRows, rows + 20, rows + 30, rows + 40);
                buildPagesBuilder.pageBreak();
                rows += newRows;
            }

            types = buildPagesBuilder.getTypes();
            return buildPagesBuilder;
        }

        public List<Page> getBuildPages1()
        {
            return buildPages1;
        }

        public List<Page> getBuildPages2()
        {
            return buildPages2;
        }
    }

    @State(Thread)
    public static class JoinContext
            extends BuildContext
    {
        protected static final int PROBE_ROWS_NUMBER = 700_000;

        //@Param({"0.1", "1", "2"})
        protected double matchRate = 1;

        protected List<Page> probePages1;
        protected List<Page> probePages2;

        @Setup
        public void setup()
        {
            super.setup();
            probePages1 = initializeProbePages();
            probePages2 = initializeProbePages();
        }

        public List<Page> getProbePages1()
        {
            return probePages1;
        }

        public List<Page> getProbePages2()
        {
            return probePages2;
        }

        protected List<Page> initializeProbePages()
        {
            RowPagesBuilder probePagesBuilder = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT));

            Random random = new Random(42);
            int remainingRows = PROBE_ROWS_NUMBER;
            int rowsInPage = 0;
            while (remainingRows > 0) {
                double roll = random.nextDouble();

                int columnA = 20 + remainingRows;
                int columnB = 30 + remainingRows;
                int columnC = 40 + remainingRows;

                int rowsCount = 1;
                if (matchRate < 1) {
                    // each row has matchRate chance to join
                    if (roll > matchRate) {
                        // generate not matched row
                        columnA *= -1;
                        columnB *= -1;
                        columnC *= -1;
                    }
                }
                else if (matchRate > 1) {
                    // each row has will be repeated between one and 2*matchRate times
                    roll = roll * 2 * matchRate + 1;
                    // example for matchRate == 2:
                    // roll is within [0, 5) range
                    // rowsCount is within [0, 4] range, where each value has same probability
                    // so expected rowsCount is 2
                    rowsCount = (int) Math.floor(roll);
                }

                for (int i = 0; i < rowsCount; i++) {
                    if (rowsInPage >= ROWS_PER_PAGE) {
                        probePagesBuilder.pageBreak();
                        rowsInPage = 0;
                    }
                    probePagesBuilder.row(format("%d", columnA), columnB, columnC);
                    --remainingRows;
                    rowsInPage++;
                }
            }
            return probePagesBuilder.build();
        }

        public List<Type> getResultTypes()
        {
            List<Type> resultTypes = new ArrayList<>();
            resultTypes.addAll(getTypes());
            resultTypes.addAll(getTypes());
            resultTypes.addAll(getTypes());
            return resultTypes;
        }
    }

    @Benchmark
    public List<Page> baselineMultiJoin(JoinContext joinContext)
    {
        HashBuilderOperatorFactory hashBuilderOperatorFactory1 = joinContext.getHashBuilderOperatorFactory(joinContext.getTypes());

        OperatorFactory joinOperatorFactory1 = LookupJoinOperators.innerJoin(
                HASH_JOIN_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                hashBuilderOperatorFactory1.getLookupSourceSupplier(),
                joinContext.getTypes(),
                joinContext.getHashChannels(),
                joinContext.getHashChannel(),
                false);

        List<Type> join1OutputTypes = new ArrayList<>();
        join1OutputTypes.addAll(joinContext.getTypes());
        join1OutputTypes.addAll(joinContext.getTypes());

        HashBuilderOperatorFactory hashBuilderOperatorFactory2 = joinContext.getHashBuilderOperatorFactory(join1OutputTypes);

        OperatorFactory joinOperatorFactory2 = LookupJoinOperators.innerJoin(
                HASH_JOIN_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                hashBuilderOperatorFactory2.getLookupSourceSupplier(),
                joinContext.getTypes(),
                joinContext.getHashChannels(),
                joinContext.getHashChannel(),
                false);

        feed(joinContext, hashBuilderOperatorFactory1, joinContext.getBuildPages1().iterator());
        List<Page> joinOutput1 = feed(joinContext, joinOperatorFactory1, joinContext.getProbePages1().iterator());
        feed(joinContext, hashBuilderOperatorFactory2, joinOutput1.iterator());
        return feed(joinContext, joinOperatorFactory2, joinContext.getProbePages2().iterator());
    }

    @Benchmark
    public List<Page> handcodedMultiJoin(JoinContext joinContext)
    {
        HashBuilderOperatorFactory hashBuilderOperatorFactory1 = joinContext.getHashBuilderOperatorFactory(joinContext.getTypes());
        HashBuilderOperatorFactory hashBuilderOperatorFactory2 = joinContext.getHashBuilderOperatorFactory(joinContext.getTypes());

        OperatorFactory multiJoinOperatorFactory = LookupJoinOperators.multiJoin(
                HASH_JOIN_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                hashBuilderOperatorFactory2.getLookupSourceSupplier(),
                hashBuilderOperatorFactory2.getLookupSourceSupplier(),
                joinContext.getTypes(),
                joinContext.getHashChannels(),
                joinContext.getHashChannel(),
                false);

        feed(joinContext, hashBuilderOperatorFactory1, joinContext.getBuildPages1().iterator());
        feed(joinContext, hashBuilderOperatorFactory2, joinContext.getBuildPages1().iterator());
        return feed(joinContext, multiJoinOperatorFactory, joinContext.getProbePages2().iterator());
    }

    @Benchmark
    public List<Page> handcodedBigintMultiJoin(JoinContext joinContext)
    {
        HashBuilderOperatorFactory hashBuilderOperatorFactory1 = joinContext.getHashBuilderOperatorFactory(joinContext.getTypes());
        HashBuilderOperatorFactory hashBuilderOperatorFactory2 = joinContext.getHashBuilderOperatorFactory(joinContext.getTypes());

        OperatorFactory multiJoinOperatorFactory = LookupJoinOperators.bigintMultiJoin(
                HASH_JOIN_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                hashBuilderOperatorFactory2.getLookupSourceSupplier(),
                hashBuilderOperatorFactory2.getLookupSourceSupplier(),
                joinContext.getTypes(),
                joinContext.getHashChannels(),
                joinContext.getHashChannel(),
                false);

        feed(joinContext, hashBuilderOperatorFactory1, joinContext.getBuildPages1().iterator());
        feed(joinContext, hashBuilderOperatorFactory2, joinContext.getBuildPages1().iterator());
        return feed(joinContext, multiJoinOperatorFactory, joinContext.getProbePages2().iterator());
    }

    @Benchmark
    public List<Page> xcompiledMultiJoin(JoinContext joinContext)
    {
        HashBuilderOperatorFactory hashBuilderOperatorFactory1 = joinContext.getHashBuilderOperatorFactory(joinContext.getTypes());
        HashBuilderOperatorFactory hashBuilderOperatorFactory2 = joinContext.getHashBuilderOperatorFactory(joinContext.getTypes());

        OperatorFactory multiJoinOperatorFactory = LookupJoinOperators.xcompiledMultiJoin(
                HASH_JOIN_OPERATOR_ID,
                TEST_PLAN_NODE_ID,
                hashBuilderOperatorFactory2.getLookupSourceSupplier(),
                hashBuilderOperatorFactory2.getLookupSourceSupplier(),
                joinContext.getTypes(),
                joinContext.getHashChannels(),
                joinContext.getHashChannel(),
                false);

        feed(joinContext, hashBuilderOperatorFactory1, joinContext.getBuildPages1().iterator());
        feed(joinContext, hashBuilderOperatorFactory2, joinContext.getBuildPages1().iterator());
        return feed(joinContext, multiJoinOperatorFactory, joinContext.getProbePages2().iterator());
    }

    @Test
    public void testBaseline()
    {
        JoinContext joinContext = new JoinContext();
        joinContext.setup();
        List<Page> pages = baselineMultiJoin(joinContext);
        long totalPositions = 0;
        for (Page page : pages) {
            totalPositions += page.getPositionCount();
            assertEquals(joinContext.getTypes().size() * 3, page.getBlocks().length);
        }
        assertEquals(699_999, totalPositions);
    }

    @Test
    public void testHandcodedMultiJoin()
    {
        JoinContext joinContext = new JoinContext();
        joinContext.setup();
        List<Page> handcodedPages = handcodedMultiJoin(joinContext);
        List<Page> baselinePages = baselineMultiJoin(joinContext);

        assertPages(joinContext.getResultTypes(), baselinePages, handcodedPages);
    }

    @Test
    public void testHandcodedBigintMultiJoin()
    {
        JoinContext joinContext = new JoinContext();
        joinContext.setup();
        List<Page> handcodedPages = handcodedBigintMultiJoin(joinContext);
        List<Page> baselinePages = baselineMultiJoin(joinContext);

        assertPages(joinContext.getResultTypes(), baselinePages, handcodedPages);
    }

    @Test
    public void testXcompiledMultiJoin()
    {
        JoinContext joinContext = new JoinContext();
        joinContext.setup();
        List<Page> xcompiledPages = xcompiledMultiJoin(joinContext);
        List<Page> baselinePages = baselineMultiJoin(joinContext);

        assertPages(joinContext.getResultTypes(), baselinePages, xcompiledPages);
    }

    private void assertPages(List<Type> types, List<Page> expectedPages, List<Page> actualPages)
    {
        assertEquals(expectedPages.size(), actualPages.size());

        for (int i = 0; i < actualPages.size(); i++) {
            Page handcodedPage = actualPages.get(i);
            Page baselinePage = expectedPages.get(i);
            assertPageEquals(types, baselinePage, handcodedPage);
        }
    }

    private static List<Page> feed(JoinContext joinContext, OperatorFactory factory, Iterator<Page> input)
    {
        DriverContext driverContext = joinContext.createTaskContext().addPipelineContext(true, true).addDriverContext();
        Operator joinOperator = factory.createOperator(driverContext);

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;
        for (int loops = 0; !joinOperator.isFinished() && loops < 1_000_000; loops++) {
            if (!joinOperator.isBlocked().isDone()) {
                continue;
            }
            if (joinOperator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    joinOperator.addInput(inputPage);
                }
                else if (!finishing) {
                    joinOperator.finish();
                    finishing = true;
                }
            }

            Page outputPage = joinOperator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        return outputPages.build();
    }
}
