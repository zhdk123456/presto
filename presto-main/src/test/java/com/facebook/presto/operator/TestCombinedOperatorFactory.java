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
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.cross.CrossCompilationContext;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperator;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperatorFactory;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.equal;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.multiply;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestCombinedOperatorFactory
{
    private static final MetadataManager METADATA_MANAGER = createTestMetadataManager();

    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testCrossCompilation()
    {
        RowPagesBuilder pagesBuilder = rowPagesBuilder(BIGINT, BIGINT);
        pagesBuilder.addSequencePage(100, 0, 0);
        Page page = getOnlyElement(pagesBuilder.build());

        CombinedOperatorFactory factory = new CombinedOperatorFactory(
                0, new PlanNodeId("0"), METADATA_MANAGER,
                ImmutableList.of(
                        new TestCrossOperatorFactory(ImmutableList.of(BIGINT, BIGINT), 42),
                        new TestCrossOperatorFactory(ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT), 66)),
                ImmutableList.of(BIGINT, BIGINT));

        Operator operator = factory.createOperator(driverContext);

        operator.addInput(page);

        operator.finish();
        Page outputPage = operator.getOutput();
    }

    private class TestCrossOperatorFactory
            implements CrossCompiledOperatorFactory
    {
        final List<Type> inputTypes;
        final long value;

        TestCrossOperatorFactory(List<Type> inputTypes, long value)
        {
            this.inputTypes = inputTypes;
            this.value = value;
        }

        @Override
        public CrossCompiledOperator createCrossCompiledOperator(DriverContext driverContext)
        {
            return new CrossCompiledOperator()
            {
                @Override
                public BytecodeBlock process(CrossCompilationContext context)
                {
                    Variable counter = context.getMethodScope().declareVariable(context.uniqueVariableName("counter"), context.getMethodHeader(), constantInt(0));
                    for (int i = 0; i < inputTypes.size(); ++i) {
                        context.mapInputToOutputChannel(i, i + 2);
                    }
                    context.defineChannel(0, () -> multiply(constantLong(2), context.getChannel(0)));
                    context.defineIsNull(0, () -> constantBoolean(false));
                    context.defineChannel(1, () -> context.getField("example"));
                    context.defineIsNull(1, () -> equal(counter, constantInt(2)));
                    BytecodeBlock block = new BytecodeBlock();
                    return block
                            .append(counter.increment())
                            .append(context.processDownstreamOperator());
                }

                @Override
                public Map<String, FieldDefinition> getFields()
                {
                    return ImmutableMap.of("example", new FieldDefinition(long.class, value));
                }

                @Override
                public void close()
                {
                }
            };
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.<Type>builder()
                    .addAll(inputTypes)
                    .add(BIGINT)
                    .add(BIGINT).build();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            return null;
        }

        @Override
        public void close()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return null;
        }
    }
}
