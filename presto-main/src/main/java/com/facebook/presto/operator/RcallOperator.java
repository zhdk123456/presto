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

import com.facebook.presto.operator.scalar.PageToRTranslator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RcallOperator
        implements Operator
{
    public static class RcallOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Type> types;
        private final Map<Symbol, Integer> sourceLayout;
        private final String rProgram;
        private final List<Symbol> params;
        private boolean closed;

        public RcallOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes, Map<Symbol, Integer> sourceLayout, String rProgram, List<Symbol> params)
        {
            this.operatorId = operatorId;
            this.rProgram = rProgram;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "type is null"));
            this.types = new ImmutableList.Builder<Type>()
                    .addAll(sourceTypes)
                    .add(VarcharType.createUnboundedVarcharType())
                    .build();
            this.sourceLayout = ImmutableMap.copyOf(requireNonNull(sourceLayout, "sourceLayout is null"));
            this.params = ImmutableList.copyOf(requireNonNull(params, "params can not be null"));
        }

        @Override
        public List<Type> getTypes()
        {
            return sourceTypes;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, RcallOperator.class.getSimpleName());
            return new RcallOperator(operatorContext, types, sourceTypes, sourceLayout, rProgram, params);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new RcallOperatorFactory(operatorId, planNodeId, sourceTypes, sourceLayout, rProgram, params);
        }
    }

    private final OperatorContext operatorContext;
    private final String rProgram;
    private boolean finishing;
    private final List<Type> types;
    private final List<Type> sourceTypes;
    private final Map<Symbol, Integer> sourceLayout;
    private Page outputPage = null;
    private final List<Symbol> params;
    private final Type[] argumentsTypes;
    private final PageToRTranslator rcaller = new PageToRTranslator();

    public RcallOperator(OperatorContext operatorContext, List<Type> types, List<Type> sourceTypes, Map<Symbol, Integer> sourceLayout, String rProgram, List<Symbol> params)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = types;
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.sourceLayout = ImmutableMap.copyOf(requireNonNull(sourceLayout, "sourceLayout is null"));
        this.rProgram = requireNonNull(rProgram, "rProgram can not be null");
        this.params = ImmutableList.copyOf(requireNonNull(params, "params can not be null"));

        argumentsTypes = new Type[params.size()];
        for (int i = 0; i < params.size(); ++i) {
            argumentsTypes[i] = sourceTypes.get(i);
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && outputPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && outputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        Page argumentsPage = buildArgumentsPage(page);
        Page rResultPage = rcaller.RCALL(rProgram, argumentsPage, VarcharType.createUnboundedVarcharType(), argumentsTypes);
        outputPage = buildResultPage(page, rResultPage);
    }

    private Page buildArgumentsPage(Page page)
    {
        Block[] blocks = new Block[params.size()];
        for (int i = 0; i < params.size(); ++i) {
            blocks[i] = page.getBlock(sourceLayout.get(params.get(i)));
        }
        return new Page(blocks);
    }

    private Page buildResultPage(Page page, Page rResultPage)
    {
        Preconditions.checkArgument(rResultPage.getChannelCount() == 1, "expected one channel");
        Block[] resultBlocks = new Block[page.getChannelCount() + 1];
        for (int i = 0; i < page.getChannelCount(); ++i) {
            resultBlocks[i] = page.getBlock(i);
        }
        resultBlocks[page.getChannelCount()] = rResultPage.getBlock(0);
        return new Page(resultBlocks);
    }

    @Override
    public Page getOutput()
    {
        if (outputPage == null) {
            return outputPage;
        }
        Page ret = outputPage;
        outputPage = null;
        return ret;
    }

    Page RCALL(String rCode, Page page, Type returnType, Type[] types)
    {
        PageBuilder pb = new PageBuilder(ImmutableList.of(VarcharType.createUnboundedVarcharType()));
        pb.declarePositions(page.getPositionCount());
        for (int i = 0; i < page.getPositionCount(); ++i) {
            Slice slice = Slices.utf8Slice(String.valueOf(i));
            BlockBuilder blockBuilder = pb.getBlockBuilder(0);
            blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
        }
        return pb.build();
    }
}
