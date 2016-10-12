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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.BytecodeExpressionVisitor;
import com.facebook.presto.sql.gen.cross.CrossCompilationContext;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperator;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperatorFactory;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.getProcessingOptimization;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FilterAndProjectOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> types;

    private final PageBuilder pageBuilder;
    private final PageProcessor processor;
    private final String processingOptimization;
    private Page currentPage;
    private int currentPosition;
    private boolean finishing;

    public FilterAndProjectOperator(OperatorContext operatorContext, Iterable<? extends Type> types, PageProcessor processor)
    {
        this.processor = requireNonNull(processor, "processor is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.processingOptimization = FeaturesConfig.ProcessingOptimization.COLUMNAR;
        this.pageBuilder = new PageBuilder(getTypes());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public final List<Type> getTypes()
    {
        return types;
    }

    @Override
    public final void finish()
    {
        finishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        return finishing && pageBuilder.isEmpty() && currentPage == null;
    }

    @Override
    public final boolean needsInput()
    {
        return !finishing && !pageBuilder.isFull() && currentPage == null;
    }

    @Override
    public final void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(!pageBuilder.isFull(), "Page buffer is full");

        currentPage = page;
        currentPosition = 0;
    }

    @Override
    public final Page getOutput()
    {
        if (!pageBuilder.isFull() && currentPage != null) {
            switch (processingOptimization) {
                case FeaturesConfig.ProcessingOptimization.COLUMNAR: {
                    Page page = processor.processColumnar(operatorContext.getSession().toConnectorSession(), currentPage, getTypes());
                    currentPage = null;
                    currentPosition = 0;
                    return page;
                }
                case FeaturesConfig.ProcessingOptimization.COLUMNAR_DICTIONARY: {
                    Page page = processor.processColumnarDictionary(operatorContext.getSession().toConnectorSession(), currentPage, getTypes());
                    currentPage = null;
                    currentPosition = 0;
                    return page;
                }
                case FeaturesConfig.ProcessingOptimization.DISABLED: {
                    currentPosition = processor.process(operatorContext.getSession().toConnectorSession(), currentPage, currentPosition, currentPage.getPositionCount(), pageBuilder);
                    if (currentPosition == currentPage.getPositionCount()) {
                        currentPage = null;
                        currentPosition = 0;
                    }
                    break;
                }
                default:
                    throw new IllegalStateException();
            }
        }

        if (!finishing && !pageBuilder.isFull() || pageBuilder.isEmpty()) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    public static class FilterAndProjectOperatorFactory
            implements OperatorFactory, CrossCompiledOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<PageProcessor> processor;
        private final List<Type> types;
        private final RowExpression filter;
        private final List<RowExpression> projections;
        private boolean closed;

        public FilterAndProjectOperatorFactory(int operatorId, PlanNodeId planNodeId, Supplier<PageProcessor> processor, List<Type> types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.processor = processor;
            this.types = types;
            this.filter = null;
            this.projections = null;
        }

        public FilterAndProjectOperatorFactory(int operatorId, PlanNodeId planNodeId, Supplier<PageProcessor> processor, List<Type> types,
                RowExpression filter, List<RowExpression> projections)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.processor = processor;
            this.types = types;
            this.filter = filter;
            this.projections = projections;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, FilterAndProjectOperator.class.getSimpleName());
            return new FilterAndProjectOperator(operatorContext, types, processor.get());
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new FilterAndProjectOperatorFactory(operatorId, planNodeId, processor, types);
        }

        @Override
        public CrossCompiledOperator createCrossCompiledOperator(DriverContext driverContext)
        {
            return new CrossCompiledOperator()
            {
                @Override
                public BytecodeBlock process(CrossCompilationContext context)
                {
                    BytecodeBlock block = new BytecodeBlock();

                    BytecodeBlock projectionsBlock = new BytecodeBlock();
                    for (int i = 0; i < projections.size(); ++i) {
                        if (projections.get(i) instanceof InputReferenceExpression) {
                            InputReferenceExpression reference = (InputReferenceExpression) projections.get(i);
                            context.mapInputToOutputChannel(reference.getField(), i);
                        }
                        else {
                            Variable wasNull = context.getMethodScope().createTempVariable(boolean.class);
                            projectionsBlock.append(wasNull.set(constantFalse()));
                            BytecodeExpression result = generateProject(context, projectionsBlock, projections.get(i), wasNull);
                            context.defineChannel(i, () -> result);
                            context.defineIsNull(i, () -> wasNull);
                        }
                    }

                    block.append(new IfStatement()
                            .condition(generateFilter(context, block))
                            .ifTrue(projectionsBlock.append(context.processDownstreamOperator())));

                    return block;
                }

                @Override
                public Map<String, FieldDefinition> getFields()
                {
                    return ImmutableMap.of();
                }

                @Override
                public void close()
                {
                }

                private BytecodeExpression generateProject(CrossCompilationContext context, BytecodeBlock block, RowExpression projection, Variable wasNullVariable)
                {
                    BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(
                            context.getCachedInstanceBinder().getCallSiteBinder(),
                            context.getCachedInstanceBinder(),
                            fieldReferenceCompiler(context, wasNullVariable),
                            context.getMetadata().getFunctionRegistry(),
                            ImmutableMap.of(),
                            wasNullVariable
                    );

                    if (!projection.getType().getJavaType().equals(void.class)) {
                        Variable result = context.getMethodScope().createTempVariable(projection.getType().getJavaType());
                        block.append(projection.accept(visitor, context.getMethodScope()))
                                .putVariable(result);
                        return result;
                    }
                    else {
                        block.append(projection.accept(visitor, context.getMethodScope()));
                        return constantNull(Void.class);
                    }
                }

                private BytecodeExpression generateFilter(CrossCompilationContext context, BytecodeBlock block)
                {
                    Variable wasNullVariable = context.getMethodScope().createTempVariable(boolean.class);
                    block.append(wasNullVariable.set(constantFalse()));

                    BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(
                            context.getCachedInstanceBinder().getCallSiteBinder(),
                            context.getCachedInstanceBinder(),
                            fieldReferenceCompiler(context, wasNullVariable),
                            context.getMetadata().getFunctionRegistry(),
                            ImmutableMap.of(),
                            wasNullVariable);

                    BytecodeNode visitorBody = filter.accept(visitor, context.getMethodScope());
                    Variable result = context.getMethodScope().createTempVariable(boolean.class);
                    block.append(visitorBody)
                            .putVariable(result)
                            .append(new IfStatement()
                                    .condition(wasNullVariable)
                                    .ifTrue(result.set(constantFalse())));

                    return result;
                }

                private RowExpressionVisitor<Scope, BytecodeNode> fieldReferenceCompiler(CrossCompilationContext context, Variable wasNullVariable)
                {
                    return new RowExpressionVisitor<Scope, BytecodeNode>()
                    {
                        @Override
                        public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
                        {
                            int field = node.getField();
                            Type type = node.getType();

                            Class<?> javaType = type.getJavaType();
                            if (!javaType.isPrimitive() && javaType != Slice.class) {
                                javaType = Object.class;
                            }

                            IfStatement ifStatement = new IfStatement();
                            ifStatement.condition(context.isNull(field));

                            ifStatement.ifTrue()
                                    .putVariable(wasNullVariable, true)
                                    .pushJavaDefault(javaType);

                            ifStatement.ifFalse(context.getChannel(field));

                            return ifStatement;
                        }

                        @Override
                        public BytecodeNode visitCall(CallExpression node, Scope scope)
                        {
                            throw new UnsupportedOperationException("not yet implemented");
                        }

                        @Override
                        public BytecodeNode visitConstant(ConstantExpression node, Scope scope)
                        {
                            throw new UnsupportedOperationException("not yet implemented");
                        }
                    };
                }
            };
        }
    }
}
