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
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.CachedInstanceBinder;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.cross.CrossCompilationContext;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperator;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperatorFactory;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThan;
import static com.facebook.presto.bytecode.instruction.JumpInstruction.jump;
import static com.facebook.presto.sql.gen.BytecodeUtils.generateWrite;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CombinedOperatorFactory
        implements OperatorFactory
{
    private final Metadata metadata;
    private final List<CrossCompiledOperatorFactory> factories;
    private final List<Type> inputTypes;
    private final int operatorId;
    private final PlanNodeId planNodeId;

    private static final CrossCompiledOperator INIT_OPERATOR = new CrossCompiledOperator()
    {
        @Override
        public BytecodeBlock process(CrossCompilationContext context)
        {
            return null;
        }

        @Override
        public Map<String, CrossCompiledOperator.FieldDefinition> getFields()
        {
            return null;
        }

        @Override
        public void close()
        {
        }
    };

    private static class CrossOperatorContext
    {
        final CrossCompiledOperator operator;
        final BytecodeBlock header = new BytecodeBlock();
        final Map<CrossCompiledOperator, Map<Integer, BytecodeExpression>> definedChannels;
        final Map<CrossCompiledOperator, Map<Integer, BytecodeExpression>> definedIsNull;

        CrossOperatorContext(CrossCompiledOperator operator)
        {
            this.operator = operator;
            this.definedChannels = new HashMap<>();
            this.definedIsNull = new HashMap<>();
        }
    }

    private static class CrossContext
            implements CrossCompilationContext
    {
        final Metadata metadata;
        final Variable thisVariable;
        final CachedInstanceBinder cachedInstanceBinder;
        final Scope methodScope;
        final BytecodeBlock methodHeader;
        final List<CrossCompiledOperator> remainingOperators;
        final List<CrossOperatorContext> operatorStack = new ArrayList<>();
        final Map<CrossCompiledOperator, Map<Integer, Supplier<BytecodeExpression>>> channelDefinitions = new HashMap<>();
        final Map<CrossCompiledOperator, Map<Integer, Supplier<BytecodeExpression>>> isNullDefinitions = new HashMap<>();

        int topOperatorIndex = 0;
        int currentOperatorIndex = 0;
        int variableNumber;

        CrossContext(
                Metadata metadata,
                Variable thisVariable,
                CachedInstanceBinder cachedInstanceBinder,
                Scope methodScope,
                BytecodeBlock methodHeader,
                Map<Integer, Supplier<BytecodeExpression>> inputChannelDefinitions,
                Map<Integer, Supplier<BytecodeExpression>> inputIsNullDefinitions,
                List<CrossCompiledOperator> operators)
        {
            this.metadata = metadata;
            this.thisVariable = thisVariable;
            this.cachedInstanceBinder = cachedInstanceBinder;
            this.methodScope = methodScope;
            this.methodHeader = methodHeader;
            this.remainingOperators = new ArrayList<>(operators);

            operatorStack.add(new CrossOperatorContext(INIT_OPERATOR));
            channelDefinitions.put(INIT_OPERATOR, inputChannelDefinitions);
            isNullDefinitions.put(INIT_OPERATOR, inputIsNullDefinitions);
        }

        @Override
        public Metadata getMetadata()
        {
            return metadata;
        }

        @Override
        public CachedInstanceBinder getCachedInstanceBinder()
        {
            return cachedInstanceBinder;
        }

        @Override
        public Scope getMethodScope()
        {
            return methodScope;
        }

        @Override
        public BytecodeBlock getMethodHeader()
        {
            return methodHeader;
        }

        @Override
        public String uniqueVariableName(String name)
        {
            return "variable" + currentOperatorIndex + name;
        }

        @Override
        public BytecodeExpression getField(String name)
        {
            return thisVariable.getField("field" + (currentOperatorIndex - 1) + name, operator(currentOperatorIndex).getFields().get(name).getType());
        }

        @Override
        public BytecodeExpression getChannel(int channel)
        {
            CrossCompiledOperator parentOperator = operator(currentOperatorIndex - 1);
            Optional<BytecodeExpression> channelExpression = getChannel(parentOperator, channel);
            if (channelExpression.isPresent()) {
                return channelExpression.get();
            }

            BytecodeExpression definedChannelExpression = computeParentChannelDefinition(channel);
            if (definedChannelExpression instanceof Variable) {
                return definedChannelExpression;
            }

            CrossOperatorContext topOperatorContext = topOperatorContext();
            Variable definedChannelVariable = methodScope.declareVariable("variable" + (variableNumber++), topOperatorContext.header, definedChannelExpression);
            topOperatorDefinedChannels(parentOperator).put(channel, definedChannelVariable);
            return definedChannelVariable;
        }

        @Override
        public BytecodeExpression isNull(int channel)
        {
            CrossCompiledOperator parentOperator = operator(currentOperatorIndex - 1);
            Optional<BytecodeExpression> isNullExpression = getIsNull(parentOperator, channel);
            if (isNullExpression.isPresent()) {
                return isNullExpression.get();
            }

            BytecodeExpression definedIsNullExpression = computeParentIsNullDefinition(channel);
            if (definedIsNullExpression instanceof Variable) {
                return definedIsNullExpression;
            }

            CrossOperatorContext topOperatorContext = topOperatorContext();
            Variable definedIsNullVariable = methodScope.declareVariable("isNull" + (variableNumber++), topOperatorContext.header, definedIsNullExpression);
            topOperatorDefinedIsNull(parentOperator).put(channel, definedIsNullVariable);
            return definedIsNullVariable;
        }

        Optional<BytecodeExpression> getChannel(CrossCompiledOperator operator, int channel)
        {
            for (int operatorIndex = topOperatorIndex; operatorIndex >= 0; --operatorIndex) {
                if (operatorDefinedChannels(operatorIndex, operator).containsKey(channel)) {
                    return Optional.of(operatorDefinedChannels(operatorIndex, operator).get(channel));
                }
            }
            return Optional.empty();
        }

        Optional<BytecodeExpression> getIsNull(CrossCompiledOperator operator, int channel)
        {
            for (int operatorIndex = topOperatorIndex; operatorIndex >= 0; --operatorIndex) {
                if (operatorDefinedIsNull(operatorIndex, operator).containsKey(channel)) {
                    return Optional.of(operatorDefinedIsNull(operatorIndex, operator).get(channel));
                }
            }
            return Optional.empty();
        }

        BytecodeExpression computeParentChannelDefinition(int channel)
        {
            currentOperatorIndex--;
            try {
                return channelDefinitions.get(operator(currentOperatorIndex)).get(channel).get();
            }
            finally {
                currentOperatorIndex++;
            }
        }

        BytecodeExpression computeParentIsNullDefinition(int channel)
        {
            currentOperatorIndex--;
            try {
                return isNullDefinitions.get(operator(currentOperatorIndex)).get(channel).get();
            }
            finally {
                currentOperatorIndex++;
            }
        }

        @Override
        public void defineChannel(int channel, Supplier<BytecodeExpression> definition)
        {
            channelDefinitions
                    .computeIfAbsent(topOperator(), operator -> new HashMap<>())
                    .put(channel, definition);
        }

        @Override
        public void defineIsNull(int channel, Supplier<BytecodeExpression> definition)
        {
            isNullDefinitions
                    .computeIfAbsent(topOperator(), operator -> new HashMap<>())
                    .put(channel, definition);
        }

        @Override
        public BytecodeBlock processDownstreamOperator()
        {
            operatorStack.add(new CrossOperatorContext(remainingOperators.remove(0)));
            currentOperatorIndex = ++topOperatorIndex;
            BytecodeBlock header = topOperatorContext().header;
            BytecodeBlock processBlock = topOperator().process(this);
            return header.append(processBlock);
        }

        Map<Integer, BytecodeExpression> topOperatorDefinedChannels(CrossCompiledOperator operator)
        {
            return operatorDefinedChannels(operatorStack.size() - 1, operator);
        }

        Map<Integer, BytecodeExpression> operatorDefinedChannels(int index, CrossCompiledOperator operator)
        {
            return operatorStack.get(index).definedChannels.computeIfAbsent(operator, op -> new HashMap<>());
        }

        Map<Integer, BytecodeExpression> topOperatorDefinedIsNull(CrossCompiledOperator operator)
        {
            return operatorDefinedIsNull(operatorStack.size() - 1, operator);
        }

        Map<Integer, BytecodeExpression> operatorDefinedIsNull(int index, CrossCompiledOperator operator)
        {
            return operatorStack.get(index).definedIsNull.computeIfAbsent(operator, op -> new HashMap<>());
        }

        CrossCompiledOperator topOperator()
        {
            return operator(operatorStack.size() - 1);
        }

        CrossCompiledOperator operator(int index)
        {
            return operatorStack.get(index).operator;
        }

        CrossOperatorContext topOperatorContext()
        {
            return operatorContext(operatorStack.size() - 1);
        }

        CrossOperatorContext operatorContext(int index)
        {
            return operatorStack.get(index);
        }
    }

    public interface PageProcessor
    {
        int process(ConnectorSession session, Page page, int start, int end, PageBuilder pageBuilder);
    }

    private static class CombinedOperator
            implements Operator
    {
        final OperatorContext operatorContext;
        final List<CrossCompiledOperator> operators;
        final List<Type> types;
        final PageBuilder pageBuilder;
        final PageProcessor processor;
        Page currentPage;
        int currentPosition;
        boolean finishing;

        public CombinedOperator(
                OperatorContext operatorContext,
                List<CrossCompiledOperator> operators,
                PageProcessor processor,
                Iterable<? extends Type> types)
        {
            this.processor = requireNonNull(processor, "processor is null");
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.operators = requireNonNull(operators, "operators is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
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
        public ListenableFuture<?> isBlocked()
        {
            return Futures.allAsList(operators.stream()
                    .map(CrossCompiledOperator::isBlocked)
                    .collect(toList()));
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
                currentPosition = processor.process(operatorContext.getSession().toConnectorSession(), currentPage, currentPosition, currentPage.getPositionCount(), pageBuilder);
                if (currentPosition == currentPage.getPositionCount()) {
                    currentPage = null;
                    currentPosition = 0;
                }
            }

            if (!finishing && !pageBuilder.isFull() || pageBuilder.isEmpty()) {
                return null;
            }

            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        @Override
        public void close()
        {
            operators.stream().forEach(CrossCompiledOperator::close);
        }
    }

    public CombinedOperatorFactory(int operatorId, PlanNodeId planNodeId, Metadata metadata, List<CrossCompiledOperatorFactory> factories, List<Type> inputTypes)
    {
        this.operatorId = operatorId;
        this.planNodeId = planNodeId;
        this.metadata = metadata;
        this.factories = factories;
        this.inputTypes = inputTypes;
    }

    @Override
    public List<Type> getTypes()
    {
        return factories.get(factories.size() - 1).getTypes();
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        List<CrossCompiledOperator> operators = factories.stream()
                .map(factory -> factory.createCrossCompiledOperator(driverContext))
                .collect(toList());

        Class<? extends PageProcessor> compiledPageProcessor = compilePageProcessor(operators);

        List<Object> args = new ArrayList<>();
        List<Class<?>> constructorTypes = new ArrayList<>();
        for (CrossCompiledOperator operator : operators) {
            for (Map.Entry<String, CrossCompiledOperator.FieldDefinition> entry : operator.getFields().entrySet()) {
                args.add(entry.getValue().getValue());
                constructorTypes.add(entry.getValue().getType());
            }
        }

        PageProcessor processor;
        try {
            Object[] argsArray = args.toArray(new Object[args.size()]);
            Class<?>[] constructorTypesArray = constructorTypes.toArray(new Class<?>[constructorTypes.size()]);
            Constructor<? extends PageProcessor> constructor = compiledPageProcessor.getConstructor(constructorTypesArray);
            processor = constructor.newInstance(argsArray);
        }
        catch (Exception e) {
            Throwables.propagate(e);
            return null;
        }

        return new CombinedOperator(driverContext.addOperatorContext(operatorId, new PlanNodeId("Cross"), "CROSS"), operators, processor, getTypes());
    }

    @Override
    public void close()
    {
        factories.stream().forEach(OperatorFactory::close);
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new CombinedOperatorFactory(operatorId, planNodeId, metadata, factories, inputTypes);
    }

    private Class<? extends PageProcessor> compilePageProcessor(List<CrossCompiledOperator> operators)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(PageProcessor.class.getSimpleName()),
                type(Object.class),
                type(PageProcessor.class));
        CallSiteBinder callSiteBinder = new CallSiteBinder();
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        generateProcessMethod(classDefinition, cachedInstanceBinder, operators);
        generateConstructor(classDefinition, cachedInstanceBinder, operators);

        return defineClass(classDefinition, PageProcessor.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private void generateProcessMethod(ClassDefinition classDefinition, CachedInstanceBinder cachedInstanceBinder, List<CrossCompiledOperator> operators)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter start = arg("start", int.class);
        Parameter end = arg("end", int.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(int.class), session, page, start, end, pageBuilder);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        // extract blocks
        ImmutableMap.Builder<Integer, Variable> builder = ImmutableMap.builder();
        for (int channel = 0; channel < inputTypes.size(); ++channel) {
            Variable blockVariable = scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
            builder.put(channel, blockVariable);
        }
        Map<Integer, Variable> channelBlocks = builder.build();

        // process loop
        Variable position = scope.declareVariable(int.class, "position");
        BytecodeBlock header = new BytecodeBlock();
        body.append(header);
        Map<Integer, Supplier<BytecodeExpression>> channelDefinitions = channelBlocks.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> getInputChannelDefinition(cachedInstanceBinder, entry.getKey(), entry.getValue(), position)));
        Map<Integer, Supplier<BytecodeExpression>> isNullDefinitions = channelBlocks.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> getInputIsNullDefinition(entry.getValue(), position)));
        operators = ImmutableList.<CrossCompiledOperator>builder()
                .addAll(operators)
                .add(rowAppender(pageBuilder, scope))
                .build();
        CrossContext context = new CrossContext(metadata, thisVariable, cachedInstanceBinder, scope, header, channelDefinitions, isNullDefinitions, operators);
        BytecodeBlock processBlock = context.processDownstreamOperator();

        // for loop loop body
        LabelNode done = new LabelNode("done");
        ForLoop loop = new ForLoop()
                .initialize(position.set(start))
                .condition(lessThan(position, end))
                .update(position.set(add(position, constantInt(1))))
                .body(new BytecodeBlock()
                        .append(new IfStatement()
                                .condition(pageBuilder.invoke("isFull", boolean.class))
                                .ifTrue(jump(done)))
                        .append(processBlock));

        body
                .append(loop)
                .visitLabel(done)
                .append(position.ret());
    }

    private CrossCompiledOperator rowAppender(final Parameter pageBuilder, final Scope scope)
    {
        return new CrossCompiledOperator()
        {
            @Override
            public BytecodeBlock process(CrossCompilationContext context)
            {
                BytecodeBlock block = new BytecodeBlock();
                block.append(pageBuilder.invoke("declarePosition", void.class));
                for (int channel = 0; channel < getTypes().size(); ++channel) {
                    block.append(pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, constantInt(channel)))
                            .append(context.getChannel(channel))
                            .append(generateWrite(context.getCachedInstanceBinder().getCallSiteBinder(), scope, context.isNull(channel), getTypes().get(channel)));
                }
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
        };
    }

    private Supplier<BytecodeExpression> getInputChannelDefinition(CachedInstanceBinder cachedInstanceBinder, int channel, BytecodeExpression block, BytecodeExpression position)
    {
        return () -> {
            Type type = inputTypes.get(channel);
            Class<?> javaType = type.getJavaType();
            if (!javaType.isPrimitive() && javaType != Slice.class) {
                javaType = Object.class;
            }
            String methodName = "get" + Primitives.wrap(javaType).getSimpleName();
            return constantType(cachedInstanceBinder.getCallSiteBinder(), type).invoke(methodName, javaType, block, position);
        };
    }

    private Supplier<BytecodeExpression> getInputIsNullDefinition(BytecodeExpression block, BytecodeExpression position)
    {
        return () -> block.invoke("isNull", boolean.class, position);
    }

    private void generateConstructor(ClassDefinition classDefinition, CachedInstanceBinder cachedInstanceBinder, List<CrossCompiledOperator> operators)
    {
        Map<String, Parameter> parameters = new HashMap<>();
        Map<String, FieldDefinition> fields = new HashMap<>();
        List<Parameter> parametersList = new ArrayList<>();
        for (int operatorIndex = 0; operatorIndex < operators.size(); ++operatorIndex) {
            for (Map.Entry<String, CrossCompiledOperator.FieldDefinition> entry : operators.get(operatorIndex).getFields().entrySet()) {
                String name = "field" + operatorIndex + entry.getKey();
                Parameter parameter = arg(name, type(entry.getValue().getType()));
                parameters.put(name, parameter);
                parametersList.add(parameter);
                fields.put(name, classDefinition.declareField(a(PRIVATE, FINAL), name, entry.getValue().getType()));
            }
        }

        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), parametersList);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        parameters.entrySet().stream()
                .forEach(entry -> body.append(thisVariable.setField(fields.get(entry.getKey()), entry.getValue())));

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();
    }
}
