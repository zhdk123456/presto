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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.ParametricImplementations;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.operator.annotations.ImplementationDependency;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.operator.aggregation.AggregationImplementation.Parser.isAggregationMetaAnnotation;
import static com.facebook.presto.operator.aggregation.AggregationImplementation.Parser.isParameterBlock;
import static com.facebook.presto.operator.aggregation.AggregationImplementation.Parser.isParameterNullable;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.fromSqlType;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.operator.aggregation.state.StateCompiler.generateStateSerializer;
import static com.facebook.presto.operator.annotations.ImplementationDependency.isImplementationDependencyAnnotation;
import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public class BindableAggregationFunction
    extends SqlAggregationFunction
{
    AggregationHeader details;
    ParametricImplementations<AggregationImplementation> implementations;

    public BindableAggregationFunction(Signature signature,
            AggregationHeader details,
            ParametricImplementations implementations)
    {
        super(signature);
        this.details = details;
        this.implementations = implementations;
    }

    @Override
    public String getDescription()
    {
        return details.getDescription().orElse("");
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables variables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Optional<AggregationImplementation> implementation = Optional.empty();

        Signature boundSignature = applyBoundVariables(getSignature(), variables, arity);
        if (implementations.getExactImplementations().containsKey(boundSignature)) {
            implementation = Optional.of(implementations.getExactImplementations().get(boundSignature));
        }
        else {
            for (AggregationImplementation genericImpl : implementations.getGenericImplementations()) {
                if (genericImpl.areTypesAssignable(boundSignature, variables, typeManager, functionRegistry)) {
                    if (implementation.isPresent()) {
                        throw new PrestoException(AMBIGUOUS_FUNCTION_CALL, format("Ambiguous function call (%s) for %s", variables, getSignature()));
                    }
                    implementation = Optional.of(genericImpl);
                }
            }
        }

        if (!implementation.isPresent()) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("Unsupported type parameters (%s) for %s", variables, getSignature()));
        }

        AggregationImplementation concreteImplementation = implementation.get();
        Class<?> definitionClass = concreteImplementation.getDefinitionClass();
        Class<?> stateClass = concreteImplementation.getStateClass();
        Method inputFunction = concreteImplementation.getInputFunction();
        Method outputFunction = concreteImplementation.getOutputFunction();

        List<Type> inputTypes = boundSignature.getArgumentTypes().stream().map(x -> typeManager.getType(x)).collect(toImmutableList());
        Type outputType = typeManager.getType(boundSignature.getReturnType());

        DynamicClassLoader classLoader = new DynamicClassLoader(definitionClass.getClassLoader(), getClass().getClassLoader());

        AggregationMetadata metadata;
        AccumulatorStateSerializer<?> stateSerializer = getAccumulatorStateSerializer(concreteImplementation, variables, typeManager, functionRegistry, stateClass, classLoader);
        Type intermediateType = stateSerializer.getSerializedType();
        Method combineFunction = concreteImplementation.getCombineFunction();
        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(stateClass, classLoader);

        try {
            MethodHandle inputHandle = bindDependencies(inputFunction, concreteImplementation.getInputDependencies(), variables, typeManager, functionRegistry);
            MethodHandle combineHandle = bindDependencies(combineFunction, concreteImplementation.getCombineDependencies(), variables, typeManager, functionRegistry);
            MethodHandle outputHandle = bindDependencies(outputFunction, concreteImplementation.getOutputDependencies(), variables, typeManager, functionRegistry);

            metadata = new AggregationMetadata(
                    generateAggregationName(getSignature().getName(), outputType.getTypeSignature(), signaturesFromTypes(inputTypes)),
                    getParameterMetadata(inputFunction, inputTypes),
                    inputHandle,
                    combineHandle,
                    outputHandle,
                    stateClass,
                    stateSerializer,
                    stateFactory,
                    outputType);
        }
        catch (IllegalAccessException e) {
            throw Throwables.propagate(e);
        }

        AccumulatorFactoryBinder factory = new LazyAccumulatorFactoryBinder(metadata, classLoader);

        return new InternalAggregationFunction(getSignature().getName(),
                inputTypes,
                intermediateType,
                outputType,
                details.isDecomposable(),
                factory);
    }

    // TODO Extract for scalar use case
    private static MethodHandle bindDependencies(Method function, List<ImplementationDependency> dependencies, BoundVariables variables, TypeManager typeManager, FunctionRegistry functionRegistry)
            throws IllegalAccessException
    {
        MethodHandle inputHandle = lookup().unreflect(function);
        for (ImplementationDependency dependency : dependencies) {
            inputHandle = inputHandle.bindTo(dependency.resolve(variables, typeManager, functionRegistry));
        }
        return inputHandle;
    }

    private AccumulatorStateSerializer<?> getAccumulatorStateSerializer(AggregationImplementation implementation, BoundVariables variables, TypeManager typeManager, FunctionRegistry functionRegistry, Class<?> stateClass, DynamicClassLoader classLoader)
    {
        AccumulatorStateSerializer<?> stateSerializer;
        Optional<Method> stateSerializerFactory = implementation.getStateSerializerFactory();
        if (stateSerializerFactory.isPresent()) {
            try {
                MethodHandle factoryHandle = bindDependencies(stateSerializerFactory.get(), implementation.getStateSerializerFactoryDependencies(), variables, typeManager, functionRegistry);
                stateSerializer = (AccumulatorStateSerializer<?>) factoryHandle.invoke();
            }
            catch (Throwable e) {
                throw Throwables.propagate(e);
            }
        }
        else {
            stateSerializer = generateStateSerializer(stateClass, classLoader);
        }
        return stateSerializer;
    }

    public InternalAggregationFunction specialize(BoundVariables variables, int arity, TypeManager typeManager)
    {
        return specialize(variables, arity, typeManager, null);
    }

    private static List<TypeSignature> signaturesFromTypes(List<Type> types)
    {
        return types
                .stream()
                .map(x -> x.getTypeSignature())
                .collect(toImmutableList());
    }

    private static List<ParameterMetadata> getParameterMetadata(@Nullable Method method, List<Type> inputTypes)
    {
        if (method == null) {
            return null;
        }

        ImmutableList.Builder<ParameterMetadata> builder = ImmutableList.builder();

        Annotation[][] annotations = method.getParameterAnnotations();
        String methodName = method.getDeclaringClass() + "." + method.getName();

        checkArgument(annotations.length > 0, "At least @AggregationState argument is required for each of aggregation functions.");

        int inputId = 0;
        int i = 0;
        if (annotations[0].length == 0) {
            // Backward compatibility - first argument without annotations is interpreted as State argument
            builder.add(new ParameterMetadata(STATE));
            i++;
        }

        for (; i < annotations.length; i++) {
            Annotation baseTypeAnnotation = baseTypeAnnotation(annotations[i], methodName);
            if (isImplementationDependencyAnnotation(baseTypeAnnotation)) {
                // skip, this is bound at this point
            }
            else if (baseTypeAnnotation instanceof AggregationState) {
                builder.add(new ParameterMetadata(STATE));
            }
            else if (baseTypeAnnotation instanceof SqlType) {
                builder.add(fromSqlType(inputTypes.get(inputId++), isParameterBlock(annotations[i]), isParameterNullable(annotations[i]), methodName));
            }
            else if (baseTypeAnnotation instanceof BlockIndex) {
                builder.add(new ParameterMetadata(BLOCK_INDEX));
            }
            else if (baseTypeAnnotation instanceof AggregationState) {
                builder.add(new ParameterMetadata(STATE));
            }
            else {
                throw new IllegalArgumentException("Unsupported annotation: " + annotations[i]);
            }
        }
        return builder.build();
    }

    private static Annotation baseTypeAnnotation(Annotation[] annotations, String methodName)
    {
        List<Annotation> baseTypes = Arrays.asList(annotations).stream()
                .filter(annotation -> isAggregationMetaAnnotation(annotation) || annotation instanceof SqlType)
                .collect(toImmutableList());

        checkArgument(baseTypes.size() == 1, "Parameter of %s must have exactly one of @SqlType, @BlockIndex", methodName);

        boolean nullable = isParameterNullable(annotations);
        boolean isBlock = isParameterBlock(annotations);

        Annotation annotation = baseTypes.get(0);
        checkArgument((!isBlock && !nullable) || (annotation instanceof SqlType),
                "%s contains a parameter with @BlockPosition and/or @NullablePosition that is not @SqlType", methodName);

        return annotation;
    }
}
