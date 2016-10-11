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

import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.INNER;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class MultiJoinOperatorFactory
        implements JoinOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> probeTypes;
    private final List<Type> buildTypes1;
    private final List<Type> buildTypes2;
    private final JoinType joinType;
    private final LookupSourceSupplier lookupSourceSupplier1;
    private final LookupSourceSupplier lookupSourceSupplier2;
    private final JoinProbeFactory joinProbeFactory;
    private final ReferenceCount referenceCount;
    private boolean closed;

    public MultiJoinOperatorFactory(int operatorId,
            PlanNodeId planNodeId,
            LookupSourceSupplier lookupSourceSupplier1,
            LookupSourceSupplier lookupSourceSupplier2,
            List<Type> probeTypes,
            JoinType joinType,
            JoinProbeFactory joinProbeFactory)
    {
        if (joinType != INNER) {
            throw new NotImplementedException();
        }

        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.lookupSourceSupplier1 = requireNonNull(lookupSourceSupplier1, "lookupSourceSupplier1 is null");
        this.lookupSourceSupplier2 = requireNonNull(lookupSourceSupplier2, "lookupSourceSupplier2 is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildTypes1 = ImmutableList.copyOf(lookupSourceSupplier1.getTypes());
        this.buildTypes2 = ImmutableList.copyOf(lookupSourceSupplier2.getTypes());
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");

        this.referenceCount = new ReferenceCount();

        this.referenceCount.getFreeFuture().addListener(lookupSourceSupplier1::destroy, directExecutor());
        this.referenceCount.getFreeFuture().addListener(lookupSourceSupplier2::destroy, directExecutor());
    }

    private MultiJoinOperatorFactory(MultiJoinOperatorFactory other)
    {
        requireNonNull(other, "other is null");
        operatorId = other.operatorId;
        planNodeId = other.planNodeId;
        probeTypes = other.probeTypes;
        buildTypes1 = other.buildTypes1;
        buildTypes2 = other.buildTypes2;
        joinType = other.joinType;
        lookupSourceSupplier1 = other.lookupSourceSupplier1;
        lookupSourceSupplier2 = other.lookupSourceSupplier2;
        joinProbeFactory = other.joinProbeFactory;
        referenceCount = other.referenceCount;

        referenceCount.retain();
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(buildTypes1)
                .addAll(buildTypes2)
                .build();
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupJoinOperator.class.getSimpleName());

        lookupSourceSupplier1.setTaskContext(driverContext.getPipelineContext().getTaskContext());
        lookupSourceSupplier2.setTaskContext(driverContext.getPipelineContext().getTaskContext());

        referenceCount.retain();
        return new MultiJoinOperator(
                operatorContext,
                getTypes(),
                joinType,
                lookupSourceSupplier1.getLookupSource(),
                lookupSourceSupplier2.getLookupSource(),
                joinProbeFactory,
                referenceCount::release);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        referenceCount.release();
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new MultiJoinOperatorFactory(this);
    }

    @Override
    public Optional<OperatorFactory> createOuterOperatorFactory()
    {
        return Optional.empty();
    }
}
