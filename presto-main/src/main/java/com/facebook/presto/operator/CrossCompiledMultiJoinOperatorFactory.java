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
import com.facebook.presto.sql.gen.cross.CrossCompiledOperator;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperatorFactory;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.INNER;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class CrossCompiledMultiJoinOperatorFactory
        implements CrossCompiledOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> probeTypes;
    private final List<Type> buildTypes;
    private final JoinType joinType;
    private final LookupSourceSupplier lookupSourceSupplier;
    private final JoinProbeFactory joinProbeFactory;
    private final ReferenceCount referenceCount;
    private final ImmutableList<Integer> probeJoinChannels;
    private boolean closed;

    public CrossCompiledMultiJoinOperatorFactory(int operatorId,
            PlanNodeId planNodeId,
            LookupSourceSupplier lookupSourceSupplier,
            List<Type> probeTypes,
            List<Integer> probeJoinChannels,
            JoinType joinType,
            JoinProbeFactory joinProbeFactory)
    {
        if (joinType != INNER) {
            throw new NotImplementedException();
        }

        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.lookupSourceSupplier = requireNonNull(lookupSourceSupplier, "lookupSourceSupplier is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildTypes = ImmutableList.copyOf(this.lookupSourceSupplier.getTypes());
        this.probeJoinChannels = ImmutableList.copyOf(requireNonNull(probeJoinChannels, "probeJoinChannels is null"));
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");

        this.referenceCount = new ReferenceCount();

        this.referenceCount.getFreeFuture().addListener(this.lookupSourceSupplier::destroy, directExecutor());
    }

    private CrossCompiledMultiJoinOperatorFactory(CrossCompiledMultiJoinOperatorFactory other)
    {
        requireNonNull(other, "other is null");
        operatorId = other.operatorId;
        planNodeId = other.planNodeId;
        probeTypes = other.probeTypes;
        buildTypes = other.buildTypes;
        joinType = other.joinType;
        probeJoinChannels = other.probeJoinChannels;
        lookupSourceSupplier = other.lookupSourceSupplier;
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
                .addAll(buildTypes)
                .build();
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        throw new NotImplementedException();
    }

    @Override
    public CrossCompiledOperator createCrossCompiledOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupJoinOperator.class.getSimpleName());

        lookupSourceSupplier.setTaskContext(driverContext.getPipelineContext().getTaskContext());

        referenceCount.retain();
        return new CrossCompiledMultiJoinOperator(
                operatorContext,
                getTypes(),
                probeJoinChannels,
                joinType,
                lookupSourceSupplier.getLookupSource(),
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
        return new CrossCompiledMultiJoinOperatorFactory(this);
    }
}
