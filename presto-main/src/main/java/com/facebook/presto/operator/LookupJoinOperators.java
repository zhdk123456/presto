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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.BigintMultiJoinProbeCompiler;
import com.facebook.presto.sql.gen.CrossCompiledJoinProbeCompiler;
import com.facebook.presto.sql.gen.JoinProbeCompiler;
import com.facebook.presto.sql.gen.MultiJoinProbeCompiler;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperatorFactory;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static jersey.repackaged.com.google.common.base.Preconditions.checkArgument;

public class LookupJoinOperators
{
    public enum JoinType {
        INNER,
        PROBE_OUTER, // the Probe is the outer side of the join
        LOOKUP_OUTER, // The LookupSource is the outer side of the join
        FULL_OUTER,
    }

    private LookupJoinOperators()
    {
    }

    private static final JoinProbeCompiler JOIN_PROBE_COMPILER = new JoinProbeCompiler();
    private static final MultiJoinProbeCompiler MULTI_JOIN_PROBE_COMPILER = new MultiJoinProbeCompiler();
    private static final BigintMultiJoinProbeCompiler BIGINT_MULTI_JOIN_PROBE_COMPILER = new BigintMultiJoinProbeCompiler();
    private static final CrossCompiledJoinProbeCompiler CROSS_COMPILED_JOIN_PROBE_COMPILER = new CrossCompiledJoinProbeCompiler();

    public static OperatorFactory innerJoin(int operatorId, PlanNodeId planNodeId, LookupSourceSupplier lookupSourceSupplier, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, boolean filterFunctionPresent)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceSupplier, probeTypes, probeJoinChannel, probeHashChannel, JoinType.INNER, filterFunctionPresent);
    }

    public static OperatorFactory probeOuterJoin(int operatorId, PlanNodeId planNodeId, LookupSourceSupplier lookupSourceSupplier, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, boolean filterFunctionPresent)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceSupplier, probeTypes, probeJoinChannel, probeHashChannel, JoinType.PROBE_OUTER, filterFunctionPresent);
    }

    public static OperatorFactory lookupOuterJoin(int operatorId, PlanNodeId planNodeId, LookupSourceSupplier lookupSourceSupplier, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, boolean filterFunctionPresent)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceSupplier, probeTypes, probeJoinChannel, probeHashChannel, JoinType.LOOKUP_OUTER, filterFunctionPresent);
    }

    public static OperatorFactory fullOuterJoin(int operatorId, PlanNodeId planNodeId, LookupSourceSupplier lookupSourceSupplier, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, boolean filterFunctionPresent)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceSupplier, probeTypes, probeJoinChannel, probeHashChannel, JoinType.FULL_OUTER, filterFunctionPresent);
    }

    public static OperatorFactory multiJoin(
            int operatorId,
            PlanNodeId planNodeId,
            LookupSourceSupplier lookupSourceSupplier1,
            LookupSourceSupplier lookupSourceSupplier2,
            List<? extends Type> probeTypes,
            List<Integer> probeJoinChannel,
            Optional<Integer> probeHashChannel,
            boolean filterFunctionPresent)
    {
        return MULTI_JOIN_PROBE_COMPILER.compileMultiJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceSupplier1,
                lookupSourceSupplier2,
                probeTypes,
                probeJoinChannel,
                probeHashChannel,
                JoinType.INNER,
                filterFunctionPresent);

    }

    public static OperatorFactory bigintMultiJoin(
            int operatorId,
            PlanNodeId planNodeId,
            LookupSourceSupplier lookupSourceSupplier1,
            LookupSourceSupplier lookupSourceSupplier2,
            List<? extends Type> probeTypes,
            List<Integer> probeJoinChannel,
            Optional<Integer> probeHashChannel,
            boolean filterFunctionPresent)
    {
        checkArgument(probeJoinChannel.size() == 1 && probeTypes.get(probeJoinChannel.get(0)).equals(BigintType.BIGINT));
        return BIGINT_MULTI_JOIN_PROBE_COMPILER.compileMultiJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceSupplier1,
                lookupSourceSupplier2,
                probeTypes,
                probeJoinChannel.get(0),
                probeHashChannel,
                JoinType.INNER,
                filterFunctionPresent);
    }

    public static OperatorFactory xcompiledMultiJoin(
            int operatorId,
            PlanNodeId planNodeId,
            LookupSourceSupplier lookupSourceSupplier1,
            LookupSourceSupplier lookupSourceSupplier2,
            List<? extends Type> probeTypes,
            List<Integer> probeJoinChannel,
            Optional<Integer> probeHashChannel,
            boolean filterFunctionPresent)
    {
        ImmutableList.Builder<CrossCompiledOperatorFactory> factories = ImmutableList.builder();

        factories.add(CROSS_COMPILED_JOIN_PROBE_COMPILER.xcompiledMultiJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceSupplier1,
                probeTypes,
                probeJoinChannel,
                probeHashChannel,
                JoinType.INNER));

        List<Type> join2ProbeTypes = new ArrayList<>();
        join2ProbeTypes.addAll(probeTypes);
        join2ProbeTypes.addAll(lookupSourceSupplier1.getTypes());
        factories.add(CROSS_COMPILED_JOIN_PROBE_COMPILER.xcompiledMultiJoinOperatorFactory(
                operatorId + 15,
                planNodeId,
                lookupSourceSupplier2,
                join2ProbeTypes,
                probeJoinChannel,
                probeHashChannel,
                JoinType.INNER));

        return new CombinedOperatorFactory(
                operatorId + 42,
                planNodeId,
                MetadataManager.createTestMetadataManager(),
                factories.build(),
                ImmutableList.copyOf(probeTypes));

    }

}
