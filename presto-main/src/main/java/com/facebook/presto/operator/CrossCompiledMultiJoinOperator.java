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
import com.facebook.presto.bytecode.OpCode;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.WhileLoop;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.cross.CrossCompilationContext;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static java.util.Objects.requireNonNull;

public class CrossCompiledMultiJoinOperator
        implements CrossCompiledOperator
{
    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final List<Integer> probeJoinChannels;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;

    private final PageBuilder pageBuilder;
    private final List<Type> probeTypes;
    private final List<Type> buildTypes;

    public InMemoryJoinHash lookupSource;
    private JoinProbe probe1;
    private JoinProbe probe2;

    private boolean closed;
    private long joinPosition = -1;

    public CrossCompiledMultiJoinOperator(
            OperatorContext operatorContext,
            List<Type> types,
            List<Type> probeTypes,
            List<Type> buildTypes,
            List<Integer> probeJoinChannels,
            JoinType joinType,
            ListenableFuture<LookupSource> lookupSourceFuture,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));

        requireNonNull(joinType, "joinType is null");

        this.lookupSourceFuture = requireNonNull(lookupSourceFuture, "lookupSourceFuture is null");
        this.probeJoinChannels = requireNonNull(probeJoinChannels, "probeJoinChannels is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");

        this.pageBuilder = new PageBuilder(types);
    }

    @Override
    public BytecodeBlock process(CrossCompilationContext context)
    {
        BytecodeExpression lookupSourceField = context.getField("this").getField("lookupSource", InMemoryJoinHash.class);

        BytecodeExpression probeColumns[] = new BytecodeExpression[probeJoinChannels.size()];
        for (Integer probeJoinChannel : probeJoinChannels) {
            probeColumns[probeJoinChannel] = context.getChannel(probeJoinChannel);
        }

        Variable joinPosition = context.getMethodScope().createTempVariable(long.class);

        BytecodeBlock body = new BytecodeBlock();

        /*

    public long tmp(long rawHash)
    {
        int pos = getPos(rawHash);

        while (getJoinPositionFromPos(pos) != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(key[pos], (byte) rawHash, rightPosition, hashChannelsPage)) {
                return getNextJoinPositionFrom(key[pos], rightPosition, allChannelsPage);
            }
            // increment position and mask to handler wrap around
            pos = incrementJoinPosition(pos);
        }
        return -1;
    }
         */

        Variable rawHash = context.getMethodScope().createTempVariable(long.class);
        body.push(0L).putVariable(rawHash);

        CallSiteBinder callSiteBinder = context.getCachedInstanceBinder().getCallSiteBinder();
        for (int index = 0; index < probeJoinChannels.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, types.get(index));
            body
                    .getVariable(rawHash)
                    .push(31L)
                    .append(OpCode.LMUL)
                    // TODO: add support for non bigint columns
                    .append(type.invoke("hash", long.class, probeColumns[probeJoinChannels.get(index)]))
                    .append(OpCode.LADD)
                    .putVariable(rawHash);
        }

        Variable pos = context.getMethodScope().createTempVariable(int.class);
        body.append(lookupSourceField.invoke("getPos", int.class, rawHash)).putVariable(pos);
        body.append(new WhileLoop()
                .condition(
                        BytecodeExpressions.add(
                                BytecodeExpressions.notEqual(
                                        lookupSourceField.invoke("getJoinPositionFromPos", int.class, pos),
                                        BytecodeExpressions.constantInt(-1)),
                                BytecodeExpressions.notEqual(
                                        // TODO: add support for multiple any type columns
                                        probeColumns[probeJoinChannels.get(0)],
                                        lookupSourceField.invoke(
                                                "getLongValue",
                                                long.class,
                                                lookupSourceField.invoke("getJoinPositionFromPos", int.class, pos)))))
                .body(new BytecodeBlock()
                        .append(lookupSourceField.invoke("incrementJoinPosition", int.class, pos))
                        .putVariable(pos)));
        body.getVariable(pos).putVariable(joinPosition);

        for (int index = 0; index < probeTypes.size(); index++) {
            context.mapInputToOutputChannel(index, index);
        }

        BytecodeBlock downStreamBlock = new BytecodeBlock();

        //long pageAddress = addresses.getLong(Ints.checkedCast(position));
        Variable pageAddress = context.getMethodScope().createTempVariable(long.class);
        downStreamBlock.append(pageAddress.set(lookupSourceField.invoke("getPageAddress", long.class, joinPosition)));

        Variable blockIndex = context.getMethodScope().createTempVariable(int.class);
        downStreamBlock.append(blockIndex.set(invokeStatic(SyntheticAddress.class, "decodeSliceIndex", long.class, pageAddress)));
        Variable blockPosition = context.getMethodScope().createTempVariable(int.class);
        downStreamBlock.append(blockPosition.set(invokeStatic(SyntheticAddress.class, "decodeSliceIndex", long.class, pageAddress)));

        for (int index = 0; index < buildTypes.size(); index++) {
            final int dupa = index;
            context.defineChannel(
                    probeTypes.size() + index,
                    () -> getNativeType(
                            callSiteBinder,
                            buildTypes.get(dupa),
                            lookupSourceField.invoke(
                                "getBlock",
                                Block.class,
                                BytecodeExpressions.constantInt(dupa),
                                blockIndex),
                            blockPosition));

        }

        downStreamBlock.append(context.processDownstreamOperator());

        body.append(new WhileLoop()
                .condition(BytecodeExpressions.greaterThanOrEqual(joinPosition, BytecodeExpressions.constantInt(0)))
                .body(new BytecodeBlock()
                        .append(downStreamBlock)
                        .append(joinPosition.set(lookupSourceField.invoke("getNextJoinPosition", long.class, joinPosition)))));

        return body;
    }

    private BytecodeExpression getNativeType(CallSiteBinder callSiteBinder, Type type, BytecodeExpression block, BytecodeExpression blockPosition)
    {
        String methodName = "get" + Primitives.wrap(type.getJavaType()).getSimpleName();
        return constantType(callSiteBinder, type).invoke(methodName, type.getJavaType(), block, blockPosition);
    }

    @Override
    public Map<String, FieldDefinition> getFields()
    {
        return ImmutableMap.of("this", new FieldDefinition(CrossCompiledMultiJoinOperator.class, this));
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return Operator.NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return true;
    }

    @Override
    public void close()
    {
        // Closing the lookupSource is always safe to do, but we don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        probe1 = null;
        probe2 = null;
        pageBuilder.reset();
        onClose.run();
        // closing lookup source is only here for index join
        if (lookupSource != null) {
            lookupSource.close();
        }
    }
}
