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
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.control.WhileLoop;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import com.facebook.presto.bytecode.instruction.JumpInstruction;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.cross.CrossCompilationContext;
import com.facebook.presto.sql.gen.cross.CrossCompilationContext.ChannelBlock;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.equal;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.greaterThanOrEqual;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.notEqual;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
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

    public LookupSource lookupSource;
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
        Variable lookupSource = context.getMethodScope().createTempVariable(LookupSource.class);
        context.getMethodHeader().append(lookupSource.set(context.getField("this").getField("lookupSource", LookupSource.class)));

        BytecodeBlock body = new BytecodeBlock();
        CallSiteBinder callSiteBinder = context.getCachedInstanceBinder().getCallSiteBinder();

        BytecodeExpression probeColumns[] = new BytecodeExpression[probeJoinChannels.size()];
        int probeColumn = 0;
        for (Integer probeJoinChannel : probeJoinChannels) {
            probeColumns[probeColumn++] = context.getChannel(probeJoinChannel);
        }

        Variable rawHash = context.getMethodScope().declareVariable(context.uniqueVariableName("rawHash"), body, constantLong(0L));

        for (int index = 0; index < probeJoinChannels.size(); index++) {
            body
                    .getVariable(rawHash)
                    .push(31L)
                    .append(OpCode.LMUL)
                    // TODO: add support for non bigint columns
                    .append(invokeStatic(AbstractLongType.class, "hash", long.class, probeColumns[index]))
                    .append(OpCode.LADD)
                    .putVariable(rawHash);
        }

        //body.append(joinPosition.set(inMemoryJoinHash.invoke("getJoinPositionFromVlaue", long.class, rawHash, probeColumns[0])));

        Variable pos = context.getMethodScope().declareVariable(
                context.uniqueVariableName("pos"), body,
                lookupSource.invoke("getPos", int.class, rawHash));
        Variable joinPosition = context.getMethodScope().declareVariable(
                context.uniqueVariableName("joinPosition"),
                body, lookupSource.invoke("getJoinPositionFromPos", long.class, pos));

        LabelNode done = new LabelNode("done");
        body.append(new WhileLoop()
                .condition(
                        notEqual(joinPosition, constantLong(-1)))
                .body(new BytecodeBlock()
                        .append(new IfStatement()
                                .condition(
                                        BytecodeExpressions.and(
                                                equal(rawHash.cast(byte.class).cast(int.class), lookupSource.invoke("getPositionHash", byte.class, joinPosition.cast(int.class)).cast(int.class)),
                                                equal(
                                                        // TODO: add support for multiple any type columns
                                                        probeColumns[0],
                                                        lookupSource.invoke(
                                                                "getLongValue",
                                                                long.class,
                                                                joinPosition))))
                                .ifTrue(JumpInstruction.jump(done)))
                        .append(pos.set(lookupSource.invoke("incrementJoinPosition", int.class, pos)))
                        .append(joinPosition.set(lookupSource.invoke("getJoinPositionFromPos", long.class, pos)))));
        body.visitLabel(done);

        for (int index = 0; index < probeTypes.size(); index++) {
            if (probeJoinChannels.contains(index)) {
                final int finalIndex = index;
                context.defineChannel(index, () -> context.getChannel(finalIndex));
                context.defineIsNull(index, BytecodeExpressions::constantFalse);
            }
            else {
                context.mapInputToOutputChannel(index, index);
            }
        }

        BytecodeBlock downStreamBlock = new BytecodeBlock();
        Variable pageAddress = context.getMethodScope().declareVariable(
                context.uniqueVariableName("pageAddress"),
                downStreamBlock,
                lookupSource.invoke("getPageAddress", long.class, joinPosition));

        Variable blockIndex = context.getMethodScope().declareVariable(
                context.uniqueVariableName("blockIndex"),
                downStreamBlock,
                invokeStatic(SyntheticAddress.class, "decodeSliceIndex", int.class, pageAddress));

        Variable blockPosition = context.getMethodScope().declareVariable(
                context.uniqueVariableName("blockPosition"),
                downStreamBlock,
                invokeStatic(SyntheticAddress.class, "decodePosition", int.class, pageAddress));

        for (int index = 0; index < buildTypes.size(); index++) {
            final int channelIndex = index;
            context.defineIsNull(probeTypes.size() + index, BytecodeExpressions::constantFalse); // TODO: add support for null
            context.defineChannel(
                    probeTypes.size() + index,
                    () -> getNativeType(
                            callSiteBinder,
                            buildTypes.get(channelIndex),
                            lookupSource.invoke(
                                    "getBlock",
                                    Block.class,
                                    BytecodeExpressions.constantInt(channelIndex),
                                    blockIndex),
                            blockPosition));
            context.defineChannelBlock(
                    probeTypes.size() + index,
                    () -> new ChannelBlock(
                            lookupSource.invoke(
                                    "getBlock",
                                    Block.class,
                                    BytecodeExpressions.constantInt(channelIndex),
                                    blockIndex),
                            blockPosition)
            );
        }

        downStreamBlock.append(context.processDownstreamOperator());

        body.append(new WhileLoop()
                .condition(greaterThanOrEqual(joinPosition, constantLong(0)))
                .body(new BytecodeBlock()
                        .append(downStreamBlock)
                        .append(joinPosition.set(lookupSource.invoke("getNextJoinPosition", long.class, joinPosition)))));
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
        return ImmutableMap.of("this", new FieldDefinition(this.getClass(), this));
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (lookupSourceFuture.isDone()) {
            if (lookupSource == null) {
                lookupSource = tryGetFutureValue(lookupSourceFuture).orElse(null);
            }
        }
        return lookupSourceFuture;
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
