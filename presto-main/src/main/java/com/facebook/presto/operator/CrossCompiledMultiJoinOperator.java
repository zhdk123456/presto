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
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.control.WhileLoop;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import com.facebook.presto.bytecode.instruction.InvokeInstruction;
import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.cross.CrossCompilationContext;
import com.facebook.presto.sql.gen.cross.CrossCompiledOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThan;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.google.common.base.Preconditions.checkState;
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

    public InMemoryJoinHash lookupSource;
    private JoinProbe probe1;
    private JoinProbe probe2;

    private boolean closed;
    private long joinPosition = -1;

    public CrossCompiledMultiJoinOperator(
            OperatorContext operatorContext,
            List<Type> types,
            List<Integer> probeJoinChannels,
            JoinType joinType,
            ListenableFuture<LookupSource> lookupSourceFuture,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

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

        String joinPositionName = context.uniqueVariableName("joinPosition");
        Variable joinPosition = context.getMethodScope().declareVariable(long.class, joinPositionName);

        BytecodeBlock body = new BytecodeBlock();
        body.append(
                joinPosition);

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

        Variable rawHash = context.getMethodScope().declareVariable(long.class, context.uniqueVariableName("rawHash"));
        body.push(0L).putVariable(rawHash);

        CallSiteBinder callSiteBinder = context.getCachedInstanceBinder().getCallSiteBinder();
        for (int index = 0; index < probeJoinChannels.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, types.get(index));
            body
                    .getVariable(rawHash)
                    .push(31L)
                    .append(OpCode.LMUL)
                    .append(type.invoke("hash", long.class, probeColumns[index])) // TODO: this will not work for non Bigint
                    .append(OpCode.LADD)
                    .putVariable(rawHash);
        }

        Variable pos = context.getMethodScope().declareVariable(int.class, context.uniqueVariableName("pos"));

        body.append(lookupSourceField.invoke("getPos", int.class, rawHash)).putVariable(pos);
        body.append(new WhileLoop()
                .condition(lookupSourceField.invoke("getJoinPositionFromPos", int.class, pos))
                .body(new BytecodeBlock()
                        .append(/* if break*/)
                        .append(lookupSourceField.invoke("incrementJoinPosition", int.class, pos))
                        .putVariable(pos)));
        body.getVariable(pos).putVariable(joinPosition);

        for (int index = 0; index < probeJoinChannels.size(); index++) {
            context.mapInputToOutputChannel(index, index);
        }

        context.defineChannel(probeJoinChannels.size() + 1, () -> lookupSourceField.invoke("getChannel", Block.class, BytecodeExpressions.constantInt(0).invoke("getLong", long.class, joinPosition)));
        BytecodeBlock downStreamBlock = context.processDownstreamOperator();

        body.append(new WhileLoop()
                .condition(BytecodeExpressions.lessThanOrEqual(joinPosition, BytecodeExpressions.constantInt(0)))
                .body(new BytecodeBlock().append(downStreamBlock)));

        return body;
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
        // closing lookup source is only here for index join
        if (lookupSource2 != null) {
            lookupSource2.close();
        }
    }

    private boolean joinCurrentPosition()
    {
        // while we have a position to join against...
        while (joinPosition >= 0) {
            long startingJoinPosition2 = joinPosition2;
            while (joinPosition2 >= 0) {
                pageBuilder.declarePosition();

                // write probe columns
                probe1.appendTo(pageBuilder);

                // write build columns
                lookupSource.appendTo(joinPosition, pageBuilder, probe1.getChannelCount());
                lookupSource2.appendTo(joinPosition2, pageBuilder, probe1.getChannelCount() + lookupSource.getChannelCount());

                // get next join position for this row
                joinPosition2 = lookupSource.getNextJoinPosition(joinPosition2, probe1.getPosition(), probe1.getPage());
            }
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe1.getPosition(), probe1.getPage());

            if (pageBuilder.isFull()) {
                joinPosition2 = startingJoinPosition2; // revert join
                return false;
            }
        }
        return true;
    }

    private boolean advanceProbePosition()
    {
        boolean hasNext1 = probe1.advanceNextPosition();
        boolean hasNext2 = probe2.advanceNextPosition();
        checkState(hasNext1 == hasNext2);
        if (!hasNext1) {
            probe1 = null;
            probe2 = null;
            return false;
        }

        // update join position
        joinPosition = probe1.getCurrentJoinPosition();
        joinPosition2 = probe2.getCurrentJoinPosition();
        return true;
    }
}
