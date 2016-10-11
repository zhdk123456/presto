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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;

public class BigintMultiJoinOperator
        implements Operator, Closeable
{
    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture1;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture2;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;

    private final PageBuilder pageBuilder;

    private final Integer probeJoinChannel;

    private LookupSource lookupSource1;
    private LookupSource lookupSource2;
    private JoinProbe probe;

    private boolean closed;
    private boolean finishing;
    private long joinPosition1 = -1;
    private long joinPosition2 = -1;

    public BigintMultiJoinOperator(
            OperatorContext operatorContext,
            List<Type> types,
            JoinType joinType,
            int probeJoinChannel,
            ListenableFuture<LookupSource> lookupSourceFuture1,
            ListenableFuture<LookupSource> lookupSourceFuture2,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        requireNonNull(joinType, "joinType is null");
        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError

        this.probeJoinChannel = requireNonNull(probeJoinChannel, "probeJoinChannel is null");
        this.lookupSourceFuture1 = requireNonNull(lookupSourceFuture1, "lookupSourceFuture1 is null");
        this.lookupSourceFuture2 = requireNonNull(lookupSourceFuture2, "lookupSourceFuture2 is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");

        this.pageBuilder = new PageBuilder(types);
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
        boolean finished = finishing && probe == null && pageBuilder.isEmpty();

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (lookupSourceFuture1.isDone()) {
            return lookupSourceFuture2;
        }
        return lookupSourceFuture1;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }

        if (lookupSource1 == null) {
            lookupSource1 = tryGetFutureValue(lookupSourceFuture1).orElse(null);
        }
        if (lookupSource2 == null) {
            lookupSource2 = tryGetFutureValue(lookupSourceFuture2).orElse(null);
        }
        return lookupSource1 != null && lookupSource2 != null && probe == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(lookupSource1 != null, "Lookup source 1 has not been built yet");
        checkState(lookupSource2 != null, "Lookup source 2 has not been built yet");
        checkState(probe == null, "Current page has not been completely processed yet");

        // create probe
        probe = joinProbeFactory.createJoinProbe(lookupSource1, page);

        // initialize to invalid join position to force output code to advance the cursors
        joinPosition1 = -1;
        joinPosition2 = -1;
    }

    @Override
    public Page getOutput()
    {
        if (lookupSource1 == null || lookupSource2 == null) {
            return null;
        }

        // join probe page with the lookup source
        if (probe != null) {
            while (joinCurrentPosition()) {
                if (!advanceProbePosition()) {
                    break;
                }
            }
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty() && probe == null)) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }

    @Override
    public void close()
    {
        // Closing the lookupSource is always safe to do, but we don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        probe = null;
        pageBuilder.reset();
        onClose.run();
        // closing lookup source is only here for index join
        if (lookupSource1 != null) {
            lookupSource1.close();
        }
        // closing lookup source is only here for index join
        if (lookupSource2 != null) {
            lookupSource2.close();
        }
    }

    private boolean joinCurrentPosition()
    {
        // while we have a position to join against...
        while (joinPosition1 >= 0) {
            long startingJoinPosition2 = joinPosition2;
            while (joinPosition2 >= 0) {
                pageBuilder.declarePosition();

                // write probe columns
                probe.appendTo(pageBuilder);

                // write build columns
                lookupSource1.appendTo(joinPosition1, pageBuilder, probe.getChannelCount());
                lookupSource2.appendTo(joinPosition2, pageBuilder, probe.getChannelCount() + lookupSource1.getChannelCount());

                // get next join position for this row
                joinPosition2 = lookupSource2.getNextJoinPosition(joinPosition2, probe.getPosition(), probe.getPage());
            }
            joinPosition1 = lookupSource1.getNextJoinPosition(joinPosition1, probe.getPosition(), probe.getPage());

            if (pageBuilder.isFull()) {
                joinPosition2 = startingJoinPosition2; // revert join
                return false;
            }
        }
        return true;
    }

    private boolean advanceProbePosition()
    {
        if (!probe.advanceNextPosition()) {
            probe = null;
            return false;
        }
        int probePosition = probe.getPosition();
        long probeValue = probe.getPage().getBlock(probeJoinChannel).getLong(probePosition, 0);

        joinPosition1 = lookupSource1.getJoinPositionFromVlaue(probeValue);
        joinPosition2 = lookupSource2.getJoinPositionFromVlaue(probeValue);

        return true;
    }
}
