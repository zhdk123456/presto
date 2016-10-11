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
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.Closeable;
import java.util.List;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;

public class MultiJoinOperator
        implements Operator, Closeable
{
    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture1;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture2;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;

    private final PageBuilder pageBuilder;

    private final boolean probeOnOuterSide;

    private LookupSource lookupSource1;
    private LookupSource lookupSource2;
    private JoinProbe probe1;
    private JoinProbe probe2;

    private boolean closed;
    private boolean finishing;
    private long joinPosition1 = -1;
    private long joinPosition2 = -1;

    private LongArrayList join2Positions = new LongArrayList();

    public MultiJoinOperator(
            OperatorContext operatorContext,
            List<Type> types,
            JoinType joinType,
            ListenableFuture<LookupSource> lookupSourceFuture1,
            ListenableFuture<LookupSource> lookupSourceFuture2,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        requireNonNull(joinType, "joinType is null");
        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;

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
        boolean finished = finishing && probe1 == null && probe2 == null && pageBuilder.isEmpty();

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
        return lookupSource1 != null && lookupSource2 != null && probe1 == null && probe2 == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(lookupSource1 != null, "Lookup source 1 has not been built yet");
        checkState(lookupSource2 != null, "Lookup source 2 has not been built yet");
        checkState(probe1 == null, "Current page has not been completely processed yet");

        // create probe
        probe1 = joinProbeFactory.createJoinProbe(lookupSource1, page);
        probe2 = joinProbeFactory.createJoinProbe(lookupSource2, page);

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
        if (probe1 != null) {
            while (joinCurrentPosition()) {
                if (!advanceProbePosition()) {
                    break;
                }
            }
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty() && probe1 == null && probe2 == null)) {
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
        probe1 = null;
        probe2 = null;
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
        join2Positions.clear();
        long startingJoinPosition2 = joinPosition2;
        while (joinPosition2 >= 0) {
            join2Positions.add(joinPosition2);
            joinPosition2 = lookupSource2.getNextJoinPosition(joinPosition2, probe1.getPosition(), probe1.getPage());
        }

        // while we have a position to join against...
        while (joinPosition1 >= 0) {
            for (long join2Position : join2Positions) {
                pageBuilder.declarePosition();

                // write probe columns
                probe1.appendTo(pageBuilder);

                // write build columns
                lookupSource1.appendTo(joinPosition1, pageBuilder, probe1.getChannelCount());
                lookupSource2.appendTo(join2Position, pageBuilder, probe1.getChannelCount() + lookupSource1.getChannelCount());

                // get next join position for this row
                joinPosition1 = lookupSource1.getNextJoinPosition(joinPosition1, probe1.getPosition(), probe1.getPage());
            }
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
        joinPosition1 = probe1.getCurrentJoinPosition();
        joinPosition2 = probe2.getCurrentJoinPosition();
        return true;
    }
}
