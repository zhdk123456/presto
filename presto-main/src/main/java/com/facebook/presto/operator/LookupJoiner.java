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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;

public class LookupJoiner
        implements Closeable
{
    private static final int MAX_POSITIONS_EVALUATED_PER_CALL = 10000;

    private final ListenableFuture<? extends LookupSource> lookupSourceFuture;
    private final JoinProbeFactory joinProbeFactory;

    private final PageBuilder pageBuilder;

    private final boolean probeOnOuterSide;

    private LookupSource lookupSource;
    private JoinProbe probe;

    private long joinPosition = -1;
    private boolean finishing;
    private boolean currentProbePositionProducedRow;

    public LookupJoiner(
            List<Type> types,
            ListenableFuture<LookupSource> lookupSourceFuture,
            JoinProbeFactory joinProbeFactory,
            boolean probeOnOuterSide)
    {
        this.pageBuilder = new PageBuilder(types);
        this.lookupSourceFuture = requireNonNull(lookupSourceFuture, "lookupSourceFuture is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.probeOnOuterSide = probeOnOuterSide;
    }

    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }

        if (lookupSource == null) {
            lookupSource = tryGetFutureValue(lookupSourceFuture).orElse(null);
        }
        return lookupSource != null && probe == null && (probeOnOuterSide || !lookupSource.isEmpty());
    }

    public ListenableFuture<?> isBlocked()
    {
        return lookupSourceFuture;
    }

    public void finish()
    {
        finishing = true;
    }

    public boolean isFinished()
    {
        return (finishing && probe == null && pageBuilder.isEmpty()) || (!probeOnOuterSide && lookupSource != null && lookupSource.isEmpty());
    }

    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(lookupSource != null, "Lookup source has not been built yet");
        checkState(probe == null, "Current page has not been completely processed yet");

        // create probe
        probe = joinProbeFactory.createJoinProbe(lookupSource, page);

        // initialize to invalid join position to force output code to advance the cursors
        joinPosition = -1;
    }

    public Page getOutput()
    {
        if (lookupSource == null) {
            return null;
        }

        // join probe page with the lookup source
        Counter lookupPositionsConsidered = new Counter();
        if (probe != null) {
            while (true) {
                if (probe.getPosition() >= 0) {
                    if (!joinCurrentPosition(lookupPositionsConsidered)) {
                        break;
                    }
                    if (!currentProbePositionProducedRow) {
                        currentProbePositionProducedRow = true;
                        if (!outerJoinCurrentPosition()) {
                            break;
                        }
                    }
                }
                currentProbePositionProducedRow = false;
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
        probe = null;
        pageBuilder.reset();
        // closing lookup source is only here for index join
        if (lookupSource != null) {
            lookupSource.close();
        }
    }

    /**
     * Produce rows matching join condition for the current probe position. If this method was called previously
     * for the current probe position, calling this again will produce rows that wasn't been produced in previous
     * invocations.
     *
     * @return true if all eligible rows have been produced; false otherwise (because pageBuilder became full)
     */
    private boolean joinCurrentPosition(Counter lookupPositionsConsidered)
    {
        // while we have a position on lookup side to join against...
        while (joinPosition >= 0) {
            lookupPositionsConsidered.increment();
            if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                currentProbePositionProducedRow = true;

                pageBuilder.declarePosition();
                // write probe columns
                probe.appendTo(pageBuilder);
                // write build columns
                lookupSource.appendTo(joinPosition, pageBuilder, probe.getOutputChannelCount());
            }

            // get next position on lookup side for this probe row
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());

            if (lookupPositionsConsidered.get() >= MAX_POSITIONS_EVALUATED_PER_CALL) {
                return false;
            }
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return whether there are more positions on probe side
     */
    private boolean advanceProbePosition()
    {
        if (!probe.advanceNextPosition()) {
            probe = null;
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition();
        return true;
    }

    /**
     * Produce a row for the current probe position, if it doesn't match any row on lookup side and this is an outer join.
     *
     * @return whether pageBuilder became full
     */
    private boolean outerJoinCurrentPosition()
    {
        if (probeOnOuterSide && joinPosition < 0) {
            // write probe columns
            pageBuilder.declarePosition();
            probe.appendTo(pageBuilder);

            // write nulls into build columns
            int outputIndex = probe.getOutputChannelCount();
            for (int buildChannel = 0; buildChannel < lookupSource.getChannelCount(); buildChannel++) {
                pageBuilder.getBlockBuilder(outputIndex).appendNull();
                outputIndex++;
            }
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    // This class needs to be public because LookupJoinOperator is isolated.
    public static class Counter
    {
        private int count;

        public void increment()
        {
            count++;
        }

        public int get()
        {
            return count;
        }
    }
}
