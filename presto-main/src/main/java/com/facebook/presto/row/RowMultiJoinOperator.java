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
package com.facebook.presto.row;

import com.facebook.presto.operator.JoinProbeFactory;
import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;

public class RowMultiJoinOperator
        implements RowOperator
{
    private final int id;
    private final List<Type> types;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;

    private final PageBuilder pageBuilder;

    private LookupSource lookupSource;

    private RowObject probe = null;
    private boolean closed;
    private long joinPosition = -1;

    public RowMultiJoinOperator(
            OperatorContext operatorContext,
            List<Type> types,
            JoinType joinType,
            ListenableFuture<LookupSource> lookupSourceFuture,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose,
            int id)
    {
        this.id = id;
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        requireNonNull(joinType, "joinType is null");

        this.lookupSourceFuture = requireNonNull(lookupSourceFuture, "lookupSourceFuture is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");

        this.pageBuilder = new PageBuilder(types);
    }

    @Override
    public void prepare()
    {
        if (lookupSource == null) {
            lookupSource = tryGetFutureValue(lookupSourceFuture).orElse(null);
        }
    }

    @Override
    public void addInput(RowObject row)
    {
        checkState(lookupSource != null, "Lookup source 1 has not been built yet");

        probe = row;
        joinPosition = -1;
    }

    @Override
    public RowObject getOutput()
    {
        if (lookupSource == null) {
            return null;
        }
        if (probe == null) {
            return null;
        }

        // join probe page with the lookup source
        if (joinPosition < 0) {
            joinPosition = lookupSource.getJoinPosition(probe, id);
        }
        else {
            joinPosition = lookupSource.getNextJoinPosition(joinPosition);
        }
        if (joinPosition < 0) {
            probe = null;
            return null;
        }
        lookupSource.appendTo(joinPosition, probe, id);

        return probe;
    }
}
