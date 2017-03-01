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

import com.facebook.presto.operator.LookupSourceFactory.LookupPartition;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class SpillledLookupJoiner
{
    private final LookupJoiner lookupJoiner;
    private final Iterator<Page> probePages;
    private final CompletableFuture<LookupSource> lookupSourceFuture;
    private final CompletableFuture<LookupPartition> lookupPartitionFuture;

    public SpillledLookupJoiner(
            List<Type> allTypes,
            CompletableFuture<LookupPartition> lookupPartitionFuture,
            JoinProbeFactory joinProbeFactory,
            Iterator<Page> probePages,
            boolean probeOnOuterSide)
    {
        this.lookupPartitionFuture = requireNonNull(lookupPartitionFuture, "lookupPartitionFuture is null");
        this.lookupSourceFuture = lookupPartitionFuture.thenApply(LookupPartition::getLookupSource);
        this.lookupJoiner = new LookupJoiner(allTypes, toListenableFuture(lookupSourceFuture), joinProbeFactory, probeOnOuterSide);
        this.probePages = requireNonNull(probePages, "probePages is null");
    }

    public ListenableFuture<?> isBlocked()
    {
        return lookupJoiner.isBlocked();
    }

    public Page getOutput()
    {
        if (!lookupJoiner.needsInput()) {
            return lookupJoiner.getOutput();
        }
        if (!probePages.hasNext()) {
            lookupJoiner.finish();
            return null;
        }
        Page probePage = probePages.next();
        lookupJoiner.addInput(probePage);

        return lookupJoiner.getOutput();
    }

    public boolean isFinished()
    {
        return lookupJoiner.isFinished();
    }

    public long getInMemorySizeInBytes()
    {
        checkState(lookupSourceFuture.isDone(), "Size is not known yet");
        return getFutureValue(lookupSourceFuture).getInMemorySizeInBytes();
    }

    public void finish()
    {
        checkState(lookupPartitionFuture.isDone());
        lookupPartitionFuture.join().release();
    }
}
