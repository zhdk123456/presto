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

import com.facebook.presto.Session;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpiller;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SingleStreamSpiller;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.MoreFutures;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.facebook.presto.operator.OuterLookupSource.createOuterLookupSourceSupplier;
import static com.facebook.presto.operator.PartitionedLookupSource.createPartitionedLookupSourceSupplier;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class PartitionedLookupSourceFactory
        implements LookupSourceFactory
{
    private final List<Type> types;
    private final List<Type> outputTypes;
    private final Map<Symbol, Integer> layout;
    private final List<Type> hashChannelTypes;
    private final Supplier<LookupSource>[] partitions;
    private final boolean outer;
    private final CompletableFuture<?> destroyed = new CompletableFuture<>();
    private final List<Integer> hashChannels;
    private final List<Integer> outputChannels;
    private final Optional<Integer> preComputedHashChannel;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final PagesIndex.Factory pagesIndexFactory;

    @GuardedBy("this")
    private int partitionsSet;

    @GuardedBy("this")
    private Map<Integer, SingleStreamSpiller> spilledLookupSources = new HashMap<>();

    @GuardedBy("this")
    private final PartitioningSpillerFactory partitioningSpillerFactory;

    @GuardedBy("this")
    private Supplier<LookupSource> lookupSourceSupplier;

    @GuardedBy("this")
    private final List<SettableFuture<LookupSource>> lookupSourceFutures = new ArrayList<>();

    public PartitionedLookupSourceFactory(
            List<Type> types,
            List<Type> outputTypes,
            List<Integer> hashChannels,
            List<Integer> outputChannels,
            Optional<Integer> preComputedHashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            int partitionCount,
            Map<Symbol, Integer> layout,
            boolean outer,
            PartitioningSpillerFactory partitioningSpillerFactory,
            PagesIndex.Factory pagesIndexFactory)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        this.layout = ImmutableMap.copyOf(layout);
        this.partitions = (Supplier<LookupSource>[]) new Supplier<?>[partitionCount];
        this.outer = outer;
        this.partitioningSpillerFactory = partitioningSpillerFactory;
        this.hashChannels = hashChannels;
        this.outputChannels = outputChannels;
        this.preComputedHashChannel = preComputedHashChannel;
        this.filterFunctionFactory = filterFunctionFactory;
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        hashChannelTypes = hashChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    @Override
    public Map<Symbol, Integer> getLayout()
    {
        return layout;
    }

    @Override
    public synchronized ListenableFuture<LookupSource> createLookupSource()
    {
        if (lookupSourceSupplier != null) {
            return Futures.immediateFuture(lookupSourceSupplier.get());
        }

        SettableFuture<LookupSource> lookupSourceFuture = SettableFuture.create();
        lookupSourceFutures.add(lookupSourceFuture);
        return lookupSourceFuture;
    }

    public void setPartitionLookupSourceSupplier(int partitionIndex, Supplier<LookupSource> partitionLookupSource)
    {
        SetLookupSourceResult result = internalSetLookupSource(partitionIndex, partitionLookupSource);

        if (result.getLookupSourceSupplier() != null) {
            for (SettableFuture<LookupSource> lookupSourceFuture : lookupSourceFutures) {
                lookupSourceFuture.set(lookupSourceSupplier.get());
            }
        }
    }

    public void setPartitionSpilledLookupSourceSupplier(int partitionIndex, SingleStreamSpiller lookupSourceSpiller)
    {
        SetLookupSourceResult result;
        synchronized (this) {
            requireNonNull(lookupSourceSpiller, "lookupSource is null");

            spilledLookupSources.put(partitionIndex, lookupSourceSpiller);

            result = internalSetLookupSource(partitionIndex, () -> new SpilledLookupSource(types.size()));
        }

        if (result.getLookupSourceSupplier() != null) {
            for (SettableFuture<LookupSource> lookupSourceFuture : lookupSourceFutures) {
                lookupSourceFuture.set(lookupSourceSupplier.get());
            }
        }
    }

    private synchronized SetLookupSourceResult internalSetLookupSource(int partitionIndex, Supplier<LookupSource> partitionLookupSource)
    {
        requireNonNull(partitionLookupSource, "partitionLookupSource is null");

        if (destroyed.isDone()) {
            return new SetLookupSourceResult();
        }

        checkState(partitions[partitionIndex] == null, "Partition already set");
        partitions[partitionIndex] = partitionLookupSource;
        partitionsSet++;

        if (partitionsSet == partitions.length) {
            if (partitionsSet != 1) {
                List<Supplier<LookupSource>> partitions = ImmutableList.copyOf(this.partitions);
                this.lookupSourceSupplier = createPartitionedLookupSourceSupplier(partitions, hashChannelTypes, outer);
            }
            else if (outer) {
                this.lookupSourceSupplier = createOuterLookupSourceSupplier(partitionLookupSource);
            }
            else {
                this.lookupSourceSupplier = partitionLookupSource;
            }

            // store lookup source supplier and futures into local variables so they can be used outside of the lock
            return new SetLookupSourceResult(lookupSourceSupplier, ImmutableList.copyOf(this.lookupSourceFutures));
        }
        return new SetLookupSourceResult();
    }

    @Override
    public void destroy()
    {
        synchronized (this) {
            spilledLookupSources.values().forEach(SingleStreamSpiller::close);
            spilledLookupSources.clear();
        }
        destroyed.complete(null);
    }

    public CompletableFuture<?> isDestroyed()
    {
        return MoreFutures.unmodifiableFuture(destroyed);
    }

    @Override
    public synchronized PartitioningSpiller createProbeSpiller(OperatorContext operatorContext, List<Type> probeTypes, HashGenerator probeHashGenerator)
    {
        checkAllFuturesDone();
        ImmutableSet.Builder<Integer> unspilledPartitions = ImmutableSet.builder();
        for (int partition = 0; partition < partitions.length; partition++) {
            if (!spilledLookupSources.containsKey(partition)) {
                unspilledPartitions.add(partition);
            }
        }

        return partitioningSpillerFactory.create(
                probeTypes,
                new LocalPartitionGenerator(probeHashGenerator, partitions.length),
                partitions.length,
                unspilledPartitions.build(),
                () -> operatorContext.getSpillContext().newLocalSpillContext(),
                operatorContext.getSystemMemoryContext().newAggregatedMemoryContext());
    }

    private void checkAllFuturesDone()
    {
        for (SettableFuture<LookupSource> lookupSourceFuture : lookupSourceFutures) {
            checkState(lookupSourceFuture.isDone());
        }
    }

    @Override
    public synchronized Set<Integer> getSpilledPartitions()
    {
        return spilledLookupSources.keySet();
    }

    @Override
    public CompletableFuture<LookupSource> readSpilledLookupSource(Session session, int partition)
    {
        SingleStreamSpiller lookupSourceSpiller = spilledLookupSources.get(partition);
        Iterator<Page> spilledBuildPages = lookupSourceSpiller.getSpilledPages();

        PagesIndex index = pagesIndexFactory.newPagesIndex(types, 10_000);

        while (spilledBuildPages.hasNext()) {
            index.addPage(spilledBuildPages.next());
        }

        return CompletableFuture.completedFuture(
                index.createLookupSourceSupplier(
                        session,
                        hashChannels,
                        preComputedHashChannel,
                        filterFunctionFactory,
                        Optional.of(outputChannels)).get());
    }

    private static class SpilledLookupSource
            implements LookupSource
    {
        private final int channelCount;

        public SpilledLookupSource(int channelCount)
        {
            this.channelCount = channelCount;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public int getChannelCount()
        {
            return channelCount;
        }

        @Override
        public long getInMemorySizeInBytes()
        {
            return 0;
        }

        @Override
        public int getJoinPositionCount()
        {
            throw new IllegalStateException("Illegal access to spilled partition");
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
        {
            throw new IllegalStateException("Illegal access to spilled partition");
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
        {
            throw new IllegalStateException("Illegal access to spilled partition");
        }

        @Override
        public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new IllegalStateException("Illegal access to spilled partition");
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            throw new IllegalStateException("Illegal access to spilled partition");
        }

        @Override
        public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new IllegalStateException("Illegal access to spilled partition");
        }

        @Override
        public void close()
        {
        }
    }

    private static class SetLookupSourceResult
    {
        private final Supplier<LookupSource> lookupSourceSupplier;
        private final List<SettableFuture<LookupSource>> lookupSourceFutures;

        public SetLookupSourceResult(Supplier<LookupSource> lookupSourceSupplier, List<SettableFuture<LookupSource>> lookupSourceFutures)
        {
            this.lookupSourceSupplier = lookupSourceSupplier;
            this.lookupSourceFutures = lookupSourceFutures;
        }

        public SetLookupSourceResult()
        {
            this.lookupSourceSupplier = null;
            this.lookupSourceFutures = ImmutableList.of();
        }

        public Supplier<LookupSource> getLookupSourceSupplier()
        {
            return lookupSourceSupplier;
        }

        public List<SettableFuture<LookupSource>> getLookupSourceFutures()
        {
            return lookupSourceFutures;
        }
    }
}
