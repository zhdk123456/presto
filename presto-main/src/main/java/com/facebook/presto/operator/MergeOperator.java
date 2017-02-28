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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.execution.SystemMemoryUsageListener;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MergeOperator
        implements SourceOperator, Closeable
{
    public static final ConnectorId REMOTE_CONNECTOR_ID = new ConnectorId("$remote");

    public static class MergeOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final ExchangeClientSupplier exchangeClientSupplier;
        private final PagesSerdeFactory serdeFactory;
        private final List<Type> types;
        private final List<Integer> outputChannels;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private boolean closed;

        public MergeOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                ExchangeClientSupplier exchangeClientSupplier,
                PagesSerdeFactory serdeFactory,
                List<Type> types,
                List<Integer> outputChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.exchangeClientSupplier = exchangeClientSupplier;
            this.serdeFactory = serdeFactory;
            this.types = types;
            this.outputChannels = outputChannels;
            this.sortChannels = sortChannels;
            this.sortOrder = sortOrder;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, MergeOperator.class.getSimpleName());

            return new MergeOperator(
                    operatorContext,
                    types,
                    sourceId,
                    serdeFactory.createPagesSerde(),
                    () -> exchangeClientSupplier.get(new UpdateSystemMemory(driverContext.getPipelineContext())),
                    outputChannels,
                    sortChannels,
                    sortOrder);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    @NotThreadSafe
    private static final class UpdateSystemMemory
            implements SystemMemoryUsageListener
    {
        private final PipelineContext pipelineContext;

        public UpdateSystemMemory(PipelineContext pipelineContext)
        {
            this.pipelineContext = requireNonNull(pipelineContext, "pipelineContext is null");
        }

        @Override
        public void updateSystemMemoryUsage(long deltaMemoryInBytes)
        {
            if (deltaMemoryInBytes > 0) {
                pipelineContext.reserveSystemMemory(deltaMemoryInBytes);
            }
            else {
                pipelineContext.freeSystemMemory(-deltaMemoryInBytes);
            }
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final Supplier<ExchangeClient> exchangeClientSupplier;
    private final List<Integer> outputChannels;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final List<Type> types;
    private final PagesSerde serde;
    private final Set<URI> locations = new HashSet<>();
    private final SettableFuture<Void> isBlockedOnSplits = SettableFuture.create();
    private ListenableFuture<?> isBlockedOnPages = null;
    private MergeSources mergeSources;
    private final Closer closer = Closer.create();

    public MergeOperator(
            OperatorContext operatorContext,
            List<Type> types,
            PlanNodeId sourceId,
            PagesSerde serde,
            Supplier<ExchangeClient> exchangeClientSupplier,
            List<Integer> outputChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.serde = requireNonNull(serde, "serde is null");
        this.types = requireNonNull(types, "types is null");
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.outputChannels = outputChannels;
        this.sortChannels = sortChannels;
        this.sortOrder = sortOrder;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split.getConnectorId().equals(REMOTE_CONNECTOR_ID), "split is not a remote split");

        URI location = ((RemoteSplit) split.getConnectorSplit()).getLocation();
        locations.add(location);
        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        ImmutableList.Builder<MergeSource> builder = ImmutableList.builder();
        for (URI location : locations) {
            ExchangeClient exchangeClient = closer.register(exchangeClientSupplier.get();
            exchangeClient.addLocation(location);
            exchangeClient.noMoreLocations();
            builder.add(new MergeSource(exchangeClient, serde));
            // TODO: figure out how to merge statistics - e.g. operatorContext.setInfoSupplier(exchangeClient::getStatus);
        }

        mergeSources = new MergeSources(builder.build());
        isBlockedOnPages = mergeSources.isBlocked();
        isBlockedOnSplits.set(null);
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
        close();
    }

    @Override
    public boolean isFinished()
    {
        //return exchangeClients.stream().map(ExchangeClient::isFinished).collect(Collectors.toList());
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!isBlockedOnSplits.isDone()) {
            return isBlockedOnSplits;
        } else {
            checkState(isBlockedOnPages != null, "isBlockedOnPages is null");
            return isBlockedOnPages;
        }
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        if (!isBlockedOnPages.isDone()) {
            return null;
        }

        mergeSources
        SerializedPage page = exchangeClient.pollPage();
        if (page == null) {
            return null;
        }

        operatorContext.recordGeneratedInput(page.getSizeInBytes(), page.getPositionCount());
        return serde.deserialize(page);
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    private static class MergeSources {
        private final List<MergeSource> mergeSources;

        public MergeSources(List<MergeSource> mergeSources) {
            this.mergeSources = mergeSources;
        }

        public ListenableFuture<?> isBlocked() {
            return Futures.allAsList(mergeSources.stream().map(MergeSource::isBlocked).collect(Collectors.toList()));
        }

        public Page createMergedPage() {
//            if (!pageInProgress.isPresent()) {
//                // maybe not with Optional<Page>, but keep the page in progress; you might not
//                // be able to return a full page with the inputs that are available now, so if not,
//                // keep the partially merged page and return null.
//            }
            return null;
        }
    }

    private static class MergeSource {

        private final ExchangeClient exchangeClient;
        private final PagesSerde serde;

        private Optional<Page> bufferedPage = Optional.empty();

        public MergeSource(ExchangeClient exchangeClient, PagesSerde serde) {
            this.exchangeClient = exchangeClient;
            this.serde = serde;
        }

        public ListenableFuture<?> isBlocked() {
            if (bufferedPage.isPresent()) {
                return NOT_BLOCKED;
            }
            return exchangeClient.isBlocked();
        }

        public Page getPage() {
            if (bufferedPage.isPresent()) {
                return bufferedPage.get();
            }

            checkState(exchangeClient.isBlocked().isDone());
            SerializedPage serializedPage = exchangeClient.pollPage();
            checkState(serializedPage != null, "serializedPage is null");
            bufferedPage = Optional.of(serde.deserialize(serializedPage));
            return bufferedPage.get();
        }
    }

    //private static class PageWithPosi
}
