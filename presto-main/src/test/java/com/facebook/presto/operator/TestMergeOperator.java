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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.execution.buffer.TestingPagesSerdeFactory;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.execution.buffer.PagesSerdeUtil.writeSerializedPages;
import static com.facebook.presto.operator.MergeOperator.REMOTE_CONNECTOR_ID;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorIsBlocked;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HttpStatus.NO_CONTENT;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpUriBuilder.uriBuilder;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMergeOperator
{
    private ScheduledExecutorService executor;
    private PagesSerdeFactory serdeFactory;
    private PageResponseProcessor responseProcessor;
    private HttpClient httpClient;
    private ExchangeClientSupplier exchangeClientSupplier;
    private AtomicInteger operatorId = new AtomicInteger();

    @BeforeClass
    public void setUp()
    {
        executor = newScheduledThreadPool(10, daemonThreadsNamed("test-merge-operator-%s"));
        serdeFactory = new TestingPagesSerdeFactory();
        responseProcessor = new PageResponseProcessor(serdeFactory);
        httpClient = new TestingHttpClient(responseProcessor, executor);
        exchangeClientSupplier = createExchangeClientFactory(httpClient, executor);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
        executor = null;
        serdeFactory = null;
        responseProcessor = null;
        httpClient.close();
        httpClient = null;
        exchangeClientSupplier = null;
    }

    @Test(invocationCount = 5000, threadPoolSize = 20)
    public void testSingleStreamSameOutputColumns()
            throws Exception
    {
        String taskId = "testSingleStreamSameOutputColumns_" + operatorId.incrementAndGet();
        URI sourceUri = createUri("host1", taskId);

        List<Type> types = ImmutableList.of(BIGINT, BIGINT);

        MergeOperator operator = createMergeOperator(types, ImmutableList.of(0, 1), ImmutableList.of(0, 1), ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST));
        assertFalse(operator.isFinished());
        assertFalse(operator.isBlocked().isDone());

        operator.addSplit(createRemoteSplit(sourceUri));
        assertFalse(operator.isFinished());
        assertFalse(operator.isBlocked().isDone());

        operator.noMoreSplits();
        assertFalse(operator.isFinished());
        assertFalse(operator.isBlocked().isDone());

        PageBuilder pageBuilder = new PageBuilder(types);
        pageBuilder.declarePosition();
        pageBuilder.getBlockBuilder(0).writeLong(1).closeEntry();
        pageBuilder.getBlockBuilder(1).writeLong(1).closeEntry();
        pageBuilder.declarePosition();
        pageBuilder.getBlockBuilder(0).writeLong(2).closeEntry();
        pageBuilder.getBlockBuilder(1).writeLong(2).closeEntry();
        Page page1 = pageBuilder.build();

        pageBuilder.reset();
        pageBuilder.declarePosition();
        pageBuilder.getBlockBuilder(0).writeLong(3).closeEntry();
        pageBuilder.getBlockBuilder(1).writeLong(3).closeEntry();
        pageBuilder.declarePosition();
        pageBuilder.getBlockBuilder(0).writeLong(4).closeEntry();
        pageBuilder.getBlockBuilder(1).writeLong(4).closeEntry();
        Page page2 = pageBuilder.build();

        assertOperatorIsBlocked(operator);
        responseProcessor.addPage(sourceUri, page1);
        OperatorAssertion.assertOperatorIsUnblocked(operator);
        assertPageEquals(types, operator.getOutput(), page1);

        assertOperatorIsBlocked(operator);
        responseProcessor.addPage(sourceUri, page2);
        OperatorAssertion.assertOperatorIsUnblocked(operator);
        assertPageEquals(types, operator.getOutput(), page2);

        assertOperatorIsBlocked(operator);
        responseProcessor.complete(sourceUri);
        OperatorAssertion.assertOperatorIsUnblocked(operator);

        assertTrue(operator.isFinished());

        operator.close();
    }

    private MergeOperator createMergeOperator(List<Type> sourceTypes, List<Integer> outputChannels, List<Integer> sortChannels, List<SortOrder> sortOrder)
    {
        MergeOperator.MergeOperatorFactory factory = new MergeOperator.MergeOperatorFactory(
                operatorId.getAndIncrement(),
                new PlanNodeId("plan_node_id" + operatorId.getAndIncrement()),
                exchangeClientSupplier,
                serdeFactory,
                new SimpleMergeSortComparator.Factory(),
                sourceTypes,
                outputChannels,
                sortChannels,
                sortOrder);
        DriverContext driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(0, true, true)
                .addDriverContext();
        return (MergeOperator) factory.createOperator(driverContext);
    }

    private static URI createUri(String host, String taskId)
    {
        return uriBuilder()
                .scheme("http")
                .host(host)
                .port(8080)
                .appendPath(format("%s/results/0", taskId))
                .build();
    }

    private static Split createRemoteSplit(URI uri)
    {
        RemoteSplit remoteSplit = new RemoteSplit(uri);
        return new Split(REMOTE_CONNECTOR_ID, new TestingTransactionHandle(UUID.randomUUID()), remoteSplit);
    }

    private static ExchangeClientFactory createExchangeClientFactory(HttpClient httpClient, ScheduledExecutorService executorService)
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        BlockEncodingSerde serde = new BlockEncodingManager(typeRegistry);
        ExchangeClientConfig config = new ExchangeClientConfig();
        return new ExchangeClientFactory(serde, config, httpClient, executorService);
    }

    private static class PageResponseProcessor
            implements TestingHttpClient.Processor
    {
        private static final Page COMPLETE_MARKER = new Page(new IntArrayBlock(0, new boolean[] {}, new int[] {}));

        private final Map<URI, Queue<Page>> pages = new HashMap<>();

        private final PagesSerdeFactory serdeFactory;

        private PageResponseProcessor(PagesSerdeFactory serdeFactory)
        {
            this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
        }

        @Override
        public synchronized Response handle(Request request)
        {
            URI uri = request.getUri();
            if (request.getMethod().equals("DELETE")) {
                cleanup(uri);
                return new TestingResponse(OK, ImmutableListMultimap.of(), new byte[] {});
            }
            String path = uri.getPath();
            List<String> pathSplits = Splitter.on("/").limit(4).omitEmptyStrings().splitToList(path);
            checkArgument(pathSplits.size() == 4, "invalid uri: %s", uri);
            String token = getLast(pathSplits);
            URI uriNoToken = uriBuilderFrom(uri).replacePath(Joiner.on("/").join(pathSplits.subList(0, 3))).build();

            boolean complete = false;
            List<Page> pages = getPages(uriNoToken);
            if (!pages.isEmpty() && getLast(pages) == COMPLETE_MARKER) {
                complete = true;
                pages = pages.subList(0, pages.size() - 1);
            }

            HttpStatus status = pages.isEmpty() ? NO_CONTENT : OK;
            byte[] serialized = serializePages(pages);
            return new TestingResponse(status, createResponseHeaders(token, complete), serialized);
        }

        private ListMultimap<String, String> createResponseHeaders(String token, boolean complete)
        {
            return ImmutableListMultimap.of(
                    PRESTO_TASK_INSTANCE_ID, "instance",
                    PRESTO_PAGE_TOKEN, token,
                    PRESTO_PAGE_NEXT_TOKEN, (Long.valueOf(token) + 1) + "",
                    PRESTO_BUFFER_COMPLETE, complete + "",
                    CONTENT_TYPE, PRESTO_PAGES);
        }

        private byte[] serializePages(List<Page> pages)
        {
            if (pages.isEmpty()) {
                return new byte[] {};
            }
            PagesSerde pagesSerde = serdeFactory.createPagesSerde();
            List<SerializedPage> serializedPages = pages.stream().map(pagesSerde::serialize).collect(toImmutableList());
            int pagesSize = serializedPages.stream().mapToInt(SerializedPage::getSizeInBytes).sum();
            ByteArrayOutputStream output = new ByteArrayOutputStream(pagesSize * 2);
            SliceOutput sliceOutput = new OutputStreamSliceOutput(output);
            writeSerializedPages(sliceOutput, serializedPages);
            // We use flush instead of close, because the underlying stream would be closed and that is not allowed.
            try {
                sliceOutput.close();
                output.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return output.toByteArray();
        }

        private List<Page> getPages(URI uri)
        {
            checkState(Thread.holdsLock(this));
            Queue<Page> queue = pages.get(uri);
            if (queue == null || queue.isEmpty()) {
                return ImmutableList.of();
            }
            ImmutableList.Builder<Page> result = ImmutableList.builder();
            while (!queue.isEmpty()) {
                result.add(queue.poll());
            }
            return result.build();
        }

        public synchronized void addPage(URI uri, Page page)
        {
            Queue<Page> queue = pages.computeIfAbsent(uri, k -> new LinkedList<>());
            queue.add(page);
        }

        public synchronized void complete(URI uri)
        {
            addPage(uri, COMPLETE_MARKER);
        }

        public synchronized void cleanup(URI uri)
        {
            pages.remove(uri);
        }
    }
}
