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
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorIsUnblocked;
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

    @Test
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

        Page page1 = createPage(types, new long[][] {
                new long[] {1, 1},
                new long[] {2, 2}});

        Page page2 = createPage(types, new long[][] {
                new long[] {3, 3},
                new long[] {4, 4}});

        assertOperatorIsBlocked(operator);
        responseProcessor.addPage(sourceUri, page1);
        assertOperatorIsUnblocked(operator);
        assertPageEquals(types, operator.getOutput(), page1);

        assertOperatorIsBlocked(operator);
        responseProcessor.addPage(sourceUri, page2);
        assertOperatorIsUnblocked(operator);
        assertPageEquals(types, operator.getOutput(), page2);

        assertOperatorIsBlocked(operator);
        responseProcessor.complete(sourceUri);
        assertOperatorIsUnblocked(operator);

        assertTrue(operator.isFinished());

        operator.close();
    }

    @Test
    public void testSingleStreamDifferentOutputColumns()
            throws Exception
    {
        String taskId = "testSingleStreamDifferentOutputColumns_" + operatorId.incrementAndGet();
        URI sourceUri = createUri("host1", taskId);

        List<Type> types = ImmutableList.of(BIGINT, BIGINT);

        MergeOperator operator = createMergeOperator(types, ImmutableList.of(0), ImmutableList.of(0, 1), ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST));
        operator.addSplit(createRemoteSplit(sourceUri));
        operator.noMoreSplits();

        Page page1 = createPage(types, new long[][] {
                new long[] {1, 2},
                new long[] {3, 4}});

        responseProcessor.addPage(sourceUri, page1);
        assertOperatorIsUnblocked(operator);
        Page expected = createPage(ImmutableList.of(BIGINT), new long[][] {
                new long[] {1},
                new long[] {3}});
        assertPageEquals(ImmutableList.of(BIGINT), operator.getOutput(), expected);

        responseProcessor.complete(sourceUri);
        assertOperatorIsUnblocked(operator);
        assertTrue(operator.isFinished());
        operator.close();
    }

    @Test(invocationCount = 5000, threadPoolSize = 20)
    public void testMultipleStreamsSameOutputColumns()
    {
        String taskId = "testMultipleStreamsSameOutputColumns_" + operatorId.incrementAndGet();

        URI source1 = createUri("host1", taskId);
        URI source2 = createUri("host2", taskId);
        URI source3 = createUri("host3", taskId);

        List<Type> types = ImmutableList.of(BIGINT, BIGINT, BIGINT);

        MergeOperator operator = createMergeOperator(types, ImmutableList.of(0, 1, 2), ImmutableList.of(0), ImmutableList.of(ASC_NULLS_FIRST));
        operator.addSplit(createRemoteSplit(source1));
        operator.addSplit(createRemoteSplit(source2));
        operator.addSplit(createRemoteSplit(source3));
        operator.noMoreSplits();

        assertFalse(operator.isFinished());
        assertFalse(operator.isBlocked().isDone());

        Page source1page1 = createPage(types, new long[][] {
                new long[] {1, 1, 2},
                new long[] {8, 1, 1},
                new long[] {19, 1, 3},
                new long[] {27, 1, 4},
                new long[] {41, 2, 5}
        });

        Page source1page2 = createPage(types, new long[][] {
                new long[] {55, 1, 2},
                new long[] {89, 1, 3},
                new long[] {101, 1, 4},
                new long[] {202, 1, 3},
                new long[] {399, 2, 2}
        });

        Page source1page3 = createPage(types, new long[][] {
                new long[] {400, 1, 1},
                new long[] {401, 1, 7},
                new long[] {402, 1, 6}
        });

        Page source2page1 = createPage(types, new long[][] {
                new long[] {2, 1, 2},
                new long[] {8, 1, 1},
                new long[] {19, 1, 3},
                new long[] {25, 1, 4},
                new long[] {26, 2, 5}
        });

        Page source2page2 = createPage(types, new long[][] {
                new long[] {56, 1, 2},
                new long[] {66, 1, 3},
                new long[] {77, 1, 4},
                new long[] {88, 1, 3},
                new long[] {99, 2, 2}
        });

        Page source2page3 = createPage(types, new long[][] {
                new long[] {99, 1, 1},
                new long[] {100, 1, 7},
                new long[] {100, 1, 6}
        });

        Page source3page1 = createPage(types, new long[][] {
                new long[] {88, 1, 3},
                new long[] {89, 1, 3},
                new long[] {90, 1, 3},
                new long[] {91, 1, 4},
                new long[] {92, 2, 5}
        });

        Page source3page2 = createPage(types, new long[][] {
                new long[] {93, 1, 2},
                new long[] {94, 1, 3},
                new long[] {95, 1, 4},
                new long[] {97, 1, 3},
                new long[] {98, 2, 2}
        });

        responseProcessor.addPage(source1, source1page1);
        assertOperatorIsBlocked(operator);
        responseProcessor.addPage(source2, source2page1);
        assertOperatorIsBlocked(operator);
        responseProcessor.addPage(source3, source3page1);
        assertOperatorIsUnblocked(operator);
        Page expected = createPage(types, new long[][] {
                new long[] {1, 1, 2},
                new long[] {2, 1, 2},
                new long[] {8, 1, 1},
                new long[] {8, 1, 1},
                new long[] {19, 1, 3},
                new long[] {19, 1, 3},
                new long[] {25, 1, 4},
                new long[] {26, 2, 5},
                });
        assertPageEquals(types, operator.getOutput(), expected);
        assertOperatorIsBlocked(operator);

        responseProcessor.addPage(source1, source1page2);
        assertOperatorIsBlocked(operator);
        responseProcessor.addPage(source2, source2page2);
        assertOperatorIsUnblocked(operator);
        expected = createPage(types, new long[][] {
                new long[] {27, 1, 4},
                new long[] {41, 2, 5},
                new long[] {55, 1, 2},
                new long[] {56, 1, 2},
                new long[] {66, 1, 3},
                new long[] {77, 1, 4},
                new long[] {88, 1, 3},
                new long[] {88, 1, 3},
                new long[] {89, 1, 3},
                new long[] {89, 1, 3},
                new long[] {90, 1, 3},
                new long[] {91, 1, 4},
                new long[] {92, 2, 5}
        });
        assertPageEquals(types, operator.getOutput(), expected);
        assertOperatorIsBlocked(operator);

        responseProcessor.addPage(source3, source3page2);
        assertOperatorIsUnblocked(operator);
        expected = createPage(types, new long[][] {
                new long[] {93, 1, 2},
                new long[] {94, 1, 3},
                new long[] {95, 1, 4},
                new long[] {97, 1, 3},
                new long[] {98, 2, 2}
        });
        assertPageEquals(types, operator.getOutput(), expected);
        assertOperatorIsBlocked(operator);

        responseProcessor.complete(source3);
        assertOperatorIsUnblocked(operator);

        expected = createPage(types, new long[][] {
                new long[] {99, 2, 2}
        });
        assertPageEquals(types, operator.getOutput(), expected);
        assertOperatorIsBlocked(operator);

        responseProcessor.addPage(source2, source2page3);
        assertOperatorIsUnblocked(operator);
        expected = createPage(types, new long[][] {
                new long[] {99, 1, 1},
                new long[] {100, 1, 7},
                new long[] {100, 1, 6}
        });
        assertPageEquals(types, operator.getOutput(), expected);
        assertOperatorIsBlocked(operator);

        responseProcessor.complete(source2);
        assertOperatorIsUnblocked(operator);
        expected = createPage(types, new long[][] {
                new long[] {101, 1, 4},
                new long[] {202, 1, 3},
                new long[] {399, 2, 2}
        });
        assertPageEquals(types, operator.getOutput(), expected);
        assertOperatorIsBlocked(operator);

        responseProcessor.addPage(source1, source1page3);
        assertOperatorIsUnblocked(operator);
        expected = createPage(types, new long[][] {
                new long[] {400, 1, 1},
                new long[] {401, 1, 7},
                new long[] {402, 1, 6}
        });
        assertPageEquals(types, operator.getOutput(), expected);
        assertOperatorIsBlocked(operator);

        responseProcessor.complete(source1);
        assertOperatorIsUnblocked(operator);
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

    private static Page createPage(List<Type> types, long[][] rows)
    {
        checkState(types.stream().allMatch(type -> type.equals(BIGINT)));
        PageBuilder pageBuilder = new PageBuilder(types);
        for (long[] row : rows) {
            checkState(row.length == types.size());
            pageBuilder.declarePosition();
            for (int columnIndex = 0; columnIndex < row.length; columnIndex++) {
                long value = row[columnIndex];
                pageBuilder.getBlockBuilder(columnIndex).writeLong(value).closeEntry();
            }
        }
        return pageBuilder.build();
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
