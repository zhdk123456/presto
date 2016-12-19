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
package com.facebook.presto.jdbc;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import org.skife.clocked.ClockedExecutorService;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

public class TestReferenceCountingLoadingCache
{
    @Test
    public void testSimpleLoad()
    {
        String zero = "zero";
        SingleKeyMockCacheLoaderDisposer<Integer, String> loader = mockCacheLoaderDisposer(0, ImmutableList.of(zero));
        TestCache<Integer, String> cache = new TestCache<>(loader, (value) -> {
        });
        assertSame(cache.acquire(0), zero);
    }

    @Test
    public void testSimpleLoadRelease()
            throws ExecutionException, InterruptedException
    {
        String zero = "zero";
        SingleKeyMockCacheLoaderDisposer<Integer, String> loaderDisposer = mockCacheLoaderDisposer(0, ImmutableList.of(zero));
        TestCache<Integer, String> cache = new TestCache<>(loaderDisposer, loaderDisposer);
        assertSame(cache.acquire(0), zero);
        cache.release(0);
        cache.advanceRetention();
        loaderDisposer.validate();
    }

    @Test
    public void testDontLoadOnReleaseNonexistentKey()
            throws ExecutionException, InterruptedException
    {
        TestCache<Integer, String> cache = new TestCache<>(
                new CacheLoader<Integer, String>()
                {
                    public String load(Integer key)
                    {
                        fail("Loader shouldn't have been called");
                        // not reached
                        return "zero";
                    }
                },
                value -> fail("Disposer *definitely* shouldn't have been called"));
        cache.release(0);
        cache.advanceRetention();
    }

    @Test
    public void testReacquireRetainedValue()
            throws ExecutionException, InterruptedException
    {
        String zero = "zero";
        SingleKeyMockCacheLoaderDisposer<Integer, String> loaderDisposer = mockCacheLoaderDisposer(0, ImmutableList.of(zero));
        TestCache<Integer, String> cache = new TestCache<>(loaderDisposer, loaderDisposer);
        assertSame(cache.acquire(0), zero);
        cache.release(0);

        cache.advance(cache.getRetentionPeriod() / 2, cache.getRetentionUnit());

        assertSame(cache.acquire(0), zero);
        cache.release(0);

        cache.advanceRetention();
        loaderDisposer.validate();
    }

    @Test
    public void testAcquireNewValueForDisposedKey()
            throws ExecutionException, InterruptedException
    {
        String first = "first";
        String second = "second";
        SingleKeyMockCacheLoaderDisposer<Integer, String> loaderDisposer = mockCacheLoaderDisposer(0, ImmutableList.of(first, second));
        TestCache<Integer, String> cache = new TestCache<>(loaderDisposer, loaderDisposer);
        assertSame(cache.acquire(0), first);
        cache.release(0);

        cache.advanceRetention();

        assertSame(cache.acquire(0), second);
        cache.release(0);

        cache.advanceRetention();

        loaderDisposer.validate();
    }

    @Test
    public void testMultiplyAcquiredValueNotReleased()
            throws ExecutionException, InterruptedException
    {
        String first = "first";
        SingleKeyMockCacheLoaderDisposer<Integer, String> loader = mockCacheLoaderDisposer(0, ImmutableList.of(first));
        TestCache<Integer, String> cache = new TestCache<>(
                loader,
                value -> fail("Multiply acquired value shouldn't have been released"));
        assertSame(cache.acquire(0), first);
        assertSame(cache.acquire(0), first);

        cache.release(0);

        cache.advanceRetention();
    }

    @Test
    public void testClose()
    {
        String first = "first";
        SingleKeyMockCacheLoaderDisposer<Integer, String> loaderDisposer = mockCacheLoaderDisposer(0, ImmutableList.of(first));
        TestCache<Integer, String> cache = new TestCache<>(loaderDisposer, loaderDisposer);
        assertSame(cache.acquire(0), first);
        cache.release(0);
        cache.close();
        loaderDisposer.validate();
    }

    @Test
    public void testCloseOutstandingObjects()
    {
        String first = "first";
        SingleKeyMockCacheLoaderDisposer<Integer, String> loader = mockCacheLoaderDisposer(0, ImmutableList.of(first));
        TestCache<Integer, String> cache = new TestCache<>(
                loader,
                value -> fail("Shouldn't have called dispose on unreleased object"));
        assertSame(cache.acquire(0), first);
        cache.close();
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = ".*closed.*")
    public void testCloseAcquire()
    {
        String first = "first";
        SingleKeyMockCacheLoaderDisposer<Integer, String> loaderDisposer = mockCacheLoaderDisposer(0, ImmutableList.of(first));
        TestCache<Integer, String> cache = new TestCache<>(loaderDisposer, loaderDisposer);
        assertSame(cache.acquire(0), first);
        cache.release(0);
        cache.close();
        loaderDisposer.validate();
        cache.acquire(0);
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = ".*closed.*")
    public void testCloseRelease()
    {
        String first = "first";
        SingleKeyMockCacheLoaderDisposer<Integer, String> loaderDisposer = mockCacheLoaderDisposer(0, ImmutableList.of(first));
        TestCache<Integer, String> cache = new TestCache<>(loaderDisposer, loaderDisposer);
        assertSame(cache.acquire(0), first);
        cache.close();
        cache.release(0);
    }

    @Test
    public void testCloseTwice()
    {
        String first = "first";
        SingleKeyMockCacheLoaderDisposer<Integer, String> loaderDisposer = mockCacheLoaderDisposer(0, ImmutableList.of(first));
        TestCache<Integer, String> cache = new TestCache<>(loaderDisposer, loaderDisposer);
        assertSame(cache.acquire(0), first);
        cache.release(0);
        cache.close();
        loaderDisposer.validate();
        cache.close();
        loaderDisposer.validate();
    }

    private static class TestCache<K, V>
    {
        private ReferenceCountingLoadingCache<K, V> cache;
        private ClockedExecutorService valueCleanupService;

        private TestCache(
                CacheLoader<K, V> loader,
                Consumer<V> disposer)
        {
            valueCleanupService = new ClockedExecutorService();
            cache = ReferenceCountingLoadingCache.<K, V>builder()
                    .withCleanupService(valueCleanupService)
                    .withRetentionTime(30, TimeUnit.SECONDS)
                    .build(loader, disposer);
        }

        public void advance(long time, TimeUnit unit)
                throws ExecutionException, InterruptedException
        {
            valueCleanupService.advance(time, unit).get();
        }

        public void advanceRetention()
                throws ExecutionException, InterruptedException
        {
            advance(getRetentionPeriod(), getRetentionUnit());
        }

        public void close()
        {
            cache.close();
        }

        public V acquire(K key)
        {
            return cache.acquire(key);
        }

        public void release(K key)
        {
            cache.release(key);
        }

        public long getRetentionPeriod()
        {
            return cache.getRetentionPeriod();
        }

        public TimeUnit getRetentionUnit()
        {
            return cache.getRetentionUnit();
        }
    }

    private static <K, V> SingleKeyMockCacheLoaderDisposer<K, V> mockCacheLoaderDisposer(K key, List<V> values)
    {
        return new SingleKeyMockCacheLoaderDisposer<>(key, values);
    }

    private static class SingleKeyMockCacheLoaderDisposer<K, V>
            extends CacheLoader<K, V>
            implements Consumer<V>
    {
        K key;
        List<V> values;
        int loadIndex = 0;
        int disposeIndex = 0;

        public SingleKeyMockCacheLoaderDisposer(K key, List<V> values)
        {
            this.key = requireNonNull(key, "key is null");
            this.values = requireNonNull(values, "values is null");
        }

        @Override
        public V load(K key)
                throws Exception
        {
            checkState(loadIndex == disposeIndex);
            checkState(key == this.key);
            return values.get(loadIndex++);
        }

        @Override
        public void accept(V v)
        {
            checkState(loadIndex == disposeIndex + 1);
            ++disposeIndex;
        }

        public void validate()
        {
            assertEquals(disposeIndex, loadIndex);
        }
    }
}
