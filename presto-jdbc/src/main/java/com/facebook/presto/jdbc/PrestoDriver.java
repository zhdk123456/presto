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

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.client.StatementClient;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.jetty.JettyIoPool;
import io.airlift.http.client.jetty.JettyIoPoolConfig;

import java.io.Closeable;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoDriver
        implements Driver, Closeable
{
    static final int VERSION_MAJOR = 1;
    static final int VERSION_MINOR = 0;

    static final int JDBC_VERSION_MAJOR = 4;
    static final int JDBC_VERSION_MINOR = 1;

    static final String DRIVER_NAME = "Presto JDBC Driver";
    static final String DRIVER_VERSION = VERSION_MAJOR + "." + VERSION_MINOR;

    private static final DriverPropertyInfo[] DRIVER_PROPERTY_INFOS = {};

    private static final String DRIVER_URL_START = "jdbc:presto:";

    private static final String USER_PROPERTY = "user";

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final JettyIoPool jettyIoPool;
    private final ScheduledExecutorService queryExecutorCleanupService;

    private final LoadingCache<Map<Object, Object>, RefCountedQueryExecutor> queryExecutorCache;
    private final ReadWriteLock cacheLock;

    static {
        try {
            DriverManager.registerDriver(new PrestoDriver());
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    /*
     * The relationship between the PrestoDriver, QueryExecutors, and
     * PrestoConnections is complicated:
     *
     * PrestoConnection <-many:one-> QueryExecutorImpl <-many:one-> PrestoDriver
     *                               (HTTP client)
     *
     * - A PrestoConnection represents the outside world's view of a JDBC
     * connection to a Presto server.
     * - A PrestoConnection contains a QueryExecutorImpl, which is responsible
     * for actually executing queries against the Presto server.
     * - QueryExecutorImpl contains a driver-wide pool of execution resources,
     * and an HTTP client that executes requests using the resources in that
     * pool.
     *
     * An HTTP client is expensive to create and destroy, and we would like to
     * share it across as many PrestoConnections as possible. If every
     * connection could use the same HTTP client, life would be simple.
     * Unfortunately, some things that can be specified at the PrestoConnection
     * level must be configured at the HTTP client level. The obvious case of
     * this is the SSL/TLS trust store configuraion: An HTTP client has to be
     * configured with the appropriate trust store to make SSL connections to a
     * Presto server.
     *
     * The PrestoDriver deals in QueryExecutors, which have a 1:1 relationship
     * with HTTP clients. In order to create many PrestoConnections with the
     * same QueryExecutorImpl, the PrestoDriver maintains a cache of
     * QueryExecutorImpls.
     *
     * This leaves us having to solve one of the two hard problems in computer
     * science: cache invalidation, which in this case, is closely intertwined
     * with the need to eventually close the HTTP client contained in the
     * cached QueryExecutorImpl.
     *
     * Conceptually, the flow is simple:
     * driver.connect() returns either
     * 1) A cached QueryExecutorImpl that has an HTTP client that satisfies the
     *    requested connection properties.
     * 2) A new QueryExecutorImpl with a new HTTP client if no cached
     *    QueryExecutorImpl is satisfactory.
     *
     * connection.close() calls queryExecutor.close()
     * If this is the last reference to the queryExecutor, close the HTTP
     * client and invalidate() queryExecutor from the cache.
     *
     * Now we need a reference count. And things get hairy.
     *
     * com.google.common.cache.LoadingCache is safe for concurrent access, so
     * the get() call in driver.connect() and the invalidate() call in
     * queryExecutor.close() can't get us into a state where somebody has
     * gotten() a reference that's already been invalidated().
     *
     * The problem is actually the refcount; there's no way to atomically get()
     * a QueryExecutorImpl from the cache and increment its refcount. It gets
     * worse: there's no way to know ahead of time which QueryExecutorImpl you're
     * going to get from get(), so the locking for the refcount has to be done
     * against the cache rather than individual QueryExecutorImpls.
     *
     * The solution is to protect the refcounts with a read/write lock on the
     * cache:
     * - get() operations on the cache are protected by a read lock. After the
     * get(), the refcount on the returned QueryExecutorImpl needs to be
     * incremented by calling queryExecutor.reference().
     * - close() operations on the queryExecutor take the write lock on the
     * cache to ensure that nobody is trying to simultaneously get() the same
     * queryExecutor and increase its refcount. close() decrements the
     * refcount, and if it's zero, invalidates the cache entry.
     *
     * The last piece of the puzzle is that the refcount needs to be atomically
     * incremented to handle multiple simultaneous get() operations happening
     * under the read lock.
     *
     * The locking hierarchy is as follows:
     * 1) read/write lock on the cache in PrestoDriver
     * 2) <internal locking of com.google.common.cache.LoadingCache imlementation>
     *
     * With that in mind, the possible scenarios for multiple access are as follows:
     *
     * 1) Multiple simultaneous get() operations happening, no close() operation.
     *    a) No suitable cache entry:
     *       All callers of get() obtain the read lock
     *       The cache creates one new entry under its own locking and returns
     *         it to all callers of get() in some arbitrary sequence.
     *       All callers atomically increment the refcount on the queryExecutor
     *    b) Suitable cache entry:
     *       All callers of get() obtain the read lock
     *       The cache returns the existing QueryExecutorImpl to all callers of
     *         get() in some arbitrary sequence.
     *       All callers atomically increment the refcount on the queryExecutor
     *
     * 2) Simultaneous close() operations happening, no get() operation.
     *    Each thread calling close() gets the write lock in turn and:
     *       decrements the refcount
     *       checks to see if the refcount is zero.
     *       if so, invalidates the cache entry for the queryExecutor
     *          The cache removes the queryExecutor under its own lock
     *          and releases its own lock.
     *       The RemovalListener is called, which closes the HTTP client
     *       releases the write lock
     *
     * 3) Simultaneous get() and close(), get obtains read lock first
     *    Caller of get() obtains read lock and:
     *       calls cache.get() which returns existing queryExecutor under the
     *          cache's own lock
     *       increments queryExecutor.refcount under readlock
     *       releases read lock
     *    Caller of close() obtains write lock and:
     *       decrements refcount
     *       refcount != 0
     *       releases write lock
     *
     * 4) Simultaneous close() and get(), close() obtains write lock first
     *    Caller of close() obtains write lock and:
     *       decrements the refcount
     *       checks to see if the refcount is zero
     *       if so, invalidates cache, closing the HTTP client as in 2 above.
     *       releases write lock
     *    Caller of get() obtains read lock and:
     *       calls cache.get() which returns a new queryExecutor under the
     *          cache's own lock
     *       increments queryExecutor.refcount under readlock
     *       releases read lock
     *
     * The last case of interest is the process that is using PrestoDriver
     * calling prestoDriver.close(). This invalidates all of the cached
     * QueryExecutors regardless of their refcount, and in doing so closes
     * their HTTP clients. If anybody is still trying to use a
     * PrestoConnection created by the driver, they're SOL.
     */
    public PrestoDriver()
    {
        this.jettyIoPool = new JettyIoPool("presto-jdbc", new JettyIoPoolConfig());
        this.cacheLock = new ReentrantReadWriteLock();
        this.queryExecutorCleanupService = new ScheduledThreadPoolExecutor(1);

        this.queryExecutorCache = CacheBuilder.newBuilder()
                .removalListener(new RemovalListener<Map<Object, Object>, RefCountedQueryExecutor>()
                        {
                            @Override
                            public void onRemoval(RemovalNotification<Map<Object, Object>, RefCountedQueryExecutor> notification)
                            {
                                notification.getValue().closeHttpClient();
                            }
                        })
                .build(new CacheLoader<Map<Object, Object>, RefCountedQueryExecutor>() {
                            @Override
                            public RefCountedQueryExecutor load(Map<Object, Object> clientProperties)
                            {
                                return new RefCountedQueryExecutor(
                                        QueryExecutorImpl.create(DRIVER_NAME + "/" + DRIVER_VERSION, jettyIoPool),
                                        clientProperties);
                            }
                        });
    }

    @Override
    public void close()
    {
        if (closed.compareAndSet(false, true)) {
            queryExecutorCleanupService.shutdown();
            queryExecutorCache.invalidateAll();
            jettyIoPool.close();
        }
    }

    private class RefCountedQueryExecutor implements QueryExecutor
    {
        private QueryExecutorImpl wrapped;

        private Map<Object, Object> clientProperties;

        private AtomicInteger refcount = new AtomicInteger(0);

        private RefCountedQueryExecutor(QueryExecutorImpl wrapped, Map<Object, Object> clientProperties)
        {
            this.wrapped = requireNonNull(wrapped, "wrapped is null");
            this.clientProperties = requireNonNull(clientProperties, "clientProperties is null");
        }

        @Override
        public void close()
        {
            Runnable deferredClose = () -> {
                cacheLock.writeLock().lock();
                try {
                    if (refcount.decrementAndGet() == 0) {
                        queryExecutorCache.invalidate(clientProperties);
                    }
                }
                finally {
                    cacheLock.writeLock().unlock();
                }
            };

            /*
             * Defer cleanup into the future to avoid constantly creating and
             * destroying QE's in the case where a single thread is repeatedly
             * calling connect() and close() for the same server.
             */
            queryExecutorCleanupService.schedule(deferredClose, 5, TimeUnit.MINUTES);
        }

        @Override
        public StatementClient startQuery(ClientSession session, String query)
        {
            return wrapped.startQuery(session, query);
        }

        @Override
        public ServerInfo getServerInfo(URI server)
        {
            return wrapped.getServerInfo(server);
        }

        private int reference()
        {
            return refcount.incrementAndGet();
        }

        private void closeHttpClient()
        {
            wrapped.closeHttpClient();
        }
    }

    private static Map<Object, Object> filterClientProperties(Properties connectionProperties)
    {
        /*
         * TODO: Once there are client-specifc properties, return the subset of
         * connectionProperties that is client-specific.
         */
        return ImmutableMap.copyOf(connectionProperties);
    }

    @Override
    public Connection connect(String url, Properties connectionProperties)
            throws SQLException
    {
        if (closed.get()) {
            throw new SQLException("Already closed");
        }

        if (!acceptsURL(url)) {
            return null;
        }

        String user = connectionProperties.getProperty(USER_PROPERTY);
        if (isNullOrEmpty(user)) {
            throw new SQLException(format("Username property (%s) must be set", USER_PROPERTY));
        }

        cacheLock.readLock().lock();
        try {
            RefCountedQueryExecutor queryExecutor = queryExecutorCache.getUnchecked(
                    filterClientProperties(connectionProperties));
            queryExecutor.reference();
            return new PrestoConnection(new PrestoDriverUri(url), user, queryExecutor);
        }
        finally {
            cacheLock.readLock().unlock();
        }
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        return url.startsWith(DRIVER_URL_START);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        return DRIVER_PROPERTY_INFOS;
    }

    @Override
    public int getMajorVersion()
    {
        return VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion()
    {
        return VERSION_MINOR;
    }

    @Override
    public boolean jdbcCompliant()
    {
        // TODO: pass compliance tests
        return false;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        // TODO: support java.util.Logging
        throw new SQLFeatureNotSupportedException();
    }
}
