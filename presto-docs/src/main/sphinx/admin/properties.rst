=================
Presto properties
=================

This is a list and description of most important presto properties that may be used to tune Presto or alter it behavior when required.


.. _tuning-pref-general:

General properties
------------------


``distributed-joins-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Use hash distributed joins instead of broadcast joins. Distributed joins require redistributing both tables using a hash of the join key. This can be slower (sometimes substantially) than broadcast joins but allows much larger joins. Broadcast joins require that the tables on the right side of the join after filtering fit in memory on each node whereas distributed joins only need to fit in distributed memory across all nodes. This can also be specified on a per-query basis using the ``distributed_join`` session property.


``redistribute-writes``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Turning on this property causes additional rehashing of data before writing them to connector. This eliminates performance impact of data skewness when writing to disk by distributing data before write (usually I/O operation). It can be disabled when it's known that data set is not skewed in order to save time on rehashing operation. This can also be specified on a per-query basis using the ``redistribute_writes`` session property.


``resources.reserved-system-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``JVM max memory`` * ``0.4``
 * **Description:** Maximum amount of memory available to each Presto node. Reaching this limit will cause the server to drop operations. Higher value may increase Presto's stability, but may cause problems if physical server is used for other purposes. If too much memory is allocated to Presto, the operating system may terminate the process.


``sink.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``32 MB``
 * **Description:** Buffer size for IO writes while collecting pipeline results from cluster node. Increasing this value may improve the speed of IO operations, but will take memory away from other functions. Buffered data will be lost if the node crashes, so using a large value is not recommended when the environment is unstable.


.. _tuning-pref-exchange:

Exchange properties
-------------------

The Exchange service is responsible for transferring data between Presto nodes. Adjusting these properties may help to resolve inter-node communication issues or improve network utilization.

``exchange.client-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``25``
 * **Description:** Number of threads that the exchange server can spawn to handle clients. Higher value will increase concurrency but excessively high values may cause a drop in performance due to context switches and additional memory usage.


``exchange.concurrent-request-multiplier``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``3``
 * **Description:** Multiplier determining how many clients of the exchange server may be spawned relative to available buffer memory. The number of possible clients is determined by heuristic as the number of clients that can fit into available buffer space based on average buffer usage per request times this multiplier. For example with the ``exchange.max-buffer-size`` of ``32 MB`` and ``20 MB`` already used, and average bytes per request being ``2MB`` up to ``exchange.concurrent-request-multipier`` * ((``32MB`` - ``20MB``) / ``2MB``) = ``exchange.concurrent-request-multiplier`` * ``6`` may be spawned. Tuning this value adjusts the heuristic, which may increase concurrency and improve network utilization.


``exchange.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``32 MB``
 * **Description:** Size of memory block reserved for the client buffer in exchange server. Lower value may increase processing time under heavy load. Increasing this value may improve network utilization, but will reduce the amount of memory available for other activities.


``exchange.max-response-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size, at least ``1 MB``)
 * **Default value:** ``16 MB``
 * **Description:** Max size of messages sent through the exchange server. The size of message headers is included in this value, so the amount of data sent per message will be a little lower. Increasing this value may improve network utilization if the network is stable. In an unstable network environment, making this value smaller may improve stability.


.. _tuning-pref-node:

Node scheduler properties
-------------------------

``node-scheduler.max-pending-splits-per-node-per-stage``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``10``
 * **Description:** Must be smaller than ``node-scheduler.max-splits-per-node``. This property describes how many splits can be queued to each worker node. Having this value higher will allow more jobs to be queued but will cause resources to be used for that. Using a higher value is recommended if queries are submitted in large batches, (eg. running a large group of reports periodically). Increasing this value may help to avoid query drops and decrease the risk of short query starvation. Too high value may drastically increase processing wall time if node distribution of query work will be skew. This is especially important if nodes do have important differences in performance. The best value for that is enough to provide at least one split always waiting to be process but not higher.


``node-scheduler.max-splits-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``100``
 * **Description:** This property limits the number of splits that can be scheduled for each node. Increasing this value will allow the cluster to process more queries or reduce visibility of problems connected to data skew. Excessively high values may result in poor performance due to context switching and higher memory reservation for cluster metadata.


``node-scheduler.min-candidates``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``10``
 * **Description:** The minimal number of node candidates check by scheduler when looking for a node to schedule a split. Having this value to low may increase skew of work distribution between nodes. Too high value may increase latency of query and CPU load. The value should be aligned with number of nodes in cluster.


``node-scheduler.multiple-tasks-per-node-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Allow nodes to be selected multiple times by the node scheduler in a single stage. With this property set to ``false`` the ``hash_partition_count`` is capped at number of nodes in system. Having this set to ``true`` may allow better scheduling and concurrency, which would reduce the number of outliers and speed up computations. It may also improve reliability in unstable network conditions. The drawbacks are that some optimization may work less efficiently on smaller partitions. Also slight hardware efficiency drop is expected in heavy loaded system.

.. _node-scheduler-network-topology:

``node-scheduler.network-topology``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``legacy`` or ``flat``)
 * **Default value:** ``legacy``
 * **Description:** Sets the network topology to use when scheduling splits. ``legacy`` will ignore the topology when scheduling splits. ``flat`` will try to schedule splits on the host where the data is located by reserving 50% of the work queue for local splits. It is recommended to use ``flat`` for clusters where distributed storage runs on the same nodes as Presto workers.


.. _tuning-pref-optimizer:

Optimizer properties
--------------------

``optimizer.processing-optimization``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``disabled``, ``columnar`` or ``columnar_dictionary``)
 * **Default value:** ``disabled``
 * **Description:** Setting this property changes how filtering and projection operators are processed. Setting it to ``columnar`` allows Presto to use columnar processing instead of row by row. Setting ``columnar_dictionary`` adds additional dictionary to simplify columnar scan. Setting this to a value other than ``disabled`` may improve performance for data containing large rows often filtered by a simple key. This can also be specified on a per-query basis using the ``processing_optimization`` session property.

``optimizer.dictionary-aggregation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Enables optimization for aggregations on dictionaries. This can also be specified on a per-query basis using the ``dictionary_aggregation`` session property.


``optimizer.optimize-hash-generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Compute hash codes for distribution, joins, and aggregations early in the query plan allowing result to be shared between operations later in the plan. While this will increase the preprocessing time, it may allow the optimizer to drop some computations later in query processing. In most cases it will decrease overall query processing time. This can also be specified on a per-query basis using the ``optimize_hash_generation`` session property.


``optimizer.optimize-metadata-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Setting this property to ``true`` enables optimization of some aggregations by using values that are kept in metadata. This allows Presto to execute some simple queries in ``O(1)`` time. Currently this optimization applies to ``max``, ``min`` and ``approx_distinct`` of partition keys and other aggregation insensitive to the cardinality of the input (including ``DISTINCT`` aggregates). Using this may speed some queries significantly, though it may have a negative effect when used with very small data sets. Also it may cause incorrect/not accurate/invalid results in some backend db, especially in Hive when there are partition without any rows.


``optimizer.optimize-single-distinct``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Enables the single distinct optimization. This optimization will try to replace multiple DISTINCT clauses with a single GROUP BY clause. Enabling this optimization will speed up some specific SELECT queries, but analyzing all queries to check if they qualify for this optimization may be a slight overhead.


``optimizer.push-table-write-through-union``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** Parallelize writes when using UNION ALL in queries that write data. This improves the speed of writing output tables in UNION ALL queries because these writes do not require additional synchronization when collecting results. Enabling this optimization can improve UNION ALL speed when write speed is not yet saturated. However it may slow down queries in an already heavily loaded system. This can also be specified on a per-query basis using the ``push_table_write_through_union`` session property.


.. _tuning-pref-query:

Query execution properties
--------------------------


``query.execution-policy``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``all-at-once`` or ``phased``)
 * **Default value:** ``all-at-once``
 * **Description:** Setting this value to ``phased`` will allow the query scheduler to split a single query execution between different time slots. This will allow Presto to switch context more often and possibly stage the partially executed query in order to increase robustness. Average time to execute a query may slightly increase after setting this to ``phased``, but query execution time will be more consistent. This can also be specified on a per-query basis using the ``execution_policy`` session property.


``query.initial-hash-partitions``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``100``
 * **Description:** This value is used to determine how many nodes may share the same query when fixed partitioning is chosen by Presto. Manipulating this value will affect the distribution of work between nodes. A value lower then the number of Presto nodes may lower the utilization of the cluster in a low traffic environment. An excessively high value will cause multiple partitions of the same query to be assigned to a single node, or Presto may ignore the setting if ``node-scheduler.multiple-tasks-per-node-enabled`` is set to false - the value is internally capped at the number of available worker nodes in such scenario. This can also be specified on a per-query basis using the ``hash_partition_count`` session property.


``query.low-memory-killer.delay``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration, at least ``5s``)
 * **Default value:** ``5 m``
 * **Description:** Delay between a cluster running low on memory and invoking a query killer. A lower value may cause more queries to fail fast, but fewer queries to fail in an unexpected way.


``query.low-memory-killer.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** This property controls whether a query killer should be triggered when a cluster is running out of memory. The killer will drop the largest queries first so enabling this option may cause problems with executing large queries in a highly loaded cluster, but should increase stability of smaller queries.


``query.manager-executor-pool-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``5``
 * **Description:** Size of the thread pool used for garbage collecting after queries. Threads from this pool are used to free resources from canceled queries, as well as enforce memory limits, queries timeouts etc. More threads will allow for more efficient memory management, and so may help avoid out of memory exceptions in some scenarios. However, having more threads may also increase CPU usage for garbage collecting and will have an additional constant memory cost even if the threads have nothing to do.


``query.min-expire-age``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``15 m``
 * **Description:** This property describes the minimum time after which the query metadata may be removed from the server. If the value is too low, the client may not be able to receive information about query completion. The value describes minimum time, but if there is space available in the history queue the query data will be kept longer. The size of the history queue is defined by the ``query.max-history property``.


``query.max-concurrent-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** **Deprecated** Describes how many queries can be processed simultaneously in a single cluster node. In new configurations, the ``query.queue-config-file`` should be used instead.


.. _query-max-memory:

``query.max-memory``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``20 GB``
 * **Description:** Serves as the default value for the ``query_max_memory`` session property. This property also describes the strict limit of total memory that may be used to process a single query. A query is dropped if the limit is reached unless the ``resource_overcommit`` session property is set. This property helps ensure that a single query cannot use all resources in a cluster. It should be set higher than what is expected to be needed for a typical query in the system. It is important to set this to higher than the default if Presto will be running complex queries on large datasets. It is possible to decrease the query memory limit for a session by setting ``query_max_memory`` to a smaller value. Setting ``query_max_memory`` to a greater value than ``query.max-memory`` will not have any effect.


``query.max-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``JVM max memory`` * ``0.1``
 * **Description:** The purpose of that is same as of :ref:`query.max-memory<query-max-memory>` but the memory is not counted cluster-wise but node-wise instead. This should not be any lower than ``query.max-memory / number of nodes``. It may be required to increase this value if data are skewed.


``query.max-queued-queries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``5000``
 * **Description:** **Deprecated** Describes how many queries may wait in Presto coordinator queue. If the limit is reached the server will drop all new incoming queries. Setting this value high may allow to order a lot of queries at once with the cost of additional memory needed to keep informations about tasks to process. Lowering this value will decrease system capacity but will allow to utilize memory for real processing of data instead of queuing. It shouldn't be used in new configuration, the ``query.queue-config-file`` can be used instead.


``query.max-run-time``
^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``100 d``
 * **Description:** Used as default for session property ``query_max_run_time``. If the Presto works in environment where there are mostly very long queries (over 100 days) than it may be a good idea to increase this value to avoid dropping clients that didn't set their session property correctly. On the other hand in the Presto works in environment where they are only very short queries this value set to small value may be used to detect user errors in queries. It may also be decreased in poor Presto cluster configuration with mostly short queries to increase garbage collection efficiency and by that lowering memory usage in cluster.


``query.queue-config-file``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String``
 * **Default value:**
 * **Description:** The path to the queue config file. Queues are used to manage the number of concurrent queries across the system. More information on queues and how to configure them can be found in :doc:/admin/queue.


``query.remote-task.max-callback-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** This value describes the maximum size of the thread pool used to handle responses to HTTP requests for each task. Increasing this value will cause more resources to be used for handling HTTP communication itself, but may also improve response time when Presto is distributed across many hosts or there are a lot of small queries being run.


``query.remote-task.min-error-duration``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration, at least ``1s``)
 * **Default value:** ``2 m``
 * **Description:** The minimal time that HTTP worker must be unavailable before the coordinator assumes the worker crashed. A higher value may be recommended in unstable connection conditions. This value is only a bottom line so there is no guarantee that a node will be considered dead after the ``query.remote-task.min-error-duration``. In order to consider a node dead, the defined time must pass between two failed attempts of HTTP communication, with no successful communication in between.


``query.schedule-split-batch-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** The size of single data chunk expressed in split that will be processed in a single stage. Higher value may be used if system works in reliable environment and the responsiveness is less important then average answer time, it will require more memory reserve though. Decreasing this value may have a positive effect if there are lots of nodes in system and calculations are relatively heavy for each of splits.


.. _tuning-pref-task:

Tasks managment properties
--------------------------


.. _task-concurrency:

``task.concurrency``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``1``
 * **Description:** Default local concurrency for parallel operators. Serves as the default value for the ``task_concurrency`` session property. Increasing this value is strongly recommended when any of CPU, IO or memory is not saturated on a regular basis. It will allow queries to utilize as many resources as possible. Setting this value too high will cause queries to slow down. Slow down may happen even if none of the resources is saturated as there are cases in which increasing parallelism is not possible due to algorithms limitations.

``task.info-refresh-max-wait``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 * **Type:** ``String`` (duration)
 * **Default value:** ``1s``
 * **Description:** Controls staleness of task information, which is used in scheduling. Increasing this value can reduce coordinator CPU load, but may result in suboptimal split scheduling.


``task.http-response-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``100``
 * **Description:** Max number of threads that may be created to handle http responses. Threads are created on demand and they end when there is no response to be sent. That means that there is no overhead if there are only a small number of requests handled by the system, even if this value is big. On the other hand increasing this value may increase utilization of CPU in multicore environment (with the cost of memory usage). Also in systems having a lot of requests, the response time distribution may be manipulated using this property. A higher value may be used to prevent outliers from increasing average response time.


``task.http-timeout-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``3``
 * **Description:** Number of threads spawned for handling timeouts of http requests. Presto server sends update of query status whenever it is different then the one that client knows about. However in order to ensure client that connection is still alive, server sends this data after delay declared internally in HTTP headers (by default ``200 ms``). This property tells how many threads are designated to handle this delay. If the property turn out to low it's possible that the update time will increase even significantly when comparing to requested value (``200ms``). Increasing this value may solve the problem, but it generate a cost of additional memory even if threads are not used all the time. If there is no problem with updating status of query this value should not be manipulated.


``task.info-update-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``200 ms``
 * **Description:** Controls staleness of task information which is used in scheduling. Increasing this value can reduce coordinator CPU load but may result in suboptimal split scheduling.


``task.max-partial-aggregation-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``16 MB``
 * **Description:** Max size of partial aggregation result (if it is splitable). Increasing this value will decrease the fragmentation of the result which may improve query run times and CPU utilization with the cost of additional memory usage. Also a high value may cause a drop in performance in unstable cluster conditions.



``task.max-worker-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``Node CPUs`` * ``2``
 * **Description:** Sets the number of threads used by workers to process splits. Increasing this number can improve throughput if worker CPU utilization is low and all the threads are in use, but will cause increased heap space usage. Too high value may cause drop in performance due to a context switching. The number of active threads is available via the ``com.facebook.presto.execution.TaskExecutor.RunningSplits`` JMX stat.


``task.min-drivers``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``Node CPUs`` * ``4``
 * **Description:** This describes how many drivers are kept on a worker at any time. A lower value may cause better responsiveness for new tasks, but decrease CPU utilization. A higher value makes context switching faster, but uses additional memory. In general, if it is possible to assign a split to a driver, it is assigned if: there are fewer than ``3`` drivers assigned to the given task OR there are fewer drivers on the worker than ``task.min-drivers`` OR the task has been enqueued with the ``force start`` property.


``task.operator-pre-allocated-memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``16 MB``
 * **Description:** Memory preallocated for each driver in query execution. Increasing this value may cause less efficient memory usage but will fail fast in a low memory environment more frequently.


``task.writer-count``
^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``1``
 * **Description:** The number of concurrent writer threads per worker per query. Serves as the default for the session property ``task_writer_count``. Increasing this value may increase write speed, especially when a query is NOT I/O bounded and could use more CPU cores for parallel writes. However, in many cases increasing this value will visibly increase computation time while writing.



.. _tuning-pref-session:

Session properties
------------------

``processing_optimization``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``disabled``, ``columnar`` or ``columnar_dictionary``)
 * **Default value:** ``optimizer.processing-optimization`` (``false``)
 * **Description:** See :ref:`optimizer.processing-optimization<tuning-pref-optimizer>`.


``execution_policy``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (``all-at-once`` or ``phased``)
 * **Default value:** ``query.execution-policy`` (``all-at-once``)
 * **Description:** See :ref:`query.execution-policy <tuning-pref-query>`.


``hash_partition_count``
^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``query.initial-hash-partitions`` (``100``)
 * **Description:** See :ref:`query.initial-hash-partitions <tuning-pref-query>`.


``optimize_hash_generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.optimize-hash-generation`` (``true``)
 * **Description:** See :ref:`optimizer.optimize-hash-generation <tuning-pref-optimizer>`.


``plan_with_table_node_partitioning``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``true``
 * **Description:** **Experimental.** Adapt plan to use backend partitioning. When this is set, presto will try to partition data for workers such that each worker gets a chunk of data from a single backend partition. This enables workers to take advantage of the I/O distribution optimization in table partitioning. Note that this property is only used if a given projection uses all columns used for table partitioning inside connector.



``push_table_write_through_union``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``optimizer.push-table-write-through-union`` (``true``)
 * **Description:** See :ref:`optimizer.push-table-writethrough-union <tuning-pref-optimizer>`.


``query_max_memory``
^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``query.max-memory`` (``20 GB``)
 * **Description:** This property can be use to be nice to the cluster if a particular query is not as important as the usual cluster routines. Setting this value to less than the server property ``query.max-memory`` will cause Presto to drop the query in the session if it will require more then ``query_max_memory`` memory. Setting this value to higher than ``query.max-memory`` will not have any effect.



``query_max_run_time``
^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (duration)
 * **Default value:** ``query.max-run-time`` (``100 d``)
 * **Description:** If the expected query processing time is higher than ``query.max-run-time``, it is crucial to set this session property to prevent results of long running queries being dropped after ``query.max-run-time``. A session may also set this value to less than ``query.max-run-time`` in order to crosscheck for bugs in the query. Setting this value less than ``query.max-run-time`` may be particularly useful for a session with a very large number of short-running queries. It is important to set this value to much higher than the average query time to avoid problems with outliers (some queries may randomly take much longer due to cluster load and other circumstances). As the query timed out by this limit immediately returns all used resources this may be particularly useful in query management systems to force user limits.


``resource_overcommit``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Use resources that are not guaranteed to be available to a query. This property allows you to exceed the limits of memory available per query and session. It may allow resources to be used more efficiently, but may also cause non-deterministic query drops due to insufficient memory on machine. It can be particularly useful for performing more demanding queries.


``task_concurrency``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (power of 2).
 * **Default value:** ``task.concurrency`` (``1``)
 * **Description:** Default number of local parallel aggregation jobs per worker. Unlike `task.concurrency` this property must be power of two. See :ref:`task.concurrency<task-concurrency>`.


``task_writer_count``
^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``task.writer-count`` (``1``)
 * **Description:** See :ref:`task.writer-count <tuning-pref-task>`.


