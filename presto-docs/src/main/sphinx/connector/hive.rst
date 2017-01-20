==============
Hive Connector
==============

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

The Hive connector allows querying data stored in a Hive
data warehouse. Hive is a combination of three components:

* Data files in varying formats that are typically stored in the
  Hadoop Distributed File System (HDFS) or in Amazon S3.
* Metadata about how the data files are mapped to schemas and tables.
  This metadata is stored in a database such as MySQL and is accessed
  via the Hive metastore service.
* A query language called HiveQL. This query language is executed
  on a distributed computing framework such as MapReduce or Tez.

Presto only uses the first two components: the data and the metadata.
It does not use HiveQL or any part of Hive's execution environment.

Supported File Types
--------------------

The following file types are supported for the Hive connector:

* ORC
* Parquet
* RCFile
* SequenceFile
* Text

Configuration
-------------

Presto includes Hive connectors for multiple versions of Hadoop:

* ``hive-hadoop1``: Apache Hadoop 1.x
* ``hive-hadoop2``: Apache Hadoop 2.x
* ``hive-cdh4``: Cloudera CDH 4
* ``hive-cdh5``: Cloudera CDH 5

Create ``etc/catalog/hive.properties`` with the following contents
to mount the ``hive-cdh4`` connector as the ``hive`` catalog,
replacing ``hive-cdh4`` with the proper connector for your version
of Hadoop and ``example.net:9083`` with the correct host and port
for your Hive metastore Thrift service:

.. code-block:: none

    connector.name=hive-cdh4
    hive.metastore.uri=thrift://example.net:9083

Multiple Hive Clusters
^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Hive clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

HDFS Configuration
^^^^^^^^^^^^^^^^^^

For basic setups, Presto configures the HDFS client automatically and
does not require any configuration files. In some cases, such as when using
federated HDFS or NameNode high availability, it is necessary to specify
additional HDFS client options in order to access your HDFS cluster. To do so,
add the ``hive.config.resources`` property to reference your HDFS config files:

.. code-block:: none

    hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml

Only specify additional configuration files if necessary for your setup.
We also recommend reducing the configuration files to have the minimum
set of required properties, as additional properties may cause problems.

The configuration files must exist on all Presto nodes. If you are
referencing existing Hadoop config files, make sure to copy them to
any Presto nodes that are not running Hadoop.

HDFS Username
^^^^^^^^^^^^^

When not using Kerberos with HDFS, Presto will access HDFS using the
OS user of the Presto process. For example, if Presto is running as
``nobody``, it will access HDFS as ``nobody``. You can override this
username by setting the ``HADOOP_USER_NAME`` system property in the
Presto :ref:`presto_jvm_config`, replacing ``hdfs_user`` with the
appropriate username:

.. code-block:: none

    -DHADOOP_USER_NAME=hdfs_user

Accessing Hadoop clusters protected with Kerberos authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Kerberos authentication is currently supported for both HDFS and the Hive
metastore.

However there are still a few limitations:

* Kerberos authentication is only supported for the ``hive-hadoop2`` and
  ``hive-cdh5`` connectors.
* Kerberos authentication by ticket cache is not yet supported.

The properties that apply to Hive connector security are listed in the
`Hive Configuration Properties`_ table. Please see the
:doc:`/connector/hive-security` section for a more detailed discussion of the
security options in the Hive connector.

Hive Configuration Properties
-----------------------------

================================================== ============================================================ ==========
Property Name                                      Description                                                  Default
================================================== ============================================================ ==========
``hive.metastore.uri``                             The URI(s) of the Hive metastore to connect to using the
                                                   Thrift protocol. If multiple URIs are provided, the first
                                                   URI is used by default and the rest of the URIs are
                                                   fallback metastores. This property is required.
                                                   Example: ``thrift://192.0.2.3:9083`` or
                                                   ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

``hive.config.resources``                          An optional comma-separated list of HDFS
                                                   configuration files. These files must exist on the
                                                   machines running Presto. Only specify this if
                                                   absolutely necessary to access HDFS.
                                                   Example: ``/etc/hdfs-site.xml``

``hive.storage-format``                            The default file format used when creating new tables.       ``RCBINARY``

``hive.compression-codec``                         The compression codec to use when writing files.             ``GZIP``

``hive.force-local-scheduling``                    See :ref:`tuning section<force-local-scheduling>`            ``false``

``hive.respect-table-format``                      Should new partitions be written using the existing table    ``true``
                                                   format or the default Presto format?

``hive.immutable-partitions``                      Can new data be inserted into existing partitions?           ``false``

``hive.max-partitions-per-writers``                Maximum number of partitions per writer.                     100

``hive.metastore.authentication.type``             Hive metastore authentication type.                          ``NONE``
                                                   Possible values are ``NONE`` or ``KERBEROS``.

``hive.metastore.service.principal``               The Kerberos principal of the Hive metastore service.

``hive.metastore.client.principal``                The Kerberos principal that Presto will use when connecting
                                                   to the Hive metastore service.

``hive.metastore.client.keytab``                   Hive metastore client keytab location.

``hive.hdfs.authentication.type``                  HDFS authentication type.                                    ``NONE``
                                                   Possible values are ``NONE`` or ``KERBEROS``.

``hive.hdfs.impersonation.enabled``                Enable HDFS end user impersonation.                          ``false``

``hive.hdfs.presto.principal``                     The Kerberos principal that Presto will use when connecting
                                                   to HDFS.

``hive.hdfs.presto.keytab``                        HDFS client keytab location.

``hive.security``                                  See :doc:`hive-security`.

``security.config-file``                           Path of config file to use when ``hive.security=file``.
                                                   See :ref:`hive-file-based-authorization` for details.
================================================== ============================================================ ==========

Amazon S3 Configuration
-----------------------

The Hive Connector can read and write tables that are stored in S3.
This is accomplished by having a table or database location that
uses an S3 prefix rather than an HDFS prefix.

Presto uses its own S3 filesystem for the URI prefixes
``s3://``, ``s3n://`` and  ``s3a://``. It also uses the ``s3bfs://``
prefix for the legacy S3 block file system (not supported for
``hive-hadoop2`` or ``hive-cdh5``).

S3 Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^

============================================ =================================================================
Property Name                                Description
============================================ =================================================================
``hive.s3.use-instance-credentials``         Use the EC2 metadata service to retrieve API credentials
                                             (defaults to ``true``). This works with IAM roles in EC2.

``hive.s3.aws-access-key``                   Default AWS access key to use.

``hive.s3.aws-secret-key``                   Default AWS secret key to use.

``hive.s3.endpoint``                         The S3 storage endpoint server. This can be used to
                                             connect to an S3-compatible storage system instead
                                             of AWS.

``hive.s3.signer-type``                      Specify a different signer type for S3-compatible storage.
                                             Example: ``S3SignerType`` for v2 signer type

``hive.s3.staging-directory``                Local staging directory for data written to S3.
                                             This defaults to the Java temporary directory specified
                                             by the JVM system property ``java.io.tmpdir``.

``hive.s3.pin-client-to-current-region``     Pin S3 requests to the same region as the EC2
                                             instance where Presto is running (defaults to ``false``).

``hive.s3.ssl.enabled``                      Use HTTPS to communicate with the S3 API (defaults to ``true``).

``hive.s3.sse.enabled``                      Use S3 server-side encryption (defaults to ``false``).

``hive.s3.kms-key-id``                       If set, use S3 client-side encryption and use the AWS
                                             KMS to store encryption keys and use the value of
                                             this property as the KMS Key ID for newly created
                                             objects.

``hive.s3.encryption-materials-provider``    If set, use S3 client-side encryption and use the
                                             value of this property as the fully qualified name of
                                             a Java class which implements the AWS SDK's
                                             ``EncryptionMaterialsProvider`` interface.   If the
                                             class also implements ``Configurable`` from the Hadoop
                                             API, the Hadoop configuration will be passed in after
                                             the object has been created.
============================================ =================================================================

S3 Credentials
^^^^^^^^^^^^^^

If you are running Presto on Amazon EC2 using EMR or another facility,
it is highly recommended that you set ``hive.s3.use-instance-credentials``
to ``true`` and use IAM Roles for EC2 to govern access to S3. If this is
the case, your EC2 instances will need to be assigned an IAM Role which
grants appropriate access to the data stored in the S3 bucket(s) you wish
to use.  This is much cleaner than setting AWS access and secret keys in
the ``hive.s3.aws-access-key`` and ``hive.s3.aws-secret-key`` settings, and also
allows EC2 to automatically rotate credentials on a regular basis without
any additional work on your part.

Custom S3 Credentials Provider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can configure a custom S3 credentials provider by setting the Hadoop
configuration property ``presto.s3.credentials-provider`` to be the
fully qualified class name of a custom AWS credentials provider
implementation. This class must implement the
`AWSCredentialsProvider <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html>`_
interface and provide a two-argument constructor that takes a
``java.net.URI`` and a Hadoop ``org.apache.hadoop.conf.Configuration``
as arguments. A custom credentials provider can be used to provide
temporary credentials from STS (using ``STSSessionCredentialsProvider``),
IAM role-based credentials (using ``STSAssumeRoleSessionCredentialsProvider``),
or credentials for a specific use case (e.g., bucket/user specific credentials).
This Hadoop configuration property must be set in the Hadoop configuration
files referenced by the ``hive.config.resources`` Hive connector property.

Tuning Properties
^^^^^^^^^^^^^^^^^

The following tuning properties affect the behavior of the client
used by the Presto S3 filesystem when communicating with S3.
Most of these parameters affect settings on the ``ClientConfiguration``
object associated with the ``AmazonS3Client``.

===================================== =========================================================== ===============
Property Name                         Description                                                 Default
===================================== =========================================================== ===============
``hive.s3.max-error-retries``         Maximum number of error retries, set on the S3 client.      ``10``

``hive.s3.max-client-retries``        Maximum number of read attempts to retry.                   ``3``

``hive.s3.max-backoff-time``          Use exponential backoff starting at 1 second up to          ``10 minutes``
                                      this maximum value when communicating with S3.

``hive.s3.max-retry-time``            Maximum time to retry communicating with S3.                ``10 minutes``

``hive.s3.connect-timeout``           TCP connect timeout.                                        ``5 seconds``

``hive.s3.socket-timeout``            TCP socket read timeout.                                    ``5 seconds``

``hive.s3.max-connections``           Maximum number of simultaneous open connections to S3.      ``500``

``hive.s3.multipart.min-file-size``   Minimum file size before multi-part upload to S3 is used.   ``16 MB``

``hive.s3.multipart.min-part-size``   Minimum multi-part upload part size.                        ``5 MB``
===================================== =========================================================== ===============

S3 Data Encryption
^^^^^^^^^^^^^^^^^^

Presto supports reading and writing encrypted data in S3 using both
server-side encryption with S3 managed keys and client-side encryption using
either the Amazon KMS or a software plugin to manage AES encryption keys.

With `S3 server-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html>`_,
(called *SSE-S3* in the Amazon documentation) the S3 infrastructure takes care of all encryption and decryption
work (with the exception of SSL to the client, assuming you have ``hive.s3.ssl.enabled`` set to ``true``).
S3 also manages all the encryption keys for you. To enable this, set ``hive.s3.sse.enabled`` to ``true``.

With `S3 client-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html>`_,
S3 stores encrypted data and the encryption keys are managed outside of the S3 infrastructure. Data is encrypted
and decrypted by Presto instead of in the S3 infrastructure. In this case, encryption keys can be managed
either by using the AWS KMS or your own key management system. To use the AWS KMS for key management, set
``hive.s3.kms-key-id`` to the UUID of a KMS key. Your AWS credentials or EC2 IAM role will need to be
granted permission to use the given key as well.

To use a custom encryption key management system, set ``hive.s3.encryption-materials-provider`` to the
fully qualified name of a class which implements the
`EncryptionMaterialsProvider <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/EncryptionMaterialsProvider.html>`_
interface from the AWS Java SDK. This class will have to be accessible to the Hive Connector through the
classpath and must be able to communicate with your custom key management system. If this class also implements
the ``org.apache.hadoop.conf.Configurable`` interface from the Hadoop Java API, then the Hadoop configuration
will be passed in after the object instance is created and before it is asked to provision or retrieve any
encryption keys.

Schema Evolution
----------------

Hive allows the partitions in a table to have a different schema than the
table. This occurs when the column types of a table are changed after
partitions already exist (that use the original column types). The Hive
connector supports this by allowing the same conversions as Hive:

* ``varchar`` to and from ``tinyint``, ``smallint``, ``integer`` and ``bigint``
* ``real`` to ``double``
* Widening conversions for integers, such as ``tinyint`` to ``smallint``

Any conversion failure will result in null, which is the same behavior
as Hive. For example, converting the string ``'foo'`` to a number,
or converting the string ``'1234'`` to a ``tinyint`` (which has a
maximum value of ``127``).

Examples
--------

The Hive connector supports querying and manipulating Hive tables and schemas
(databases). While some uncommon operations will need to be performed using
Hive directly, most operations can be performed using Presto.

Create a new Hive schema named ``web`` that will store tables in an
S3 bucket named ``my-bucket``::

    CREATE SCHEMA hive.web
    WITH (location = 's3://my-bucket/')

Create a new Hive table named ``page_views`` in the ``web`` schema
that is stored using the ORC file format, partitioned by date and
country, and bucketed by user into ``50`` buckets (note that Hive
requires the partition columns to be the last columns in the table)::

    CREATE TABLE hive.web.page_views (
      view_time timestamp,
      user_id bigint,
      page_url varchar,
      ds date,
      country varchar
    )
    WITH (
      format = 'ORC',
      partitioned_by = ARRAY['ds', 'country'],
      bucketed_by = ARRAY['user_id'],
      bucket_count = 50
    )

Drop a partition from the ``page_views`` table::

    DELETE FROM hive.web.page_views
    WHERE ds = DATE '2016-08-09'
      AND country = 'US'

Query the ``page_views`` table::

    SELECT * FROM hive.web.page_views

Create an external Hive table named ``request_logs`` that points at
existing data in S3::

    CREATE TABLE hive.web.request_logs (
      request_time timestamp,
      url varchar,
      ip varchar,
      user_agent varchar
    )
    WITH (
      format = 'TEXTFILE',
      external_location = 's3://my-bucket/data/logs/'
    )

Drop the external table ``request_logs``. This only drops the metadata
for the table. The referenced data directory is not deleted::

    DROP TABLE hive.web.request_logs

Drop a schema::

    DROP SCHEMA hive.web


.. _tuning-pref-hive:

Tuning
-------

The following configuration properties may have an impact on connector performance:

``hive.assume-canonical-partition-keys``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Enable optimized metastore partition fetching for non-string partition keys. Setting this property allows to filter non-string partition keys while reading them from hive, based on the assumption that they are stored in canonical (java) format. This is disabled by default as hive allows to use non-canonical format as well (eg. boolean value ``false`` may be represented as ``0``, ``false``, ``False`` and more). Used correctly this property may drastically improve read time by reducing number of partition loaded from hive. Setting this property for non-canonical data format may cause erratic behavior.


``hive.domain-compaction-threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``100``
 * **Description:** Maximum number of ranges/values allowed while reading hive data without compacting it. A higher value will cause more data fragmentation but allow the use of the row skipping feature when reading ORC data. Increasing this value may have a large impact on ``IN`` and ``OR`` clause performance in scenarios making use of row skipping.


.. _force-local-scheduling:

``hive.force-local-scheduling``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Force splits to be scheduled on the same node (ignoring normal node selection procedures) as the Hadoop DataNode process serving the split data. This is useful for installations where Presto is collocated with every DataNode and may decrease queries time significantly. The drawback may be that if some data are accessed more often, the utilization of some nodes may be low even if the whole system is heavy loaded. See also :ref:`node-scheduler.network-topology<node-scheduler-network-topology>` if less strict constrain is preferred - especially if some nodes are overloaded and other are not fully utilized.


``hive.max-initial-split-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.max-split-size`` / ``2`` (``32 MB``)
 * **Description:** This property describes the maximum size of the first ``hive.max-initial-splits`` splits created for a query. the logic behind initial splits is described in ``hive.max-initial-splits``. Lower values will increase concurrency for small queries. This property represents the maximum size, as the real size may be lower when the amount of data to read is less than ``hive.max-initial-split-size`` (e.g. at the end of a block on a DataNode).


``hive.max-initial-splits``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``200``
 * **Description:** This property describes how many splits may be initially created for a single query using ``hive.max-initial-split-size`` instead of ``hive.max-split-size``. A higher value will force more splits to have a smaller size (``hive.max-initial-splits`` is expected to be smaller than ``hive.max-split-size``), effectively increasing the definition of what is considered a "small query". The purpose of the smaller split size for the initial splits is to increase concurrency for smaller queries.


``hive.max-outstanding-splits``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** Limit on the nubmer of splits waiting to be served by a split source. After reaching this limit, writers will stop writing new splits until some of hteme are used by workers. Higher values will increase memory usage, but allow IO to be concentrated at one time, which may be faster and increase resource utilization.


``hive.max-partitions-per-writers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``100``
 * **Description:** Maximum number of partitions per writer. A query will fail if it requires more partitions per writer than allowed by this property. It can be helpful to have queries beyond the expected maximum partitions to fail to help with error detection. Also it may allow to preactivly avoid out of memory problem.


``hive.max-split-iterator-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** This property describes how many threads may be used to iterate through splits when loading them to the worker nodes. A higher value may increase parallelism, but increased concurrency may cause too much time to be spent on context switching.


``hive.max-split-size``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``64 MB``
 * **Description:** The maximum size of splits created after the initial splits. The logic for initial splits is described in ``hive.max-initial-splits``. A higher value will reduce parallelism. This may be desirable for very large queries and a stable cluster because it allows for more efficient processing of local data without the context switching, synchronization and data collection that result from parallelization. The optimal value should be aligned with the average query size in the system.


``hive.metastore.partition-batch-size.max``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``100``
 * **Description:** This together with ``hive.metastore.partition-batch-size.min`` defines the range of partition sizes read from Hive. The first partition is always of size ``hive.metastore.partition-batch-size.min`` and each following partition is two times bigger than previous up to ``hive.mestastore.partition-batch-size.max`` (the formula for partition size ``n`` is min(``hive.metastore.partition-batch-size.max``, (``2``^``n``) * ``hive.metastore.partition-batch-size.min``)). This algorithm allows for live adjustment of partition size according to the processing requirements. If the queries in the system will differ significantly from each other in size, then this range should be extended to better adjust to processing requirements. If the queries in the system will mostly be of the same size, then setting both values to the same maximally tuned value may give a slight edge in processing time.


``hive.metastore.partition-batch-size.min``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``10``
 * **Description:** See ``hive.metastore.partition-batch-size.max``.


``hive.orc.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``8 MB``
 * **Description:** Serves as default value for ``orc_max_buffer_size`` session properties defining max size of ORC read operators. Higher value will allow bigger chunks to be processed but will decrease concurrency level.


``hive.orc.max-merge-distance``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``1 MB``
 * **Description:** Serves as the default value for the ``orc_max_merge_distance`` session property. Two reads from an ORC file may be merged into a single read if the distance between the requested data ranges in the data source is less than or equal to this value.


``hive.orc.stream-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``8 MB``
 * **Description:** Serves as the default value for the ``orc_max_buffer_size`` session property. It defines the maximum size of ORC read operators. A higher value will allow bigger chunks to be processed, but will decrease concurrency.


``hive.orc.use-column-names``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Access ORC columns using names from the file. By default, Hive access columns in ORC files using the order recoded in the Hive metastore. Setting this property allows to use columns names recorded in the ORC file instead.


.. _parquet-optimized-reader:

``hive.parquet-optimized-reader.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** *Deprecated* Serves as default value for ``parquet_optimized_reader_enabled`` session property. Enables number of reader improvements introduced by alternative parquet implementation. The new reader supports vectorized reads, lazy loading, and predicate push down, all of which make the reader more efficient and typically reduces wall clock time for a query. However as the code has changed significantly it may or may not introduce some minor issues, so it can be disabled if some  problems with environment are noticed. This property enables/disables all optimizations except predicate push down as it is managed by ``hive.parquet-predicate-pushdown.enabled`` property.


``hive.parquet-predicate-pushdown.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** *Deprecated* Serves as default value for ``parquet_predicate_pushdown_enabled`` sesssion property. See :ref:`hive.parquet-optimized-reader.enabled<parquet-optimized-reader>`.


``hive.parquet.use-column-names``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Access Parquet columns using names from the file. By default, columns in Parquet files are accessed by their ordinal position in the Hive metastore. Setting this property allows access by column name recorded in the Parquet file instead.


``hive.s3.max-connections``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``500``
 * **Description:** The maximum number of connections to S3 that may be open at a time by the S3 driver. A higher value may increase network utilization when a cluster is used on a high speed network. However, a higher values relies more on S3 servers being well configured for high parallelism.


``hive.s3.multipart.min-file-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size, at least ``16 MB``)
 * **Default value:** ``16 MB``
 * **Description:** This property describes how big a file must be to be uploaded to an S3 cluster using the multipart upload feature. Amazon recommends using ``100 MB``, but a lower value may increase upload parallelism and decrease the ``data lost``/``data sent`` ratio in unstable network conditions.


``hive.s3.multipart.min-part-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size, at least ``5 MB``)
 * **Default value:** ``5 MB``
 * **Description:** Defines the minimum part size for upload parts. Decreasing the minimum part size causes multipart uploads to be split into a larger number of smaller parts. Setting this value too low has a negative effect on transfer speeds, causing extra latency and network communication for each part.


There are also following session properties allowing to control connector behavior on single query basis:

``orc_max_buffer_size``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.orc.max-buffer-size`` (``8 MB``)
 * **Description:** See :ref:`hive.orc.max-buffer-size <tuning-pref-hive>`.


``orc_max_merge_distance``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.orc.max-merge-distance`` (``1 MB``)
 * **Description:** See :ref:`hive.orc.max-merge-distance <tuning-pref-hive>`.


``orc_stream_buffer_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.orc.max-buffer-size`` (``8 MB``)
 * **Description:** See :ref:`hive.orc.max-buffer-size <tuning-pref-hive>`.


Hive Connector Limitations
--------------------------

:doc:`/sql/delete` is only supported if the ``WHERE`` clause matches entire partitions.
