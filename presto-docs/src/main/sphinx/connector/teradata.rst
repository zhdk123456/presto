===================
Teradata  Connector
===================

Teradata provides a low latency high performing connector that
supports high concurrency, and parallel processing between Teradata
and Presto. The Teradata QueryGrid connector for Presto enables users
to execute a query within Teradata that will reach out to Presto,
execute a query against one of the data platforms Presto supports,
such as Hadoop, and then combine that result set with data within the
Teradata database platform. The QueryGrid connector can now initiate a
query from Presto to reach out to Teradata as well. The connector is
architected to be as efficient as possible, leveraging SQL pushdown,
auto data conversion, compression as well as optimized CPU usage.

There is no complex mapping or syntax that is needed within a
QueryGrid query to reach out to Presto from Teradata or vice
versa. All that is needed is an initial set up of the QueryGrid
connector for Presto on both the Teradata and Presto side. Once that
is in place, when initiating a query in Teradata that queries Presto
you simply use the syntax @presto to specify the target. For example
if querying the web logs table in Hadoop from Teradata through
Presto using the QueryGrid connector and looking for the sum of time
spent in a specific date range simply execute the following
query.

.. code-block:: none

   SELECT SUM(time_spent)
   FROM web_data.web_ logs@presto
   WHERE visit_start BETWEEN ‘2015-12-01’ AND ‘2015-12-31’

To query tables that exist in Teradata from Presto all that is needed
is to put the syntax teradata. before any table name. For example if
you want to get the average price from the sales table for the state
of Massachusetts, in Teradata, execute the following query in Presto —

.. code-block:: none
   
   SELECT AVG(price)
   FROM teradata.sales.transactions
   WHERE state=’MA’;

QueryGrid connector for Presto’s syntax is simple, and easy to use
enabling business user to quickly and interactively query between
systems.


Click `here <http://www.teradata.com/Resources/Datasheets/QueryGrid-and-Presto-Enabling-faster-more-scalable-interactive-querying-of-Hadoop/>`_ for more information about the Presto Teradata Query Grid Connector


Contact **presto@teradata.com** for more information to obtain an evaluation of the Presto Teradata Query Grid Connector.

