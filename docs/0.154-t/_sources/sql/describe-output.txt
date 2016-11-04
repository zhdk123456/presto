===============
DESCRIBE OUTPUT
===============

Synopsis
--------

.. code-block:: none

    DESCRIBE OUTPUT statement_name

Description
-----------

Describes the output columns of a prepared statement.  It returns a table
with metadata about each output column. Each row in the output provides the
following information: column name (or the column's alias if it is aliased),
table, schema, connector, type, type size in bytes, boolean indicating whether
the column is aliased and boolean indicating whether the query is a row count
query (DDL/DML query that returns no data).

Examples
--------

Prepare and describe a query with four output columns:

.. code-block:: sql

    PREPARE my_select1 FROM
    SELECT * FROM nation

.. code-block:: sql

   DESCRIBE OUTPUT my_select1;

.. code-block:: none

         Column Name | Table  | Schema | Connector |  Type   | Type Size | Aliased | Row Count Query
        -------------+--------+--------+-----------+---------+-----------+---------+-----------------
         nationkey   | nation | sf1    | tpch      | bigint  |         8 | false   | false
         name        | nation | sf1    | tpch      | varchar |         0 | false   | false
         regionkey   | nation | sf1    | tpch      | bigint  |         8 | false   | false
         comment     | nation | sf1    | tpch      | varchar |         0 | false   | false
        (4 rows)

Prepare and describe a query whose output columns are expressions:

.. code-block:: sql

   PREPARE my_select2 FROM
   SELECT count(*) as my_count, 1+2 FROM nation

.. code-block:: sql

    DESCRIBE OUTPUT my_select2;

.. code-block:: none

         Column Name | Table | Schema | Connector |  Type  | Type Size | Aliased | Row Count Query
        -------------+-------+--------+-----------+--------+-----------+---------+-----------------
         my_count    |       |        |           | bigint |         8 | true    | false
         _col1       |       |        |           | bigint |         8 | false   | false
        (2 rows)

Prepare and describe a row count query:

.. code-block:: sql

    PREPARE my_create FROM
    CREATE TABLE foo AS SELECT * FROM nation

.. code-block:: sql

    DESCRIBE OUTPUT my_create;

.. code-block:: none

     Column Name | Table | Schema | Connector | Type | Type Size | Aliased | Row Count Query
    -------------+-------+--------+-----------+------+-----------+---------+-----------------
     NULL        | NULL  | NULL   | NULL      | NULL | NULL      | NULL    | true
    (1 row)

See Also
--------

:doc:`prepare`
