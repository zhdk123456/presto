==================
Teradata Additions
==================

The following are additions that Teradata has added on top Facebook's 0.148 release of Presto

Bug Fixes
---------
* Fix issue "Hive table is corrupt. It is declared as being bucketed, but the files do not match the bucketing declaration. The number of files in the directory (1) does not match the declared." by fixing support for Hive bucketed tables. See option ``hive.multi-file-bucketing.enabled`` in the Presto Hive connector documentation.
* Fix issue "low must be less than or equal to high" that can occur with ORC and Character data. 

Prepared Statements
-------------------
Add support for Prepared statements and parameters via sql syntax.

    * PREPARE
    * DEALLOCATE PREPARE
    * EXECUTE
    * DESCRIBE INPUT
    * DESCRIBE OUTPUT

Data Types
----------
Add FLOAT support to the Hive connector
Add Char support to Hive connector
Additional Varchar(x) function implementations
Additional Decimal functions implementations

Documentation
-------------
Additional Kerberos
Grant/Revoke
Presto-Admin
Presto YARN Integration
Presto Ambari Integration
TINYINT, SMALLINT, INTEGER


Performance Improvements
------------------------

Window Functions
~~~~~~~~~~~~~~~~
Windows Functions with identical specifications merged to share work

Regular Expressions
~~~~~~~~~~~~~~~~~~~

Add support for running regular expression functions using the more efficent re2j-td library by setting the session
variable ``regex_library`` to RE2J.  The memory footprint can be adjusted by setting ``re2j_dfa_states_limit``.
Additionally, the number of times the re2j library falls back from its DFA algorithm to the NFA algorithm (due to
hitting the states limit) before immediately starting with the NFA algorithm can be set with the ``re2j_dfa_retries``
session variable.

======================
Discontinue to support
======================

Implicit casts from Varchar(x) to date types are no longer supported.
