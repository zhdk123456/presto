============
ODBC Drivers
============

Presto can be accessed from using one of the Teradata ODBC drivers. The ODBC
driver is available for Windows, Mac, and Linux. The drivers are available for
free download from Teradata.

The drivers and detailed driver documentation can be downloaded from https://www.teradata.com/presto.

ODBC for Mac
************

*Requires*

* OS X 10.9 or 10.10
* iODBC 3.52.7 or later

----

ODBC for Windows (32 & 64 bit)
******************************

*Requires*

* Microsoft ODBC Driver Manager

----

ODBC for Linux (32 & 64 bit)
****************************

*Requires*

* RedHat Enterprise Linux 6.x or or CentOS equivalent
* iODBC 3.52.7 or later, unixODBC 2.3.0 or later


Tableau Customizations
**********************

**Windows Only**
The Teradata Presto ODBC driver is distributed with a Tableau Datasource Connection (TDC) file. The file is used to better the use of Presto and Tableau by customizing the SQL that Tableau sends to Presto via the driver. The TDC will not correct functionality, it will only inform Tableau of the best way to work with the Teradata Presto ODBC driver. The TDC file is included in the Presto Client Package download.

After installing the ODBC driver on Windows, you should copy the TDC file to the location Tableau will look for it:
``C:\Users\<USER_NAME>\Documents\My Tableau Repository\Datasources``

**Tableau 10**
Tableau 10 provides a named connector for Presto. For users on Tableau 10, the TDC file is not needed.
