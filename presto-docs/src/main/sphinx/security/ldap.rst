===================
LDAP Authentication
===================

Presto can be configured to enable frontend LDAP authentication over
HTTPS for clients, such as the :ref:`cli_ldap`, or the JDBC and ODBC
drivers. At present only simple LDAP authentication mechanism involving
username and password is supported. The Presto client sends a username 
and password to the coordinator and coordinator validates these
credentials using an external LDAP service.

To enable LDAP authentication for Presto, configuration changes are made on
the Presto coordinator. No changes are required to the worker configuration;
only the communication from the clients to the coordinator is authenticated.

Presto Server Configuration
---------------------------

Environment Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

.. _ldap_server:

LDAP Server
~~~~~~~~~~~

You can configure the Presto coordinator to authenticate against one
of the following types of LDAP server implementations:

    * Active Directory
    * OpenLDAP
    * Generic (Custom user bind operation and group authorization membership)

Presto requires Secure LDAP (LDAPS), so make sure you have TLS
enabled on your LDAP server.

TLS Configuration on Presto Coordinator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You need to import the LDAP server's TLS certificate to the default Java
truststore of the Presto coordinator to secure TLS connection. You can use
the following example `keytool` command to import the certificate
``ldap_server.crt``, to the truststore on the coordinator.

.. code-block:: none

    $ keytool -import -keystore <JAVA_HOME>/jre/lib/security/cacerts -trustcacerts -alias ldap_server -file ldap_server.crt

In addition to this, access to the Presto coordinator should be
through HTTPS. You can do it by creating a :ref:`server_java_keystore` on
the coordinator.

Presto Coordinator Node Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You must make the following changes to the environment prior to configuring the
Presto coordinator to use LDAP authentication and HTTPS.

 * :ref:`ldap_server`
 * :ref:`server_java_keystore`

You also need to make following changes to the Presto configuration files.

config.properties
~~~~~~~~~~~~~~~~~

LDAP authentication is configured in the coordinator node's
config.properties file. Properties that need to be added, along
with their sample values, are listed below.

.. code-block:: none

    http-server.authentication.type=LDAP
    authentication.ldap.url=ldaps://ldap-server:636
    authentication.ldap.server-type=[ACTIVE_DIRECTORY|OPENLDAP|GENERIC]

    http-server.https.enabled=true
    http-server.https.port=8443

    http-server.https.keystore.path=/etc/presto_keystore.jks
    http-server.https.keystore.key=keystore_password

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``http-server.authentication.type``                     Enable LDAP authentication for the Presto coordinator.
                                                        Must be set to ``LDAP``.
``authentication.ldap.url``                             The url to the LDAP server. The url scheme must be
                                                        ``ldaps://`` and Presto allows only Secure LDAP.
``authentication.ldap.server-type``                     The type of LDAP server implementation.
                                                        Set to ``ACTIVE_DIRECTORY`` or ``OPENLDAP`` or
                                                        ``GENERIC``.
``http-server.https.enabled``                           Enables HTTPS access for the Presto coordinator.
                                                        Should be set to ``true``. Default value is
                                                        ``false``.
``http-server.https.port``                              HTTPS server port.
``http-server.https.keystore.path``                     The location of the Java Keystore file that will be
                                                        used to secure TLS.
``http-server.https.keystore.key``                      The password for the keystore. This must match the
                                                        password you specified when creating the keystore.
======================================================= ======================================================

In addition to the above properties, you will need to add LDAP server implementation
specific properties.

For Active Directory:

.. code-block:: none

    authentication.ldap.server-type=ACTIVE_DIRECTORY
    authentication.ldap.ad-domain=corp.domain.com

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``authentication.ldap.ad-domain``                       The domain name of the Active Directory server.
                                                        This property is required only if
                                                        ``authentication.ldap.server-type`` is set to
                                                        ``ACTIVE_DIRECTORY``. Eg: ``corp.domain.com``.
======================================================= ======================================================

For OpenLDAP:

.. code-block:: none

    authentication.ldap.server-type=OPENLDAP
    authentication.ldap.user-base-dn=OU=Asia,DC=corp,DC=domain,DC=com

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``authentication.ldap.user-base-dn``                    The base LDAP distinguished name for the user who
                                                        who tries to connect to the server. This property is
                                                        required if ``authentication.ldap.server-type`` is set
                                                        to ``OPENLDAP`` or when specifying LDAP group
                                                        membership properties.
======================================================= ======================================================

For Generic:

The ``GENERIC`` type can be used for more general cases where the LDAP server implementation types are
different from ``ACTIVE_DIRECTORY`` (where the user bind string is ``username@<authentication.ldap-ad-domain>``)
or ``OPENLDAP`` (where user bind string is ``uid=username,<authentication.ldap-user-base-dn>``)

.. code-block:: none

    authentication.ldap.server-type=GENERIC
    authentication.ldap.generic.user-bind-pattern=user=${USER},ou=org,dc=test,dc=com

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``authentication.ldap.generic.user-bind-pattern``       This property can be used when you need to specify a
                                                        custom user-defined string for the LDAP user bind
                                                        operation. This property must contain a pattern
                                                        ``${USER}`` which will be replaced by the
                                                        actual username during the password authentication.
======================================================= ======================================================

Authorization based on LDAP Group Membership
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can further restrict the set of users allowed to connect to the Presto
coordinator based on their group membership.

In addition to the basic LDAP authentication properties, you need group-membership specific
properties in ``config.properties``. ``authentication.ldap.user-base-dn`` must be set
for the LDAP group authorization irrespective of the server type.

.. code-block:: none

    authentication.ldap.user-base-dn=OU=America,DC=corp,DC=domain,DC=com

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``authentication.ldap.user-base-dn``                    The base LDAP distinguished name for the user who
                                                        who tries to connect to the server.
======================================================= ======================================================

For Active Directory and OpenLDAP, the group authorization check is enabled if you set
``authentication.ldap.group-dn`` and ``authentication.ldap.user-object-class`` properties.
Only users belonging to the group with distinguished name ``authentication.ldap.group-dn``
are authenticated successfully. Properties that need to be added, along
with their sample values, are listed below.

For Active Directory:

.. code-block:: none

    authentication.ldap.group-dn=CN=AuthorizedGroup,OU=America,DC=corp,DC=domain,DC=com
    authentication.ldap.user-object-class=person

For OpenLDAP:

.. code-block:: none

    authentication.ldap.group-dn=CN=AuthorizedGroup,OU=America,DC=corp,DC=domain,DC=com
    authentication.ldap.user-object-class=inetOrgPerson

For OpenLDAP, for this feature to work, make sure you enable the
``memberOf`` `overlay <http://www.openldap.org/doc/admin24/overlays.html>`_.

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``authentication.ldap.group-dn``                        The entire LDAP :abbr:`dn (distinguished name)` for
                                                        the group which must be authorized to access Presto.
                                                        The user trying to connect to the server with base
                                                        :abbr:`dn (distinguished name)`
                                                        ``authentication.ldap.user-base-dn`` must belong
                                                        to this group for successful authentication.
``authentication.ldap.user-object-class``               The LDAP objectClass the user implements in LDAP.
                                                        Eg: ``person``.
======================================================= ======================================================

For Generic:

This feature is enabled if you set ``authentication.ldap.generic.group-auth-pattern``.

This property can be used for general cases where the group membership search
query is different from ``(&(objectClass=<authentication.ldap.user-object-class>)
(uid=username)(memberof=<authentication.ldap.group-dn>))`` (OpenLDAP) or
``(&(objectClass=<authentication.ldap.user-object-class>)(sAMAccountName=username)
(memberof=<authentication.ldap.group-dn>))`` (Active Directory).
You can also use this property for scenarios where you want to authorize an user
based on complex group authorization search queries. For eg: if you want to authorize
a user belonging to any one of multiple groups, then this property can be set as:

.. code-block:: none

    authentication.ldap.generic.group-auth-pattern=(&(|(memberOf=CN=normal_group,DC=corp,DC=com)(memberOf=CN=another_group,DC=corp,DC=com))(sAMAccountName=${USER}))

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``authentication.ldap.generic.group-auth-pattern``      This property can be used when you need to specify a
                                                        custom user-defined string for the LDAP group
                                                        membership authorization. This property must contain
                                                        a pattern ``${USER}`` which will be replaced by the
                                                        actual username in the group authorization search
                                                        query.
======================================================= ======================================================

.. _cli_ldap:

Presto CLI
----------

Environment Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

TLS Configuration
~~~~~~~~~~~~~~~~~

Access to the Presto coordinator should be through HTTPS when using LDAP
authentication. The Presto CLI can use either a :ref:`Java Keystore
<server_java_keystore>` file or :ref:`Java Truststore <cli_java_truststore>`
for its TLS configuration. 

If you are using keystore file, it can be copied to the client machine and used
for its TLS configuration. If you are using truststore, you can either use
default java truststores or create a custom truststore on the CLI. We do not
recommend using self-signed certificates in production.

Presto CLI Execution
^^^^^^^^^^^^^^^^^^^^

In addition to the options that are required when connecting to a Presto
coordinator that does not require LDAP authentication, invoking the CLI
with LDAP support enabled requires a number of additional command line
options. You can either use ``--keystore-*`` or ``--truststore-*`` properties
to secure TLS connection. The simplest way to invoke the CLI is with a
wrapper script.

.. code-block:: none

    #!/bin/bash

    ./presto \
    --server https://presto-coordinator.example.com:8443 \
    --keystore-path /tmp/presto.jks \
    --keystore-password password \
    --truststore-path /tmp/presto_truststore.jks
    --truststore-password password
    --catalog <catalog> \
    --schema <schema> \
    --user <LDAP user>
    --password

=============================== =========================================================================
Option                          Description
=============================== =========================================================================
``--server``                    The address and port of the Presto coordinator.  The port must
                                be set to the port the Presto coordinator is listening for HTTPS
                                connections on. Presto CLI does not support using `http` scheme for
                                the url when using LDAP authentication.
``--keystore-path``             The location of the Java Keystore file that will be used
                                to secure TLS.
``--keystore-password``         The password for the keystore. This must match the
                                password you specified when creating the keystore.
``--truststore-path``           The location of the Java Truststore file that will be used
                                to secure TLS.
``--truststore-password``       The password for the truststore. This must match the
                                password you specified when creating the truststore.
``--user``                      The LDAP username. For Active Directory this should be your
                                ``sAMAccountName`` and for OpenLDAP this should be the ``uid`` of
                                the user. For Generic type, this is the username which will be
                                used to replace the ``${USER}`` placeholder pattern in the properties
                                specified in ``config.properties``.
``--password``                  Prompts for a password for the ``user``.
=============================== =========================================================================

Troubleshooting
---------------

Java Keystore File Verification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Verify the password for a keystore file and view its contents using
:ref:`troubleshooting_keystore`.

SSL Debugging for Presto CLI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you encounter any SSL related errors when running Presto CLI, you can run CLI using ``-Djavax.net.debug=ssl``
parameter for debugging. You should use the Presto CLI executable jar to enable this. Eg:

.. code-block:: none

    java -Djavax.net.debug=ssl \
    -jar \
    presto-cli-<version>-executable.jar \
    --server https://coordinator:8443 \
    <other_cli_arguments>

Common SSL errors
~~~~~~~~~~~~~~~~~

java.security.cert.CertificateException: No subject alternative names present
*****************************************************************************

This error is seen when the Presto coordinator’s certificate is invalid and does not have the IP you provide
in the ``--server`` argument of the CLI. You will have to regenerate the coordinator's SSL certificate 
with the appropriate :abbr:`SAN (Subject Alternative Name)` added.

Adding a SAN to this certificate is required in cases where ``https://`` uses IP address in the URL rather
than the domain contained in the coordinator's certificate, and the certificate does not contain the
:abbr:`SAN (Subject Alternative Name)` parameter with the matching IP address as an alternative attribute.
