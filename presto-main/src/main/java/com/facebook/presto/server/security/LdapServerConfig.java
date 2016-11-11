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
package com.facebook.presto.server.security;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.util.concurrent.TimeUnit;

public class LdapServerConfig
{
    private String ldapUrl;
    private ServerType ldapServerType;
    private String userBaseDistinguishedName;
    private String groupDistinguishedName;
    private String userObjectClass;
    private Duration ldapCacheTtl = new Duration(1, TimeUnit.MINUTES);

    public enum ServerType{
        ACTIVE_DIRECTORY,
        GENERIC,
        OPENLDAP
    }

    @NotNull
    @Pattern(regexp = "^ldaps://.*", message = "LDAP without SSL/TLS unsupported. Expected ldaps://")
    public String getLdapUrl()
    {
        return ldapUrl;
    }

    @Config("authentication.ldap.url")
    @ConfigDescription("URL of the ldap server")
    public LdapServerConfig setLdapUrl(String url)
    {
        this.ldapUrl = url;
        return this;
    }

    @NotNull
    public ServerType getLdapServerType()
    {
        return ldapServerType;
    }

    @Config("authentication.ldap.server-type")
    @ConfigDescription("LDAP server implementation type (supported types: ACTIVE_DIRECTORY, GENERIC, OPENLDAP)")
    public LdapServerConfig setLdapServerType(ServerType ldapServerType)
    {
        this.ldapServerType = ldapServerType;
        return this;
    }

    public String getUserBaseDistinguishedName()
    {
        return userBaseDistinguishedName;
    }

    @Config("authentication.ldap.user-base-dn")
    @ConfigDescription("Base distinguished name of the user")
    public LdapServerConfig setUserBaseDistinguishedName(String userBaseDistinguishedName)
    {
        this.userBaseDistinguishedName = userBaseDistinguishedName;
        return this;
    }

    public String getGroupDistinguishedName()
    {
        return groupDistinguishedName;
    }

    @Config("authentication.ldap.group-dn")
    @ConfigDescription("Entire distinguished name of the group")
    public LdapServerConfig setGroupDistinguishedName(String groupDistinguishedName)
    {
        this.groupDistinguishedName = groupDistinguishedName;
        return this;
    }

    public String getUserObjectClass()
    {
        return userObjectClass;
    }

    @Config("authentication.ldap.user-object-class")
    @ConfigDescription("LDAP object class the user implements")
    public LdapServerConfig setUserObjectClass(String userObjectClass)
    {
        this.userObjectClass = userObjectClass;
        return this;
    }

    @NotNull
    public Duration getLdapCacheTtl()
    {
        return ldapCacheTtl;
    }

    @Config("authentication.ldap.cache-ttl")
    public LdapServerConfig setLdapCacheTtl(Duration ldapCacheTtl)
    {
        this.ldapCacheTtl = ldapCacheTtl;
        return this;
    }
}
