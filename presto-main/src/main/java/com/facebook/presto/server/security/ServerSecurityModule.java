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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.TheServlet;

import javax.servlet.Filter;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class ServerSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(LdapServerConfig.class);
        LdapServerConfig ldapServerConfig = buildConfigObject(LdapServerConfig.class);

        configBinder(binder).bindConfig(SecurityConfig.class);
        SecurityConfig config = buildConfigObject(SecurityConfig.class);

        checkState(!(ldapServerConfig.getAuthenticationEnabled() && config.getAuthenticationEnabled()), "Enabling both Kerberos and LDAP authentication not supported");

        if (ldapServerConfig.getAuthenticationEnabled()) {
            checkState((ldapServerConfig.getBaseDistinguishedName() != null) != (ldapServerConfig.getActiveDirectoryDomain() != null),
                    "Set either authentication.ldap.base-dn or authentication.ldap.ad-domain server property");

            Multibinder.newSetBinder(binder, Filter.class, TheServlet.class)
                    .addBinding()
                    .to(LdapFilter.class)
                    .in(Scopes.SINGLETON);
            if (ldapServerConfig.getBaseDistinguishedName() != null) {
                binder.bind(LdapBinder.class).to(OpenLdapBinder.class).in(Scopes.SINGLETON);
            }
            else if (ldapServerConfig.getActiveDirectoryDomain() != null) {
                binder.bind(LdapBinder.class).to(ActiveDirectoryBinder.class).in(Scopes.SINGLETON);
            }
        }

        if (config.getAuthenticationEnabled()) {
            Multibinder.newSetBinder(binder, Filter.class, TheServlet.class)
                    .addBinding()
                    .to(SpnegoFilter.class)
                    .in(Scopes.SINGLETON);
        }
    }
}
