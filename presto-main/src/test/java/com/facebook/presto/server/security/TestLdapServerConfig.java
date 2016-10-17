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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.server.security.LdapServerConfig.ServerType.OPENLDAP;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestLdapServerConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(LdapServerConfig.class)
                .setLdapUrl(null)
                .setLdapServerType(null)
                .setUserBaseDistinguishedName(null)
                .setGroupDistinguishedName(null)
                .setUserObjectClass(null)
                .setLdapCacheTtl(new Duration(1, TimeUnit.HOURS)));
    }

    @Test
    public void testExplicitConfig()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("authentication.ldap.url", "ldaps://localhost:636")
                .put("authentication.ldap.server-type", "OPENLDAP")
                .put("authentication.ldap.user-base-dn", "dc=test,dc=com")
                .put("authentication.ldap.group-dn", "cn=group,dc=test,dc=com")
                .put("authentication.ldap.user-object-class", "person")
                .put("authentication.ldap.cache-ttl", "2m")
                .build();

        LdapServerConfig expected = new LdapServerConfig()
                .setLdapUrl("ldaps://localhost:636")
                .setLdapServerType(OPENLDAP)
                .setUserBaseDistinguishedName("dc=test,dc=com")
                .setGroupDistinguishedName("cn=group,dc=test,dc=com")
                .setUserObjectClass("person")
                .setLdapCacheTtl(new Duration(2, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertValidates(new LdapServerConfig()
                .setLdapUrl("ldaps://localhost")
                .setLdapServerType(OPENLDAP));

        assertFailsValidation(new LdapServerConfig().setLdapUrl("ldap://"), "ldapUrl", "LDAP without SSL/TLS unsupported. Expected ldaps://", Pattern.class);
        assertFailsValidation(new LdapServerConfig().setLdapUrl("localhost"), "ldapUrl", "LDAP without SSL/TLS unsupported. Expected ldaps://", Pattern.class);
        assertFailsValidation(new LdapServerConfig().setLdapUrl("ldaps:/localhost"), "ldapUrl", "LDAP without SSL/TLS unsupported. Expected ldaps://", Pattern.class);

        assertFailsValidation(new LdapServerConfig(), "ldapUrl", "may not be null", NotNull.class);
        assertFailsValidation(new LdapServerConfig(), "ldapServerType", "may not be null", NotNull.class);
    }
}
