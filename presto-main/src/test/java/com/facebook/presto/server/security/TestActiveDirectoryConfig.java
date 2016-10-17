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

import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.server.security.LdapServerConfig.ServerType.ACTIVE_DIRECTORY;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestActiveDirectoryConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ActiveDirectoryConfig.class)
                .setActiveDirectoryDomain(null)
                .setLdapUrl(null)
                .setLdapServerType(null)
                .setUserBaseDistinguishedName(null)
                .setGroupDistinguishedName(null)
                .setUserObjectClass(null)
                .setLdapCacheTtl(new Duration(1, TimeUnit.HOURS)));
    }

    @Test
    public void testValidation()
    {
        assertValidates(new ActiveDirectoryConfig()
                .setActiveDirectoryDomain("dc=test,dc=com")
                .setLdapUrl("ldaps://localhost")
                .setLdapServerType(ACTIVE_DIRECTORY));
        assertFailsValidation(new ActiveDirectoryConfig(), "activeDirectoryDomain", "may not be null", NotNull.class);
    }
}
