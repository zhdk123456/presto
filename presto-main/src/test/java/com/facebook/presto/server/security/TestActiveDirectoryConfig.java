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
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;

import java.util.Map;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestActiveDirectoryConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ActiveDirectoryConfig.class)
                .setActiveDirectoryDomain(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("authentication.ldap.ad-domain", "test.com")
                .build();

        ActiveDirectoryConfig expected = new ActiveDirectoryConfig()
                .setActiveDirectoryDomain("test.com");
        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertValidates(new ActiveDirectoryConfig()
                .setActiveDirectoryDomain("dc=test,dc=com"));
        assertFailsValidation(new ActiveDirectoryConfig(), "activeDirectoryDomain", "may not be null", NotNull.class);
    }
}
