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
package com.facebook.presto.server;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.BasicAuthenticationFilter;
import io.airlift.http.client.HttpClientDefaultsBinder;

import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.HttpClientDefaultsBinder.httpClientDefaultsBinder;
import static java.nio.file.Files.isReadable;

public class InternalHttpClientConfigurationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        InternalHttpClientConfiguration configuration = buildConfigObject(InternalHttpClientConfiguration.class);
        HttpClientDefaultsBinder httpClientDefaultsBinder = httpClientDefaultsBinder(binder);
        httpClientDefaultsBinder.bindConfig(configDefaults -> {
            String keyStorePath = configuration.getKeyStorePath();
            if (keyStorePath != null) {
                checkState(isReadable(Paths.get(keyStorePath)), "Keystore is not readable: %s", keyStorePath);
                configDefaults.setKeyStorePath(keyStorePath);
            }
            configDefaults.setKeyStorePassword(configuration.getKeyStorePassword());
        });

        String ldapUser = configuration.getLdapUser();
        if (ldapUser != null) {
            String ldapPassword = configuration.getLdapPassword();
            checkArgument(ldapPassword != null, "ldap password must be set");
            httpClientDefaultsBinder.addFilterBinding().toInstance(new BasicAuthenticationFilter(ldapUser, ldapPassword));
        }
    }
}
