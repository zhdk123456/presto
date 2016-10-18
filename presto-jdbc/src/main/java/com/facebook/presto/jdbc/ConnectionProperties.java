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
package com.facebook.presto.jdbc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static java.lang.String.format;

public class ConnectionProperties
{
    public static final ConnectionProperty USER = new User();
    public static final ConnectionProperty PASSWORD = new Password();
    public static final ConnectionProperty SSL = new Ssl();
    public static final ConnectionProperty SSL_TRUST_STORE_PATH = new SslTrustStorePath();
    public static final ConnectionProperty SSL_TRUST_STORE_PASSWORD = new SslTrustStorePassword();

    public static final Set<ConnectionProperty> allOf = ImmutableSet.of(
            USER,
            PASSWORD,
            SSL,
            SSL_TRUST_STORE_PATH,
            SSL_TRUST_STORE_PASSWORD);

    private static final Map<String, ConnectionProperty> keyLookup;

    static {
        ImmutableMap.Builder<String, ConnectionProperty> builder = ImmutableMap.builder();
        for (ConnectionProperty property : allOf) {
            builder.put(property.getKey(), property);
        }
        keyLookup = builder.build();
    }

    private ConnectionProperties()
    {
    }

    public static ConnectionProperty forKey(String propertiesKey)
    {
        return keyLookup.get(propertiesKey);
    }

    public static Map<String, String> getDefaults()
    {
        ImmutableMap.Builder<String, String> result = ImmutableMap.builder();
        for (ConnectionProperty property : allOf) {
            Optional<String> value = property.getDefault();
            if (value.isPresent()) {
                result.put(property.getKey(), value.get());
            }
        }
        return result.build();
    }

    private static class User
            extends BaseConnectionProperty<String>
    {
        private User()
        {
            super("user", required, stringConverter);
        }
    }

    private static class Password
            extends BaseConnectionProperty<String>
    {
        private Password()
        {
            super("password", notRequired, stringConverter);
        }
    }

    private static final Integer SSL_DISABLED = 0;
    private static final Integer SSL_ENABLED = 1;
    private static class Ssl
            extends BaseConnectionProperty<Integer>
    {
        private static final ImmutableSet<Integer> allowed = ImmutableSet.of(SSL_DISABLED, SSL_ENABLED);

        private Ssl()
        {
            super("SSL", Optional.of(SSL_DISABLED.toString()), notRequired, integerConverter);
        }

        @Override
        public void validate(Properties properties)
                throws SQLException
        {
            Optional<Integer> flag = getValue(properties);
            if (!flag.isPresent()) {
                return;
            }

            if (!allowed.contains(flag.get())) {
                throw new SQLException(format("The value of %s must be one of %s", getKey(), Joiner.on(", ").join(allowed)));
            }
        }
    }

    public static class SslTrustStorePath
            extends BaseConnectionProperty<String>
    {
        private SslTrustStorePath()
        {
            super("SSLTrustStorePath", dependsKeyValue(new Ssl(), SSL_ENABLED.toString()), stringConverter);
        }
    }

    public static class SslTrustStorePassword
            extends BaseConnectionProperty<String>
    {
        private SslTrustStorePassword()
        {
            super("SSLTrustStorePassword", dependsKeyValue(new Ssl(), SSL_ENABLED.toString()), stringConverter);
        }
    }
}
