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

import com.google.common.base.Throwables;
import io.airlift.log.Logger;

import javax.inject.Inject;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ActiveDirectoryBinder
        implements LdapBinder
{
    private static final Logger LOG = Logger.get(ActiveDirectoryBinder.class);
    private final String activeDirectoryDomain;
    private final String userObjectClass;
    private final Optional<String> baseDistinguishedName;

    @Inject
    public ActiveDirectoryBinder(LdapServerConfig config)
    {
        activeDirectoryDomain = requireNonNull(config.getActiveDirectoryDomain(), "activeDirectoryDomain is null");
        userObjectClass = requireNonNull(config.getUserObjectClass(), "userObjectClass is null");
        baseDistinguishedName = Optional.ofNullable(config.getBaseDistinguishedName());
    }

    @Override
    public String getBindDistinguishedName(String user)
    {
        return format("%s@%s", user, activeDirectoryDomain);
    }

    @Override
    public boolean checkForGroupMembership(String user, String groupDistinguishedName, DirContext context)
    {
        checkState(baseDistinguishedName.isPresent(), "Base distinguished name (DN) for user %s is missing", user);

        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        String searchBase = baseDistinguishedName.get();
        try {
            String searchFilter = format("(&(objectClass=%s)(sAMAccountName=%s)(memberof=%s))", userObjectClass, user, groupDistinguishedName);

            LOG.debug("Group membership check for user '%s' using query: %s and base distinguished name: %s", user, searchFilter, searchBase);
            NamingEnumeration<SearchResult> results = context.search(searchBase, searchFilter, searchControls);

            if (results.hasMoreElements()) {
                return true;
            }
        }
        catch (NamingException e) {
            throw Throwables.propagate(e);
        }
        return false;
    }
}
