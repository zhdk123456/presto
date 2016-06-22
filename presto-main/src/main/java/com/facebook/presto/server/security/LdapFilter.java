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

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.net.HttpHeaders;
import io.airlift.log.Logger;

import javax.inject.Inject;
import javax.naming.AuthenticationException;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.security.Principal;
import java.util.Base64;
import java.util.Hashtable;
import java.util.List;
import java.util.Objects;

import static com.facebook.presto.server.security.LdapServerConfig.ServerType.GENERIC;
import static com.google.common.base.CharMatcher.WHITESPACE;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.naming.Context.INITIAL_CONTEXT_FACTORY;
import static javax.naming.Context.PROVIDER_URL;
import static javax.naming.Context.SECURITY_AUTHENTICATION;
import static javax.naming.Context.SECURITY_CREDENTIALS;
import static javax.naming.Context.SECURITY_PRINCIPAL;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public class LdapFilter
        implements Filter
{
    private static final Logger log = Logger.get(LdapFilter.class);
    private static final String AUTHENTICATION_TYPE = "Basic";
    private static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
    private final LdapServerConfig serverConfig;
    private final GenericLdapBinder genericLdapBinder;

    @Inject
    public LdapFilter(LdapServerConfig serverConfig, GenericLdapBinder genericLdapBinder)
    {
        this.serverConfig = requireNonNull(serverConfig, "serverConfig is null");
        this.genericLdapBinder = requireNonNull(genericLdapBinder, "genericLdapBinder is null");
        try {
            authenticate(getBasicEnvironment());
        }
        catch (NamingException e) {
            throw Throwables.propagate(e);
        }
    }

    private InitialDirContext authenticate(Hashtable<String, String> environment)
            throws NamingException
    {
        return new InitialDirContext(environment);
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        // skip auth for http
        if (!servletRequest.isSecure()) {
            nextFilter.doFilter(servletRequest, servletResponse);
            return;
        }

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);

        if (header == null) {
            // request for user/password for LDAP authentication
            sendChallenge(response, SC_UNAUTHORIZED, null);
            return;
        }

        List<String> parts = Splitter.on(WHITESPACE).splitToList(header);
        if (parts.size() != 2) {
            sendChallenge(response, SC_BAD_REQUEST, "Invalid value for authorization header");
            return;
        }

        String authenticationType = parts.get(0);
        if (!authenticationType.equalsIgnoreCase(AUTHENTICATION_TYPE)) {
            sendChallenge(response, SC_BAD_REQUEST, format("Incorrect authentication type (expected %s): %s", AUTHENTICATION_TYPE, authenticationType));
            return;
        }

        String[] credentials = new String(Base64.getDecoder().decode(parts.get(1))).split(":", 2);
        if (credentials.length != 2) {
            sendChallenge(response, SC_BAD_REQUEST, "Username/password missing in the request header");
            return;
        }

        DirContext context = null;
        String user = credentials[0];

        try {
            String password = credentials[1];
            if (user.isEmpty() || password.isEmpty()) {
                throw new AuthenticationException("Username or password is empty");
            }

            Hashtable<String, String> environment = getBasicEnvironment();
            String principal = genericLdapBinder.getUserBindSearchPattern().replaceAll("\\$\\{USER\\}", user);

            environment.put(SECURITY_AUTHENTICATION, "simple");
            environment.put(SECURITY_PRINCIPAL, principal);
            environment.put(SECURITY_CREDENTIALS, password);
            context = authenticate(environment);

            log.debug("Authentication successful for user %s.", user);

            if (genericLdapBinder.getGroupAuthorizationSearchPattern().isPresent()) {
                checkForGroupMembership(genericLdapBinder.getGroupAuthorizationSearchPattern().get(), user, context);
            }

            // ldap authentication ok, continue
            nextFilter.doFilter(new HttpServletRequestWrapper(request)
            {
                @Override
                public Principal getUserPrincipal()
                {
                    return new LdapPrincipal(user);
                }
            }, servletResponse);
        }
        catch (AuthenticationException e) {
            // log the failure since the exception goes directly to the client
            log.debug("Authentication failed for user %s. Invalid credentials: %s", user, e.getMessage());
            throw new RuntimeException("Invalid credentials: " + e.getMessage());
        }
        catch (NamingException e) {
            throw Throwables.propagate(e);
        }
        finally {
            try {
                if (context != null) {
                    context.close();
                }
            }
            catch (NamingException ignored) {
            }
        }
    }

    private void checkForGroupMembership(String searchFilterPattern, String user, DirContext context)
    {
            checkState(serverConfig.getUserBaseDistinguishedName() != null, "Base distinguished name (DN) for user %s is null", user);

            try {
                String searchFilter = searchFilterPattern.replaceAll("\\$\\{USER\\}", user);
                GroupAuthorizationFilter groupFilter = new GroupAuthorizationFilter(serverConfig.getUserBaseDistinguishedName(), searchFilter);

                String groupNameIfPresent = firstNonNull(serverConfig.getLdapServerType().equals(GENERIC) ? null : serverConfig.getGroupDistinguishedName(), "");
                if (!groupFilter.authorize(context)) {
                    String errorMessage = format("User %s not a member of the group %s", user, groupNameIfPresent);
                    log.debug("Authorization failed for user. " + errorMessage);
                    throw new RuntimeException("Unauthorized user: " + errorMessage);
                }
                log.debug("Authorization succeeded for user %s in group %s", user, groupNameIfPresent);
            }
            catch (NamingException e) {
                throw Throwables.propagate(e);
            }
    }

    private Hashtable<String, String> getBasicEnvironment()
    {
        Hashtable<String, String> environment = new Hashtable<>();
        environment.put(INITIAL_CONTEXT_FACTORY, LDAP_CONTEXT_FACTORY);
        environment.put(PROVIDER_URL, serverConfig.getLdapUrl());
        return environment;
    }

    private static void sendChallenge(HttpServletResponse response, int status, String errorMessage)
            throws IOException
    {
        if (errorMessage != null) {
            response.sendError(status, errorMessage);
        }
        else {
            response.setStatus(status);
        }
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, format("%s realm=\"presto\"", AUTHENTICATION_TYPE));
    }

    @Override
    public void init(FilterConfig filterConfig)
            throws ServletException
    {
    }

    @Override
    public void destroy()
    {
    }

    private static final class LdapPrincipal
            implements Principal
    {
        private final String name;

        public LdapPrincipal(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LdapPrincipal that = (LdapPrincipal) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    private final class GroupAuthorizationFilter
    {
        private final String userBaseDistinguishedName;
        private final String searchFilter;

        public GroupAuthorizationFilter(String userBaseDistinguishedName, String searchFilter)
        {
            this.userBaseDistinguishedName = requireNonNull(userBaseDistinguishedName, "userBaseDistinguishedName is null");
            this.searchFilter = requireNonNull(searchFilter, "searchFilter is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GroupAuthorizationFilter that = (GroupAuthorizationFilter) o;
            return Objects.equals(userBaseDistinguishedName, that.userBaseDistinguishedName)
                    && Objects.equals(searchFilter, that.searchFilter);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(this.userBaseDistinguishedName, this.searchFilter);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
            .add("userBaseDistinguishedName", userBaseDistinguishedName)
            .add("searchFilter", searchFilter)
            .toString();
        }

        public boolean authorize(DirContext context) throws NamingException
        {
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            return context.search(userBaseDistinguishedName, searchFilter, searchControls).hasMoreElements();
        }
    }
}
