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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;

import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static org.testng.Assert.assertEquals;

public class TestQueryPlansDeterministic
        extends AbstractTestQueries
{
    public TestQueryPlansDeterministic()
    {
        super(createLocalQueryRunner());
    }

    //FIXME deduplicate with local query runner
    //FIXME make it run all the queries, not only the local ones
    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(new ConnectorId(TESTING_CATALOG), TEST_CATALOG_PROPERTIES);

        return localQueryRunner;
    }

    private void checkPlanIsDeterministic(String sql)
    {
        MaterializedResult result = computeActual("EXPLAIN " + sql);
        MaterializedResult result2 = computeActual("EXPLAIN " + sql);

        System.out.println(result.toString());
        System.out.println("================================================");

        int i = 0;
        while (i < 10) {
            i++;
            System.out.println(result2.toString());
            System.out.println("================================================");
            assertEquals(moduloSymbolNumbers(result2), moduloSymbolNumbers(result), "Explain differs after " + i + " attempts");
            result = result2;
            result2 = computeActual("EXPLAIN " + sql);
        }
    }

    private String moduloSymbolNumbers(MaterializedResult result2)
    {
        return result2.toString();
    }

    @Override
    protected void assertQuery(@Language("SQL") String sql)
            throws Exception
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertQuery(Session session, @Language("SQL") String sql)
            throws Exception
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    public void assertQueryOrdered(@Language("SQL") String sql)
            throws Exception
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertQueryOrdered(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertQueryOrdered(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertUpdate(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertUpdate(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertUpdate(@Language("SQL") String sql)
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertUpdate(Session session, @Language("SQL") String sql)
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertUpdate(@Language("SQL") String sql, long count)
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertUpdate(Session session, @Language("SQL") String sql, long count)
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertQueryFails(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
    }

    @Override
    protected void assertQueryFails(Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
    }

    @Override
    protected void assertAccessAllowed(@Language("SQL") String sql, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
            throws Exception
    {
    }

    @Override
    protected void assertAccessAllowed(Session session, @Language("SQL") String sql, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
            throws Exception
    {

    }

    @Override
    protected void assertAccessDenied(@Language("SQL") String sql, @Language("RegExp") String exceptionsMessageRegExp, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
            throws Exception
    {
    }

    @Override
    protected void assertAccessDenied(Session session, @Language("SQL") String sql, @Language("RegExp") String exceptionsMessageRegExp, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
            throws Exception
    {

    }

    @Override
    protected void assertTableColumnNames(String tableName, String... columnNames)
    {
    }
}
