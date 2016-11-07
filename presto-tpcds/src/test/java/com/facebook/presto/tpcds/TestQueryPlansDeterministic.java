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
package com.facebook.presto.tpcds;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tests.AbstractTestQueries;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
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
                .setSchema("sf1")
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpcdsConnectorFactory(1),
                ImmutableMap.<String, String>of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties("connector", TEST_CATALOG_PROPERTIES);

        return localQueryRunner;
    }

    @Test
    public void testTpchQ9()
            throws Exception
    {
        //FIXME test it with TPCDS Q9
        //FIXME make it run all the tpch queries
        assertQuery("SELECT\n" +
                "  nation,\n" +
                "  o_year,\n" +
                "  sum(amount) AS sum_profit\n" +
                "FROM (\n" +
                "       SELECT\n" +
                "         n_name                                                          AS nation,\n" +
                "         extract(YEAR FROM o_orderdate)                                  AS o_year,\n" +
                "         l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount\n" +
                "       FROM\n" +
                "         part,\n" +
                "         supplier,\n" +
                "         lineitem,\n" +
                "         partsupp,\n" +
                "         orders,\n" +
                "         nation\n" +
                "       WHERE\n" +
                "         s_suppkey = l_suppkey\n" +
                "         AND ps_suppkey = l_suppkey\n" +
                "         AND ps_partkey = l_partkey\n" +
                "         AND p_partkey = l_partkey\n" +
                "         AND o_orderkey = l_orderkey\n" +
                "         AND s_nationkey = n_nationkey\n" +
                "         AND p_name LIKE '%green%'\n" +
                "     ) AS profit\n" +
                "GROUP BY\n" +
                "  nation,\n" +
                "  o_year\n" +
                "ORDER BY\n" +
                "  nation,\n" +
                "  o_year DESC\n");
    }

    @Test
    public void testTpchQ6()
            throws Exception
    {
        assertQuery("SELECT a.ca_state STATE,\n" +
                "                  count(*) cnt\n" +
                "FROM \"sf1\".customer_address a,\n" +
                "     \"sf1\".customer c,\n" +
                "     \"sf1\".store_sales s,\n" +
                "     \"sf1\".date_dim d,\n" +
                "     \"sf1\".item i\n" +
                "WHERE a.ca_address_sk = c.c_current_addr_sk\n" +
                "  AND c.c_customer_sk = s.ss_customer_sk\n" +
                "  AND s.ss_sold_date_sk = d.d_date_sk\n" +
                "  AND s.ss_item_sk = i.i_item_sk\n" +
                "  AND d.d_month_seq =\n" +
                "    (SELECT DISTINCT (d_month_seq)\n" +
                "     FROM \"sf1\".date_dim\n" +
                "     WHERE d_year = 2001 \n" +
                "       AND d_moy = 1)\n" +
                "  AND i.i_current_price > 1.2 *\n" +
                "    (SELECT avg(j.i_current_price)\n" +
                "     FROM \"sf1\".item j\n" +
                "     WHERE j.i_category = i.i_category)\n" +
                "GROUP BY a.ca_state\n" +
                "HAVING count(*) >= 10\n" +
                "ORDER BY cnt LIMIT 100\n");
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
    protected void assertQuery(String sql)
            throws Exception
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertQuery(Session session, String sql)
            throws Exception
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    public void assertQueryOrdered(String sql)
            throws Exception
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertQuery(String actual, String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertQuery(Session session, String actual, String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertQueryOrdered(String actual, String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertQueryOrdered(Session session, String actual, String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertUpdate(String actual, String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertUpdate(Session session, String actual, String expected)
            throws Exception
    {
        checkPlanIsDeterministic(actual);
    }

    @Override
    protected void assertUpdate(String sql)
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertUpdate(Session session, String sql)
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertUpdate(String sql, long count)
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertUpdate(Session session, String sql, long count)
    {
        checkPlanIsDeterministic(sql);
    }

    @Override
    protected void assertQueryFails(String sql, String expectedMessageRegExp)
    {
    }

    @Override
    protected void assertQueryFails(Session session, String sql, String expectedMessageRegExp)
    {
    }

    @Override
    protected void assertAccessAllowed(String sql, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
            throws Exception
    {
    }

    @Override
    protected void assertAccessAllowed(Session session, String sql, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
            throws Exception
    {
    }

    @Override
    protected void assertAccessDenied(String sql, String exceptionsMessageRegExp, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
            throws Exception
    {
    }

    @Override
    protected void assertAccessDenied(Session session, String sql, String exceptionsMessageRegExp, TestingAccessControlManager.TestingPrivilege... deniedPrivileges)
            throws Exception
    {
    }

    @Override
    protected void assertTableColumnNames(String tableName, String... columnNames)
    {
    }
}
