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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestQuerySpillLimits
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema(TINY_SCHEMA_NAME)
            .setSystemProperty(SystemSessionProperties.SPILL_ENABLED, "true")
            .setSystemProperty(SystemSessionProperties.OPERATOR_MEMORY_LIMIT_BEFORE_SPILL, "1B") //spill constantly
            .build();

    @Test(timeOut = 240_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded local spill limit of 10B")
    public void testMaxSpillPerNodeLimit()
            throws Exception
    {
        try (QueryRunner queryRunner = createLocalQueryRunner(
                new NodeSpillConfig().setMaxSpillPerNode(DataSize.succinctBytes(10)))) {
            queryRunner.execute(SESSION, "SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
        }
    }

    @Test(timeOut = 240_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded per-query local spill limit of 10B")
    public void testQueryMaxSpillPerNodeLimit()
            throws Exception
    {
        try (QueryRunner queryRunner = createLocalQueryRunner(
                new NodeSpillConfig().setQueryMaxSpillPerNode(DataSize.succinctBytes(10)))) {
            queryRunner.execute(SESSION, "SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
        }
    }

    private static LocalQueryRunner createLocalQueryRunner(NodeSpillConfig nodeSpillConfig)
            throws Exception
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(
                SESSION,
                new FeaturesConfig().setSpillEnabled(true),
                nodeSpillConfig,
                false,
                true);

        queryRunner.createCatalog(
                SESSION.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());

        return queryRunner;
    }
}
