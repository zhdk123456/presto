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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.assertions.SymbolMatcher;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.SampleNode.Type.BERNOULLI;

public class TestPushFilter
{
    private final RuleTester tester = new RuleTester();
    private final Rule rule = new PushFilter();

    @Test
    public void testDoesNotFire()
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("uid"),
                                p.assignUniqueId(p.symbol("uid", BOOLEAN),
                                        p.values(p.symbol("a", BOOLEAN)))))
                .doesNotFire();
    }

    @Test
    public void testAssignUniqueId()
            throws Exception
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("a"),
                                p.assignUniqueId(p.symbol("uid", BOOLEAN),
                                        p.values(p.symbol("a", BOOLEAN)))))
                .matches(node(AssignUniqueId.class,
                        filter("a",
                                values(ImmutableMap.of("a", 0)))));

        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("a AND uid"),
                                p.assignUniqueId(p.symbol("uid", BOOLEAN),
                                        p.values(p.symbol("a", BOOLEAN)))))
                .matches(
                        filter("uid",
                                node(AssignUniqueId.class,
                                        filter("a",
                                                values(ImmutableMap.of("a", 0))))
                                        .withAlias("uid", new SymbolMatcher(1))));
    }

    @Test
    public void testSample()
            throws Exception
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("a"),
                                p.sample(1.0, BERNOULLI, p.values(p.symbol("a", BIGINT)))))
                .matches(node(SampleNode.class,
                        filter("a",
                                values(ImmutableMap.of("a", 0)))));
    }

    @Test
    public void testSort()
            throws Exception
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("a"),
                                p.sort(ImmutableList.of(p.symbol("a", BIGINT)), p.values(p.symbol("a", BIGINT)))))
                .matches(node(SortNode.class,
                        filter("a",
                                values(ImmutableMap.of("a", 0)))));
    }
}
