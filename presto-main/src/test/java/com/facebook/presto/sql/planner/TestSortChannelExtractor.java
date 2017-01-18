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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.SortChannelExtractor.SortChannel;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static org.testng.AssertJUnit.assertEquals;

public class TestSortChannelExtractor
{
    private static final Map<Symbol, Integer> BUILD_LAYOUT = ImmutableMap.of(
            new Symbol("b1"), 1,
            new Symbol("b2"), 2);

    @Test
    public void testGetSortChannel()
    {
        assertGetSortChannel(
                new ComparisonExpression(
                        ComparisonExpressionType.GREATER_THAN,
                        new FieldReference(11),
                        new FieldReference(1)),
                1,
                true);

        assertGetSortChannel(
                new ComparisonExpression(
                        ComparisonExpressionType.LESS_THAN_OR_EQUAL,
                        new FieldReference(1),
                        new FieldReference(11)),
                1,
                true);

        assertGetSortChannel(
                new ComparisonExpression(
                        ComparisonExpressionType.GREATER_THAN,
                        new FieldReference(1),
                        new FieldReference(11)),
                1,
                false);

        assertGetSortChannel(
                new ComparisonExpression(
                        ComparisonExpressionType.GREATER_THAN,
                        new FieldReference(1),
                        new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD, new FieldReference(2), new FieldReference(11))));

        assertGetSortChannel(
                new ComparisonExpression(
                        ComparisonExpressionType.GREATER_THAN,
                        new FunctionCall(QualifiedName.of("sin"), ImmutableList.of(new FieldReference(1))),
                        new FieldReference(11)));
    }

    private static void assertGetSortChannel(Expression expression)
    {
        Optional<SortChannel> actual = SortChannelExtractor.getSortChannel(BUILD_LAYOUT, expression);
        assertEquals(Optional.empty(), actual);
    }

    private static void assertGetSortChannel(Expression expression, int expectedChannel, boolean expectedIsDescending)
    {
        Optional<SortChannel> expected = Optional.of(new SortChannel(expectedChannel, expectedIsDescending));
        Optional<SortChannel> actual = SortChannelExtractor.getSortChannel(BUILD_LAYOUT, expression);
        assertEquals(expected, actual);
    }
}
