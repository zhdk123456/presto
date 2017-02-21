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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Node visitor which verifies if given expression (actual) is matching other expression given as context (expected).
 * Visitor returns true if plans match to each other.
 * <p/>
 * Note that actual expression is using real name references (table columns etc) while expected expression is using symbol aliases.
 * Given symbol alias can point only to one real name reference.
 * <p/>
 * Example:
 * <pre>
 * NOT (orderkey = 3 AND custkey = 3 AND orderkey < 10)
 * </pre>
 * will match to:
 * <pre>
 * NOT (X = 3 AND Y = 3 AND X < 10)
 * </pre>
 * , but will not match to:
 * <pre>
 * NOT (X = 3 AND Y = 3 AND Z < 10)
 * </pre>
 * nor  to
 * <pre>
 * NOT (X = 3 AND X = 3 AND X < 10)
 * </pre>
 */
final class NodeVerifier
        extends AstVisitor<Boolean, Node>
{
    private final SymbolAliases symbolAliases;

    NodeVerifier(SymbolAliases symbolAliases)
    {
        this.symbolAliases = requireNonNull(symbolAliases, "symbolAliases is null");
    }

    @Override
    protected Boolean visitNode(Node node, Node context)
    {
        throw new IllegalStateException(format("Node %s is not supported", node));
    }

    @Override
    protected Boolean visitCast(Cast actual, Node expectedExpression)
    {
        if (!(expectedExpression instanceof Cast)) {
            return false;
        }

        Cast expected = (Cast) expectedExpression;

        if (!actual.getType().equals(expected.getType())) {
            return false;
        }

        return process(actual.getExpression(), expected.getExpression());
    }

    @Override
    protected Boolean visitInPredicate(InPredicate actual, Node expectedExpression)
    {
        if (expectedExpression instanceof InPredicate) {
            InPredicate expected = (InPredicate) expectedExpression;

            if (actual.getValueList() instanceof InListExpression) {
                return process(actual.getValue(), expected.getValue()) && process(actual.getValueList(), expected.getValueList());
            }
            else {
                checkState(
                        expected.getValueList() instanceof InListExpression,
                        "ExpressionVerifier doesn't support unpacked expected values. Feel free to add support if needed");
                /*
                 * If the expected value is a value list, but the actual is e.g. a SymbolReference,
                 * we need to unpack the value from the list so that when we hit visitSymbolReference, the
                 * expected.toString() call returns something that the symbolAliases actually contains.
                 * For example, InListExpression.toString returns "(onlyitem)" rather than "onlyitem".
                 *
                 * This is required because actual passes through the analyzer, planner, and possibly optimizers,
                 * one of which sometimes takes the liberty of unpacking the InListExpression.
                 *
                 * Since the expected value doesn't go through all of that, we have to deal with the case
                 * of the actual value being unpacked, but the expected value being an InListExpression.
                 */
                List<Expression> values = ((InListExpression) expected.getValueList()).getValues();
                checkState(values.size() == 1, "Multiple expressions in expected value list %s, but actual value is not a list", values, actual.getValue());
                Expression onlyExpectedExpression = values.get(0);
                return process(actual.getValue(), expected.getValue()) && process(actual.getValueList(), onlyExpectedExpression);
            }
        }
        return false;
    }

    @Override
    protected Boolean visitComparisonExpression(ComparisonExpression actual, Node expectedExpression)
    {
        if (expectedExpression instanceof ComparisonExpression) {
            ComparisonExpression expected = (ComparisonExpression) expectedExpression;
            if (actual.getType() == expected.getType()) {
                return process(actual.getLeft(), expected.getLeft()) && process(actual.getRight(), expected.getRight());
            }
        }
        return false;
    }

    @Override
    protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression actual, Node expectedExpression)
    {
        if (expectedExpression instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression expected = (ArithmeticBinaryExpression) expectedExpression;
            if (actual.getType() == expected.getType()) {
                return process(actual.getLeft(), expected.getLeft()) && process(actual.getRight(), expected.getRight());
            }
        }
        return false;
    }

    protected Boolean visitGenericLiteral(GenericLiteral actual, Node expected)
    {
        return getValueFromLiteral(actual).equals(getValueFromLiteral(expected));
    }

    @Override
    protected Boolean visitLongLiteral(LongLiteral actual, Node expected)
    {
        return getValueFromLiteral(actual).equals(getValueFromLiteral(expected));
    }

    @Override
    protected Boolean visitDoubleLiteral(DoubleLiteral actual, Node expected)
    {
        if (expected instanceof DoubleLiteral) {
            return getValueFromLiteral(actual).equals(getValueFromLiteral(expected));
        }

        return false;
    }

    @Override
    protected Boolean visitBooleanLiteral(BooleanLiteral actual, Node expected)
    {
        return getValueFromLiteral(actual).equals(getValueFromLiteral(expected));
    }

    private static String getValueFromLiteral(Node expression)
    {
        if (expression instanceof LongLiteral) {
            return String.valueOf(((LongLiteral) expression).getValue());
        }
        else if (expression instanceof BooleanLiteral) {
            return String.valueOf(((BooleanLiteral) expression).getValue());
        }
        else if (expression instanceof DoubleLiteral) {
            return String.valueOf(((DoubleLiteral) expression).getValue());
        }
        else if (expression instanceof GenericLiteral) {
            return ((GenericLiteral) expression).getValue();
        }
        else {
            throw new IllegalArgumentException("Unsupported literal expression type: " + expression.getClass().getName());
        }
    }

    @Override
    protected Boolean visitStringLiteral(StringLiteral actual, Node expectedExpression)
    {
        if (expectedExpression instanceof StringLiteral) {
            StringLiteral expected = (StringLiteral) expectedExpression;
            return actual.getValue().equals(expected.getValue());
        }
        return false;
    }

    @Override
    protected Boolean visitLogicalBinaryExpression(LogicalBinaryExpression actual, Node expectedExpression)
    {
        if (expectedExpression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression expected = (LogicalBinaryExpression) expectedExpression;
            if (actual.getType() == expected.getType()) {
                return process(actual.getLeft(), expected.getLeft()) && process(actual.getRight(), expected.getRight());
            }
        }
        return false;
    }

    @Override
    protected Boolean visitNotExpression(NotExpression actual, Node expected)
    {
        if (expected instanceof NotExpression) {
            return process(actual.getValue(), ((NotExpression) expected).getValue());
        }
        return false;
    }

    @Override
    protected Boolean visitSymbolReference(SymbolReference actual, Node expected)
    {
        if (!(expected instanceof SymbolReference)) {
            return false;
        }
        return symbolAliases.get(((SymbolReference) expected).getName()).equals(actual);
    }

    @Override
    protected Boolean visitFunctionCall(FunctionCall node, Node expected)
    {
        if (!(expected instanceof FunctionCall)) {
            return false;
        }
        FunctionCall expectedFunctionCall = (FunctionCall) expected;

        if (!node.getName().equals(expectedFunctionCall.getName())) {
            return false;
        }

        if (!process(node.getArguments(), expectedFunctionCall.getArguments())) {
            return false;
        }

        if (!process(node.getFilter(), expectedFunctionCall.getFilter())) {
            return false;
        }

        if (!process(node.getWindow(), expectedFunctionCall.getWindow())) {
            return false;
        }

        return true;
    }

    private <T extends Node> boolean process(List<T> actual, List<T> expected)
    {
        if (actual.size() != expected.size()) {
            return false;
        }

        for (int i = 0; i < actual.size(); i++) {
            if (!process(actual.get(i), expected.get(i))) {
                return false;
            }
        }

        return true;
    }

    private <T extends Node> boolean process(Optional<T> actual, Optional<T> expected)
    {
        if (actual.isPresent() != expected.isPresent()) {
            return false;
        }

        if (actual.isPresent() && !process(actual.get(), expected.get())) {
            return false;
        }

        return true;
    }
}
