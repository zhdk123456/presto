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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.util.IdentityHashSet;

import java.util.IdentityHashMap;

import static java.util.Objects.requireNonNull;

public class ExpressionAnalysis
{
    private final IdentityHashMap<Expression, Type> expressionTypes;
    private final IdentityHashMap<Expression, Type> expressionCoercions;
    private final IdentityHashSet<Expression> typeOnlyCoercions;
    private final IdentityHashSet<Expression> columnReferences;
    private final IdentityHashSet<InPredicate> subqueryInPredicates;
    private final IdentityHashSet<SubqueryExpression> scalarSubqueries;
    private final IdentityHashSet<ExistsPredicate> existsSubqueries;
    private final IdentityHashSet<QuantifiedComparisonExpression> quantifiedComparisons;
    // For lambda argument references, maps each QualifiedNameReference to the referenced LambdaArgumentDeclaration
    private final IdentityHashMap<Identifier, LambdaArgumentDeclaration> lambdaArgumentReferences;

    public ExpressionAnalysis(
            IdentityHashMap<Expression, Type> expressionTypes,
            IdentityHashMap<Expression, Type> expressionCoercions,
            IdentityHashSet<InPredicate> subqueryInPredicates,
            IdentityHashSet<SubqueryExpression> scalarSubqueries,
            IdentityHashSet<ExistsPredicate> existsSubqueries,
            IdentityHashSet<Expression> columnReferences,
            IdentityHashSet<Expression> typeOnlyCoercions,
            IdentityHashSet<QuantifiedComparisonExpression> quantifiedComparisons,
            IdentityHashMap<Identifier, LambdaArgumentDeclaration> lambdaArgumentReferences)
    {
        this.expressionTypes = new IdentityHashMap<>(requireNonNull(expressionTypes, "expressionTypes is null"));
        this.expressionCoercions = new IdentityHashMap<>(requireNonNull(expressionCoercions, "expressionCoercions is null"));
        this.typeOnlyCoercions = IdentityHashSet.create(requireNonNull(typeOnlyCoercions, "typeOnlyCoercions is null"));
        this.columnReferences = IdentityHashSet.create(requireNonNull(columnReferences, "columnReferences is null"));
        this.subqueryInPredicates = IdentityHashSet.create(requireNonNull(subqueryInPredicates, "subqueryInPredicates is null"));
        this.scalarSubqueries = IdentityHashSet.create(requireNonNull(scalarSubqueries, "subqueryInPredicates is null"));
        this.existsSubqueries = IdentityHashSet.create(requireNonNull(existsSubqueries, "existsSubqueries is null"));
        this.quantifiedComparisons = IdentityHashSet.create(requireNonNull(quantifiedComparisons, "quantifiedComparisons is null"));
        this.lambdaArgumentReferences = new IdentityHashMap<>(requireNonNull(lambdaArgumentReferences, "lambdaArgumentReferences is null"));
    }

    public Type getType(Expression expression)
    {
        return expressionTypes.get(expression);
    }

    public IdentityHashMap<Expression, Type> getExpressionTypes()
    {
        return new IdentityHashMap<>(expressionTypes);
    }

    public Type getCoercion(Expression expression)
    {
        return expressionCoercions.get(expression);
    }

    public LambdaArgumentDeclaration getLambdaArgumentReference(Identifier qualifiedNameReference)
    {
        return lambdaArgumentReferences.get(qualifiedNameReference);
    }

    public boolean isTypeOnlyCoercion(Expression expression)
    {
        return typeOnlyCoercions.contains(expression);
    }

    public Set<Expression> getColumnReferences()
    {
        return columnReferences;
    }

    public IdentityHashSet<InPredicate> getSubqueryInPredicates()
    {
        return IdentityHashSet.create(subqueryInPredicates);
    }

    public IdentityHashSet<SubqueryExpression> getScalarSubqueries()
    {
        return IdentityHashSet.create(scalarSubqueries);
    }

    public IdentityHashSet<ExistsPredicate> getExistsSubqueries()
    {
        return IdentityHashSet.create(existsSubqueries);
    }

    public IdentityHashSet<QuantifiedComparisonExpression> getQuantifiedComparisons()
    {
        return IdentityHashSet.create(quantifiedComparisons);
    }
}
