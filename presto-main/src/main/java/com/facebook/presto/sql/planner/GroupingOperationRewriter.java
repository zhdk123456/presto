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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.type.ListLiteralType;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.scalar.GroupingOperationFunction.GROUPING;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.resolveFunction;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class GroupingOperationRewriter
        extends ExpressionRewriter<Void>
{
    private final QuerySpecification queryNode;
    private final Analysis analysis;
    private final Metadata metadata;
    private final Map<QuerySpecification, Symbol> groupIdSymbols;

    public GroupingOperationRewriter(QuerySpecification queryNode, Analysis analysis, Metadata metadata, Map<QuerySpecification, Symbol> groupIdSymbols)
    {
        this.queryNode = requireNonNull(queryNode, "node is null");
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.groupIdSymbols = requireNonNull(groupIdSymbols, "groupIdSymbols is null");
    }

    @Override
    public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return treeRewriter.defaultRewrite(node, context);
    }

    @Override
    public Expression rewriteGroupingOperation(GroupingOperation node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        Expression rewrittenExpression = rewriteGroupingOperationToExpression(node, queryNode);
        if (rewrittenExpression instanceof FunctionCall) {
            FunctionCall rewrittenFunctionCall = (FunctionCall) rewrittenExpression;
            IdentityHashMap<Expression, Type> expressionTypes = new IdentityHashMap<>();
            IdentityHashMap<FunctionCall, Signature> functionSignatures = new IdentityHashMap<>();
            List<TypeSignatureProvider> functionTypes = Arrays.asList(
                    new TypeSignatureProvider(BIGINT.getTypeSignature()),
                    new TypeSignatureProvider(ListLiteralType.LIST_LITERAL.getTypeSignature()),
                    new TypeSignatureProvider(ListLiteralType.LIST_LITERAL.getTypeSignature())
            );
            Signature functionSignature = resolveFunction(rewrittenFunctionCall, functionTypes, metadata.getFunctionRegistry());

            expressionTypes.put(rewrittenExpression, INTEGER);
            functionSignatures.put(rewrittenFunctionCall, functionSignature);
        }

        return rewrittenExpression;
    }

    public Expression rewriteGroupingOperationToExpression(GroupingOperation expression, QuerySpecification node)
    {
        List<Expression> columnReferences = ImmutableList.copyOf(analysis.getColumnReferences());
        List<Expression> groupingOrdinals;
        ImmutableList.Builder<Expression> groupingOrdinalsBuilder = ImmutableList.builder();
        groupingOrdinalsBuilder.addAll(expression.getGroupingColumns().stream()
                .map(columnReferences::indexOf)
                .map(columnOrdinal -> new LongLiteral(Integer.toString(columnOrdinal)))
                .collect(Collectors.toList()));
        groupingOrdinals = groupingOrdinalsBuilder.build();

        List<List<Expression>> groupingSetOrdinals;
        ImmutableList.Builder<List<Expression>> groupingSetOrdinalsBuilder = ImmutableList.builder();
        for (List<Expression> groupingSet : analysis.getGroupingSets(node)) {
            ImmutableList.Builder<Expression> ordinalsBuilder = ImmutableList.builder();
            ordinalsBuilder.addAll(groupingSet.stream()
                    .map(columnReferences::indexOf)
                    .map(columnOrdinal -> new LongLiteral(Integer.toString(columnOrdinal)))
                    .collect(Collectors.toList())
            );

            groupingSetOrdinalsBuilder.add(ordinalsBuilder.build());
        }
        groupingSetOrdinals = groupingSetOrdinalsBuilder.build();

        checkState(node.getGroupBy().isPresent(), "GroupBy node must be present");
        List<GroupingElement> groupingElements = node.getGroupBy().get().getGroupingElements().stream()
                .filter(element -> element instanceof GroupingSets || element instanceof Rollup || element instanceof Cube)
                .collect(toImmutableList());
        Expression firstArgument;
        // No GroupIdNode and a GROUPING() operation imply a single grouping, which
        // means that any columns specified as arguments to GROUPING() will be included
        // in the group and none of them will be aggregated over. Hence, re-write the
        // GroupingOperation to a constant literal of 0.
        // See SQL:2011:4.16.2 and SQL:2011:6.9.10.
        if (groupingElements.isEmpty() && !groupIdSymbols.containsKey(node)) {
            return new LongLiteral("0");
        }
        else {
            checkState(groupIdSymbols.containsKey(node), "groupId symbol for QuerySpecification node is missing");
            firstArgument = groupIdSymbols.get(node).toSymbolReference();
        }

        List<Expression> arguments = Arrays.asList(
                firstArgument,
                new Cast(new ArrayConstructor(groupingOrdinals), ListLiteralType.NAME),
                new Cast(new ArrayConstructor(groupingSetOrdinals.stream().map(ArrayConstructor::new).collect(toImmutableList())), ListLiteralType.NAME)
        );
        return new FunctionCall(expression.getLocation().get(), QualifiedName.of(GROUPING), arguments);
    }
}
