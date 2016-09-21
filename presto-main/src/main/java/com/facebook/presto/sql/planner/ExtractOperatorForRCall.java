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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RcallNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class ExtractOperatorForRCall
        implements PlanOptimizer
{
    public static final String R_CALL_FUNCTION_NAME = "r";
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public ExtractOperatorForRCall(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(symbolAllocator, idAllocator, metadata, sqlParser, session, types), plan, BooleanLiteral.TRUE_LITERAL);
    }

    private class Rewriter
            extends SimplePlanRewriter<Expression>
    {
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final Session session;
        private final Map<Symbol, Type> types;
        private final ExpressionEquivalence expressionEquivalence;

        private Rewriter(
                SymbolAllocator symbolAllocator,
                PlanNodeIdAllocator idAllocator,
                Metadata metadata,
                SqlParser sqlParser,
                Session session,
                Map<Symbol, Type> types)
        {
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.expressionEquivalence = new ExpressionEquivalence(metadata, sqlParser);
        }

        private Type extractType(Expression expression)
        {
            return getExpressionTypes(session, metadata, sqlParser, symbolAllocator.getTypes(), expression, emptyList() /* parameters have already been replaced */).get(expression);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Expression> context)
        {
            ProjectNode nodeRewritten = (ProjectNode) context.defaultRewrite(node);
/*            if (!containsRCall(nodeRewritten)) {
                return nodeRewritten;
            }*/

            Map<Symbol, Expression> rewrittenAssigments = new HashMap<>();
            Map<Symbol, FunctionCall> rcalls = new LinkedHashMap<>();
            for (Symbol symbol : nodeRewritten.getOutputSymbols()) {
                Expression expression = nodeRewritten.getAssignments().get(symbol);
                Expression rewrittenAssigment = ExpressionTreeRewriter.rewriteWith(new MyRewriter(), expression, rcalls);
                rewrittenAssigments.put(symbol, rewrittenAssigment);
            }

            PlanNode sourceNodePointer = node.getSource();
            for (Symbol rcallSymbol : rcalls.keySet()) {
                FunctionCall rcallFunction = rcalls.get(rcallSymbol);

                checkArgument(rcallFunction.getArguments().size() == 2, "Expected exactly 2 argument for R call");
                VarcharType varcharType = VarcharType.createUnboundedVarcharType();
                Slice rcallProgram = (Slice) ExpressionInterpreter.evaluateConstantExpression(rcallFunction.getArguments().get(0), varcharType, metadata, session, emptyList());

                ArgumentsProjectNodeResult argumentsProjectNode = buildArgumentsProjectNode(rcallFunction, sourceNodePointer);
                sourceNodePointer = new RcallNode(idAllocator.getNextId(), argumentsProjectNode.argumentsProjectNode, rcallProgram.toString(UTF_8), argumentsProjectNode.argumentsSymbols, rcallSymbol);
            }

            return new ProjectNode(node.getId(), sourceNodePointer, rewrittenAssigments);
        }

        private ArgumentsProjectNodeResult buildArgumentsProjectNode(FunctionCall rcallFunction, PlanNode rcallSourceNode)
        {
            Map<Symbol, Expression> assigments = new HashMap<>();
            ImmutableList.Builder<Symbol> argumentSymbols = ImmutableList.builder();
            for (Expression expression : getActualArguments(rcallFunction)) {
                Symbol symbol = symbolAllocator.newSymbol(expression, extractType(expression));
                assigments.put(symbol, expression);
                argumentSymbols.add(symbol);
            }
            for (Symbol symbol : rcallSourceNode.getOutputSymbols()) {
                assigments.put(symbol, symbol.toSymbolReference());
            }

            return new ArgumentsProjectNodeResult(
                    new ProjectNode(idAllocator.getNextId(), rcallSourceNode, assigments),
                    argumentSymbols.build()
            );
        }

        private List<Expression> getActualArguments(FunctionCall rcallFunction)
        {
            List actualArguments = new ArrayList<>();
            for (int i = 1; i < rcallFunction.getArguments().size(); ++i) {
                actualArguments.add(rcallFunction.getArguments().get(1));
            }
            return actualArguments;
        }

        private class ArgumentsProjectNodeResult
        {
            public final ProjectNode argumentsProjectNode;
            public final List<Symbol> argumentsSymbols;

            public ArgumentsProjectNodeResult(ProjectNode argumentsProjectNode, List<Symbol> argumentsSymbols)
            {
                this.argumentsProjectNode = argumentsProjectNode;
                this.argumentsSymbols = argumentsSymbols;
            }
        }

        private class MyRewriter
                extends ExpressionRewriter<Map<Symbol, FunctionCall>>
        {
            @Override
            public Expression rewriteFunctionCall(FunctionCall node, Map<Symbol, FunctionCall> assignedRcalls, ExpressionTreeRewriter<Map<Symbol, FunctionCall>> treeRewriter)
            {
                FunctionCall nodeRewritten = treeRewriter.defaultRewrite(node, assignedRcalls);
                if (node.getName().getSuffix().equalsIgnoreCase(R_CALL_FUNCTION_NAME)) {
                    Symbol symbolForRcall = symbolAllocator.newSymbol("rcallResult", extractType(node));
                    assignedRcalls.put(symbolForRcall, nodeRewritten);
                    return symbolForRcall.toSymbolReference();
                }
                return super.rewriteFunctionCall(node, assignedRcalls, treeRewriter);
            }
        }

        private boolean containsRCall(ProjectNode node)
        {
            return false;
        }
    }
}
