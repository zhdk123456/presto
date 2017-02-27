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

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class SymbolReferenceExtractor
{
    private SymbolReferenceExtractor() {}

    public static Set<SymbolReference> extract(Expression expression)
    {
        SymbolExtractingVisitor visitor = new SymbolExtractingVisitor();
        visitor.process(expression);

        return visitor.getSymbolReferences();
    }

    private static class SymbolExtractingVisitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        private final ImmutableSet.Builder<SymbolReference> symbolReferences = ImmutableSet.builder();

        public Set<SymbolReference> getSymbolReferences()
        {
            return symbolReferences.build();
        }

        @Override
        public Void visitSymbolReference(SymbolReference node, Void context)
        {
            symbolReferences.add(node);
            return null;
        }
    }
}
