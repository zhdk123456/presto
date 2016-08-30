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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;

import java.util.Map;
import java.util.regex.Pattern;

final class ProjectNodeMatcher
        implements Matcher
{
    private final Pattern pattern;

    ProjectNodeMatcher(String pattern)
    {
        this.pattern = Pattern.compile(pattern);
    }

    @Override
    public boolean matches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        if (node instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) node;
            for (Map.Entry<Symbol, Expression> assignment : projectNode.getAssignments().entrySet()) {
                Expression expression = assignment.getValue();
                if (pattern.matcher(expression.getClass().getSimpleName()).find()) {
                    return true;
                }
            }
        }
        return false;
    }
}
