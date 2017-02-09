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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.tree.CreateRole;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.metadata.MetadataUtil.createCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.createPrincipal;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CreateRoleTask
        implements DataDefinitionTask<CreateRole>
{
    @Override
    public String getName()
    {
        return "CREATE ROLE";
    }

    @Override
    public CompletableFuture<?> execute(CreateRole statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        String catalog = createCatalogName(session, statement, statement.getCatalog());
        metadata.createRole(
                session,
                statement.getName(),
                statement.getGrantor().map(specification -> createPrincipal(session, specification)),
                catalog);
        return completedFuture(null);
    }
}
