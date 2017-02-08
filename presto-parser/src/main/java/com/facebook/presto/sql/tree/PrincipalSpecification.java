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
package com.facebook.presto.sql.tree;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.facebook.presto.sql.tree.PrincipalSpecification.Type.CURRENT_ROLE;
import static com.facebook.presto.sql.tree.PrincipalSpecification.Type.CURRENT_USER;
import static com.facebook.presto.sql.tree.PrincipalSpecification.Type.ROLE;
import static com.facebook.presto.sql.tree.PrincipalSpecification.Type.UNSPECIFIED;
import static com.facebook.presto.sql.tree.PrincipalSpecification.Type.USER;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PrincipalSpecification
{
    public enum Type
    {
        UNSPECIFIED, USER, ROLE, CURRENT_USER, CURRENT_ROLE
    }

    private final Type type;
    @Nullable
    private final String name;

    public static PrincipalSpecification createUnspecified(String name)
    {
        return new PrincipalSpecification(UNSPECIFIED, requireNonNull(name, "name is null"));
    }

    public static PrincipalSpecification createUser(String name)
    {
        return new PrincipalSpecification(USER, requireNonNull(name, "name is null"));
    }

    public static PrincipalSpecification createRole(String name)
    {
        return new PrincipalSpecification(ROLE, requireNonNull(name, "name is null"));
    }

    public static PrincipalSpecification createCurrentUser()
    {
        return new PrincipalSpecification(CURRENT_USER, null);
    }

    public static PrincipalSpecification createCurrentRole()
    {
        return new PrincipalSpecification(CURRENT_ROLE, null);
    }

    private PrincipalSpecification(Type type, String name)
    {
        this.type = requireNonNull(type, "type is null");
        this.name = name;
    }

    public Type getType()
    {
        return type;
    }

    public String getName()
    {
        checkState(type == USER || type == ROLE || type == UNSPECIFIED, "Invalid operations for principal specification with type '%s'", type);
        return name;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrincipalSpecification that = (PrincipalSpecification) o;
        return type == that.type &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, name);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("name", name)
                .toString();
    }
}
