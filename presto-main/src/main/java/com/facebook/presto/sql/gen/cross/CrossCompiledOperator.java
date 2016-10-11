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
package com.facebook.presto.sql.gen.cross;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;

public interface CrossCompiledOperator
{
    class FieldDefinition
    {
        private final Class<?> type;
        private final Object value;

        public FieldDefinition(Class<?> type, Object value)
        {
            this.type = type;
            this.value = value;
        }

        public Object getValue()
        {
            return value;
        }

        public Class<?> getType()
        {
            return type;
        }
    }

    BytecodeBlock process(CrossCompilationContext context);

    Map<String, FieldDefinition> getFields();

    default ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    void close();
}
