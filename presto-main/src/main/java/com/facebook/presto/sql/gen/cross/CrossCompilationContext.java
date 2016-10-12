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
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.gen.CachedInstanceBinder;

import java.util.Optional;
import java.util.function.Supplier;

public interface CrossCompilationContext
{
    class ChannelBlock
    {
        private final BytecodeExpression block;
        private final BytecodeExpression position;

        public ChannelBlock(BytecodeExpression block, BytecodeExpression position)
        {
            this.block = block;
            this.position = position;
        }

        public BytecodeExpression getBlock()
        {
            return block;
        }

        public BytecodeExpression getPosition()
        {
            return position;
        }
    }

    Metadata getMetadata();

    CachedInstanceBinder getCachedInstanceBinder();

    Scope getMethodScope();

    BytecodeBlock getMethodHeader();

    String uniqueVariableName(String name);

    BytecodeExpression getField(String name);

    BytecodeExpression getChannel(int channel);

    Optional<ChannelBlock> getChannelBlock(int channel);

    BytecodeExpression isNull(int channel);

    void defineChannel(int channel, Supplier<BytecodeExpression> definition);

    void defineChannelBlock(int channel, Supplier<ChannelBlock> channelBlock);

    void defineIsNull(int channel, Supplier<BytecodeExpression> definition);

    void mapInputToOutputChannel(int inputChannel, int outputChannel);

    BytecodeBlock processDownstreamOperator();
}
