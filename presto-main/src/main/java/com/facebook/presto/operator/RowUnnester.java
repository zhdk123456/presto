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
package com.facebook.presto.operator;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.type.RowType;

import javax.annotation.Nullable;

public class RowUnnester
        implements Unnester
{
    private RowType rowType;
    private final Block block;
    private final int channelCount;

    private int position;
    private final int positionCount;

    public RowUnnester(RowType rowType, @Nullable Block mapBlock)
    {
        this.channelCount = rowType.getFields().size();
        this.rowType = rowType;

        this.block = mapBlock;
        this.positionCount = mapBlock == null ? 0 : mapBlock.getPositionCount();
    }

    protected void appendTo(PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int i = 0; i < rowType.getFields().size(); ++i) {
            BlockBuilder bb = pageBuilder.getBlockBuilder(outputChannelOffset + i);
            rowType.getTypeParameters().get(i).appendTo(block, position++, bb);
        }
    }

    @Override
    public boolean hasNext()
    {
        return position < positionCount;
    }

    @Override
    public final int getChannelCount()
    {
        return channelCount;
    }

    @Override
    public final void appendNext(PageBuilder pageBuilder, int outputChannelOffset)
    {
        appendTo(pageBuilder, outputChannelOffset);
    }
}
