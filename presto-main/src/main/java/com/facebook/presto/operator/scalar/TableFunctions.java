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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public final class TableFunctions
{
    private TableFunctions() {}

    @Description("tsequence")
    @ScalarFunction
    @Nullable
    @SqlType("array(row(x bigint))")
    public static Block tsequence(@SqlType("bigint") long a, @SqlType("bigint") long b)
    {
        RowType rowType = new RowType(ImmutableList.of(BIGINT), Optional.of(ImmutableList.of("x")));
        BlockBuilder arrayBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), (int) Math.max(0, b - a));
        for (long i = a; i < b; ++i) {
            BlockBuilder blockBuilder = new InterleavedBlockBuilder(rowType.getTypeParameters(), new BlockBuilderStatus(), 1);
            blockBuilder.writeLong(i).closeEntry();
            rowType.writeObject(arrayBuilder, blockBuilder.build());
        }
        return arrayBuilder.build();
    }

    // example call:
    // select * from (select x,y from unnest(tgrid(1,5,10,20)) as t(tmp1), unnest(tmp1));
    @Description("tgrid")
    @ScalarFunction
    @Nullable
    @SqlType("array(row(x bigint,y bigint))")
    public static Block tgrid(@SqlType("bigint") long x1, @SqlType("bigint") long y1, @SqlType("bigint") long x2, @SqlType("bigint") long y2)
    {
        RowType rowType = new RowType(ImmutableList.of(BIGINT, BIGINT), Optional.of(ImmutableList.of("x", "y")));
        long width = Math.max(0, x2 - x1);
        long height = Math.max(0, y2 - y1);
        BlockBuilder arrayBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), (int) (height * width));
        for (long y = y1; y < y2; ++y) {
            for (long x = x1; x < x2; ++x) {
                BlockBuilder blockBuilder = new InterleavedBlockBuilder(rowType.getTypeParameters(), new BlockBuilderStatus(), 2);
                blockBuilder.writeLong(x).closeEntry();
                blockBuilder.writeLong(y).closeEntry();
                rowType.writeObject(arrayBuilder, blockBuilder.build());
            }
        }
        return arrayBuilder.build();
    }
}
