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
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;

import javax.annotation.Nullable;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public final class TableFunctions
{
    private TableFunctions() {}

    @Description("tsequence")
    @ScalarFunction
    @Nullable
    @SqlType(tableType={"x bigint"})
    public static Block tsequence(@SqlType("bigint") long a, @SqlType("bigint") long b)
    {
        TableGenerator tg = new TableGenerator(BIGINT);
        for (long i = a; i < b; ++i) {
            tg.addRow(i);
        }
        return tg.buildBlock();
    }

    // example call:
    // select * from (select x,y from unnest(tgrid(1,5,10,20)) as t(tmp1), unnest(tmp1));
    @Description("tgrid")
    @ScalarFunction
    @Nullable
    @SqlType(tableType = {"x bigint", "y bigint"})
    public static Block tgrid(@SqlType("bigint") long x1, @SqlType("bigint") long y1, @SqlType("bigint") long x2, @SqlType("bigint") long y2)
    {
        TableGenerator tg = new TableGenerator(BIGINT, BIGINT);
        for (long y = y1; y < y2; ++y) {
            for (long x = x1; x < x2; ++x) {
                tg.addRow(x, y);
            }
        }
        return tg.buildBlock();
    }
}
