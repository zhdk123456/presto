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

import java.util.Date;
import java.util.Random;

import static com.facebook.presto.operator.scalar.MathFunctions.sqrt;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.pow;

public final class TableFunctions
{
    private static Random rng = new Random(new Date().getTime());

    private TableFunctions() {}

    @Description("tsequence")
    @ScalarFunction
    @Nullable
    @SqlType(tableType = {"x bigint"})
    public static Block tsequence(@SqlType("bigint") long a, @SqlType("bigint") long b)
    {
        TableGenerator tg = new TableGenerator(BIGINT);
        for (long i = a; i < b; ++i) {
            tg.addRow(i);
        }
        return tg.buildBlock();
    }

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

    @Description("pnorm")
    @ScalarFunction
    @Nullable
    @SqlType(tableType = {"x double"})
    public static Block pnorm(@SqlType("bigint") long n, @SqlType("double") double mean, @SqlType("double") double sd)
    {
        TableGenerator tg = new TableGenerator(DOUBLE);
        for (int i = 0; i < n; ++i) {
            tg.addRow(rng.nextGaussian() * sd + mean);
        }
        return tg.buildBlock();
    }

    @Description("pnorm")
    @ScalarFunction
    @Nullable
    @SqlType(tableType = {"x double", "y double"})
    public static Block ppairnorm(@SqlType("bigint") long n, @SqlType("double") double mean1, @SqlType("double") double sd1, @SqlType("double") double mean2, @SqlType("double") double sd2, @SqlType("double") double corr)
    {
        TableGenerator tg = new TableGenerator(DOUBLE, DOUBLE);
        for (int i = 0; i < n; ++i) {
            double x1 = rng.nextGaussian();
            double x2 = rng.nextGaussian();
            double y1 = mean1 + sd1 * x1;
            double y2 = mean2 + sd2 * (corr * x1 + sqrt(1 - pow(corr, 2)) * x2);
            tg.addRow(y1, y2);
        }
        return tg.buildBlock();
    }

    @Description("tall")
    @ScalarFunction
    @Nullable
    @SqlType(tableType = {"x bigint", "y integer", "z double", "a real", "b varchar", "c boolean"})
    public static Block tall()
    {
        TableGenerator tg = new TableGenerator(BIGINT, INTEGER, DOUBLE, REAL, VARCHAR, BOOLEAN);
        for (int i = 0; i < 3; ++i) {
            tg.addRow(1L + i, 2 + i, 1.0 + i, 2.0f + i, "i = " + i, i % 2 == 0);
        }
        return tg.buildBlock();
    }
}
