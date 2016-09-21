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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

@Description("Calls defined R function for given parameters")
@ScalarFunction("R")
public final class RFunctions
{
    private RFunctions() {}

    @SqlType(StandardTypes.VARCHAR)
    @TypeParameter("T")
    public static Slice typeof(@SqlType("VARCHAR") Slice functionCode, @SqlType("T") Slice value)
    {
        RConnection c = null;
        try {
            c = new RConnection();
            REXP x = c.eval("R.version.string");
            return Slices.wrappedBuffer(x.asString().getBytes());
        }
        catch (RserveException e) {
            e.printStackTrace();
        }
        catch (REXPMismatchException e) {
            e.printStackTrace();
        }

        return Slices.wrappedBuffer("".getBytes());
    }

    @SqlType(StandardTypes.VARCHAR)
    @TypeParameter("T")
    public static Slice typeof(@SqlType("VARCHAR") Slice functionCode, @SqlType("T") long value)
    {
        String result = "xxx";
        return Slices.wrappedBuffer(result.getBytes());
    }

    @SqlType(StandardTypes.VARCHAR)
    @TypeParameter("T")
    public static Slice typeof(@SqlType("VARCHAR") Slice functionCode, @SqlType("T") double value)
    {
        try {
            Page page;

            RConnection c = new RConnection();
            double[] input = { value };
            c.assign("x", input);
            c.eval(functionCode.toStringUtf8());
            double[] result = c.eval("fun(x)").asDoubles();
            return Slices.wrappedBuffer(Double.toString(result[0]).getBytes());
        }
        catch (RserveException e) {
            e.printStackTrace();
        }
        catch (REXPMismatchException e) {
            e.printStackTrace();
        }
        catch (REngineException e) {
            e.printStackTrace();
        }

        String result = "xxx";
        return Slices.wrappedBuffer(result.getBytes());
    }

    @SqlType(StandardTypes.VARCHAR)
    @TypeParameter("T")
    public static Slice typeof(@SqlType("VARCHAR") Slice functionCode, @SqlType("T") boolean value)
    {
        String result = "xxx";
        return Slices.wrappedBuffer(result.getBytes());
    }
}
