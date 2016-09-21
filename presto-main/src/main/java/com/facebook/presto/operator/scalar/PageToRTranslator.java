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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import java.util.Arrays;

import static com.facebook.presto.operator.scalar.RFunctions.getConnectionIgnoreException;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.longBitsToDouble;

public class PageToRTranslator
{
    private final RConnection connection = getConnectionIgnoreException();
    private static final String[] varNames = {"x", "y", "z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

    static double[] getDoubleColumn(Page p, int columnIndex)
    {
        Block b = p.getBlock(columnIndex);
        double[] result = new double[b.getPositionCount()];
        for (int i = 0; i < b.getPositionCount(); ++i) {
            result[i] = longBitsToDouble(b.getLong(i, 0));
        }

        return result;
    }

    static Page getReturnPage(double[] d)
    {
        return new Page(new LongArrayBlock(d.length, new boolean[d.length], Arrays.stream(d).mapToLong(x -> doubleToRawLongBits(x)).toArray()));
    }

    static int longToInt(long l)
    {
        if (l > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        if (l < Integer.MIN_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) l;
    }

    static int[] getLongColumn(Page p, int columnIndex)
    {
        Block b = p.getBlock(columnIndex);
        int[] result = new int[b.getPositionCount()];
        for (int i = 0; i < b.getPositionCount(); ++i) {
            result[i] = longToInt(b.getLong(i, 0));
        }

        return result;
    }

    static Page getReturnPage(int[] l)
    {
        return new Page(new LongArrayBlock(l.length, new boolean[l.length], Arrays.stream(l).mapToLong(x -> (long) x).toArray()));
    }

    static String[] getStringColumn(Page p, int columnIndex)
    {
        Block b = p.getBlock(columnIndex);
        String[] result = new String[b.getPositionCount()];
        for (int i = 0; i < b.getPositionCount(); ++i) {
            result[i] = b.getSlice(i, 0, b.getLength(i)).toStringUtf8();
        }

        return result;
    }

    static Page getReturnPage(String[] l)
    {
        Slice[] slices =  (Slice[]) Arrays.stream(l).map(x -> Slices.wrappedBuffer(x.getBytes())).toArray();
        return new Page(new SliceArrayBlock(l.length, slices, true));
    }

    public Page RCALL(String rCode, Page page, Type returnType, Type[] types)
    {
        Object[] arguments = prepareArguments(page, types);
        return processRCode(rCode, arguments, types, returnType);
    }

    public void closeConnection()
    {
        connection.close();
    }

    static Object[] prepareArguments(Page page, Type[] types)
    {
        Object[] arguments = new Object[types.length];
        for (int i = 0; i < page.getBlocks().length; ++i) {
            switch (types[i].getTypeSignature().getBase()) {
                case "VARCHAR":
                    String[] s = getStringColumn(page, i);
                    arguments[i] = s;
                    break;
                case "BIGINT":
                    int[] l = getLongColumn(page, i);
                    arguments[i] = l;
                    break;
                case "DOUBLE":
                    double[] d = getDoubleColumn(page, i);
                    arguments[i] = d;
                    break;
                default:
                    throw new RuntimeException("Types other then VARCHAR, BIGINT, DOUBLE are not supported in RCALL.");
            }
        }
        return arguments;
    }

    synchronized Page processRCode(String rCode, Object[] args, Type[] types, Type returnType)
    {
        if (varNames.length < types.length) {
            throw new RuntimeException("To many argumenst for RCALL");
        }

        try {
            connection.eval(rCode);
            assignVariables(types, args);
            String call = collectFunCall(args.length);
            return callAndPack(call, returnType);
        }
        catch (RserveException e) {
            e.printStackTrace();
        }
        catch (REngineException e) {
            e.printStackTrace();
        }

        return null;
    }

    private void assignVariables(Type[] types, Object[] args)
    {
        try {
            for (int i = 0; i < args.length; ++i) {
                switch (types[i].getTypeSignature().getBase()) {
                    case "VARCHAR":
                        connection.assign(varNames[i], (String[]) args[i]);
                        break;
                    case "BIGINT":
                        connection.assign(varNames[i], (int[]) args[i]);
                        break;
                    case "DOUBLE":
                        connection.assign(varNames[i], (double[]) args[i]);
                        break;
                    default:
                        throw new RuntimeException("Types other then VARCHAR, BIGINT, DOUBLE are not supported in RCALL.");
                }
            }
        }
        catch (REngineException e) {
            e.printStackTrace();
        }
    }

    private String collectFunCall(int length)
    {
        StringBuilder s = new StringBuilder();
        s.append("fun(");
        for (int i = 0; i < length; ++i)  {
            if (i != 0) {
                s.append(",");
            }
            s.append(varNames[i]);
        }
        s.append(")");
        return s.toString();
    }

    private Page callAndPack(String call, Type returnType)
    {
        try {
            switch (returnType.getTypeSignature().getBase()) {
                case "VARCHAR":
                    return getReturnPage(connection.eval(call).asStrings());
                case "BIGINT":
                    return getReturnPage(connection.eval(call).asIntegers());
                case "DOUBLE":
                    return getReturnPage(connection.eval(call).asDoubles());
                default:
                    throw new RuntimeException("Types other then VARCHAR, BIGINT, DOUBLE are not supported in RCALL.");
            }
        }
        catch (REngineException e) {
            e.printStackTrace();
        }
        catch (REXPMismatchException e) {
            e.printStackTrace();
        }
        return null;
    }
}
