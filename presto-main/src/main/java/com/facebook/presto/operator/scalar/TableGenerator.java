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
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.RowType;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Float.floatToRawIntBits;

public class TableGenerator
{
    private final RowType rowType;
    private final List<List<Object>> table;

    public TableGenerator(Type... types)
    {
        rowType = new RowType(Arrays.asList(types), Optional.empty());
        table = new ArrayList<>();
    }

    public void addRow(Object... objects)
    {
        assert objects.length == rowType.getTypeParameters().size();
        for (int i = 0; i < objects.length; ++i) {
            if (objects[i] instanceof String) {
                assert rowType.getTypeParameters().get(i).equals(VarcharType.VARCHAR);
            } else if (objects[i] instanceof Float) {
                assert rowType.getTypeParameters().get(i).equals(RealType.REAL);
            } else if (objects[i] instanceof Integer) {
                assert rowType.getTypeParameters().get(i).equals(IntegerType.INTEGER);
            } else {
                assert Primitives.wrap(rowType.getTypeParameters().get(i).getJavaType()).isInstance(objects[i]);
            }
        }

        table.add(Arrays.asList(objects));
    }

    public Block buildBlock()
    {
        int cols = rowType.getTypeParameters().size();
        int rows = table.size();
        int entries =  rows * cols;
        BlockBuilder arrayBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), entries);
        for (List<Object> row : table) {
            BlockBuilder bb = new InterleavedBlockBuilder(rowType.getTypeParameters(), new BlockBuilderStatus(), cols);
            for (Object obj : row) {
                if (obj instanceof Long) {
                    bb.writeLong((Long) obj);
                }
                else if (obj instanceof Integer) {
                    bb.writeInt((Integer) obj);
                }
                else if (obj instanceof Double) {
                    bb.writeLong(doubleToRawLongBits((Double) obj));
                }
                else if (obj instanceof Float) {
                    bb.writeInt(floatToRawIntBits((Float) obj));
                }
                else if (obj instanceof String) {
                    bb.writeBytes(Slices.utf8Slice((String) obj), 0, ((String) obj).length());
                }
                else {
                    assert false;
                }
                bb.closeEntry();
            }
            rowType.writeObject(arrayBuilder, bb.build());
        }
        return arrayBuilder.build();
    }
}
