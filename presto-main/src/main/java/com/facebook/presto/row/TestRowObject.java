package com.facebook.presto.row;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;

public class TestRowObject
        extends RowObject
{
    public Slice col0;
    public long col1;
    public long col2;
    public Slice col3;
    public long col4;
    public long col5;
    public Slice col6;
    public long col7;
    public long col8;

    @Override
    public void appendTo(PageBuilder pageBuilder)
    {
        pageBuilder.declarePosition();
        pageBuilder.getBlockBuilder(0).writeBytes(col0, 0, col0.length());
        pageBuilder.getBlockBuilder(0).closeEntry();
        pageBuilder.getBlockBuilder(1).writeLong(col1);
        pageBuilder.getBlockBuilder(2).writeLong(col2);

        pageBuilder.getBlockBuilder(3).writeBytes(col3, 0, col3.length());
        pageBuilder.getBlockBuilder(3).closeEntry();
        pageBuilder.getBlockBuilder(4).writeLong(col4);
        pageBuilder.getBlockBuilder(5).writeLong(col5);

        pageBuilder.getBlockBuilder(6).writeBytes(col6, 0, col6.length());
        pageBuilder.getBlockBuilder(6).closeEntry();
        pageBuilder.getBlockBuilder(7).writeLong(col7);
        pageBuilder.getBlockBuilder(8).writeLong(col8);
    }

    public void set(int id, int blockPosition, Block block0, Block block1, Block block2)
    {
        Slice slice = VarcharType.VARCHAR.getSlice(block0, blockPosition);
        long aLong0 = BigintType.BIGINT.getLong(block1, blockPosition);
        long aLong1 = BigintType.BIGINT.getLong(block2, blockPosition);
        switch (id) {
            case 0:
                col3 = slice;
                col4 = aLong0;
                col5 = aLong1;
                break;
            case 1:
                col6 = slice;
                col7 = aLong0;
                col8 = aLong1;
                break;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public long hash(int id)
    {
        switch (id) {
            case 0:
                return AbstractLongType.hash(col1);
            case 1:
                return AbstractLongType.hash(col1);
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public boolean equals(int id, long longValue)
    {
        switch (id) {
            case 0:
                return longValue == col1;
            case 1:
                return longValue == col1;
            default:
                throw new IllegalStateException();
        }
    }
}
