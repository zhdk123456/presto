package com.facebook.presto.row;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;

public abstract class RowObject
{
    public abstract void appendTo(PageBuilder pageBuilder);

    public abstract long hash(int id);

    public abstract boolean equals(int id, long longValue);

    public abstract void set(int id, int blockPosition, Block block0, Block block1, Block block2);
}
