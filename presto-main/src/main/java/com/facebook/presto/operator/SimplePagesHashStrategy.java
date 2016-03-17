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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SimplePagesHashStrategy
        implements PagesHashStrategy
{
    private final List<Type> types;
    private final List<List<Block>> channels;
    private final List<Block[]> channelArrays;
    private final List<Integer> hashChannels;
    private final List<Block> precomputedHashChannel;
    private final Optional<JoinFilterFunction> filterFunction;

    public SimplePagesHashStrategy(List<Type> types, List<List<Block>> channels, List<Integer> hashChannels, Optional<Integer> precomputedHashChannel, Optional<JoinFilterFunction> filterFunction)
    {
        this.filterFunction = filterFunction;
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.channels = ImmutableList.copyOf(requireNonNull(channels, "channels is null"));
        channelArrays = buildChannelArrays(channels);

        checkArgument(types.size() == channels.size(), "Expected types and channels to be the same length");
        this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
        if (precomputedHashChannel.isPresent()) {
            this.precomputedHashChannel = channels.get(precomputedHashChannel.get());
        }
        else {
            this.precomputedHashChannel = null;
        }
    }

    private List<Block[]> buildChannelArrays(List<List<Block>> channels)
    {
        if (channels.isEmpty()) {
            return new ArrayList<>();
        }

        int pagesCount = channels.get(0).size();
        List<Block[]> channelArrays = new ArrayList<>();
        for (int i = 0; i < pagesCount; ++i) {
            Block[] blocks = new Block[channels.size()];
            for (int j = 0; j < channels.size(); ++j) {
                blocks[j] = channels.get(j).get(i);
            }
            channelArrays.add(blocks);
        }
        return channelArrays;
    }

    @Override
    public int getChannelCount()
    {
        return channels.size();
    }

    @Override
    public long getSizeInBytes()
    {
        return channels.stream()
                .flatMap(List::stream)
                .mapToLong(Block::getRetainedSizeInBytes)
                .sum();
    }

    @Override
    public void appendTo(int blockIndex, int position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int i = 0; i < channels.size(); i++) {
            Type type = types.get(i);
            List<Block> channel = channels.get(i);
            Block block = channel.get(blockIndex);
            type.appendTo(block, position, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }

    @Override
    public int hashPosition(int blockIndex, int position)
    {
        if (precomputedHashChannel != null) {
            return (int) BIGINT.getLong(precomputedHashChannel.get(blockIndex), position);
        }
        int result = 0;
        for (int hashChannel : hashChannels) {
            Type type = types.get(hashChannel);
            Block block = channels.get(hashChannel).get(blockIndex);
            result = result * 31 + TypeUtils.hashPosition(type, block, position);
        }
        return result;
    }

    @Override
    public int hashRow(int position, Block... blocks)
    {
        int result = 0;
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block block = blocks[i];
            result = result * 31 + TypeUtils.hashPosition(type, block, position);
        }
        return result;
    }

    @Override
    public boolean rowEqualsRow(int leftPosition, Block[] leftBlocks, int rightPosition, Block[] rightBlocks)
    {
        checkState(!filterFunction.isPresent(), "filterFunction not supported for rowEqualsRow");
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = leftBlocks[i];
            Block rightBlock = rightBlocks[i];
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Block[] rightHashBlocks)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = rightHashBlocks[i];
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Page page, int[] rightHashChannels)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = page.getBlock(rightHashChannels[i]);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int hashChannel : hashChannels) {
            Type type = types.get(hashChannel);
            List<Block> channel = channels.get(hashChannel);
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!TypeUtils.positionEqualsPosition(type, leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Optional<JoinFilterFunction> getFilterFunction()
    {
        return filterFunction;
    }

    @Override
    public boolean applyFilterFunction(int leftBlockIndex, int leftPosition, int rightPosition, Block[] allRightBlocks)
    {
        return filterFunction.get().filter(leftPosition, channelArrays.get(leftBlockIndex), rightPosition, allRightBlocks);
    }
}
