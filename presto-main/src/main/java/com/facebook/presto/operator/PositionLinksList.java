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

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public final class PositionLinksList
        implements PositionLinks
{
    public static class Builder implements PositionLinks.Builder
    {
        private final int[] positionLinks;

        private Builder(int size)
        {
            positionLinks = new int[size];
            Arrays.fill(positionLinks, -1);
        }

        @Override
        public void link(int from, int to)
        {
            positionLinks[from] = to;
        }

        @Override
        public PositionLinksList build()
        {
            return new PositionLinksList(positionLinks);
        }

        @Override
        public void setFilterFunction(JoinFilterFunction filterFunction)
        {
        }
    }

    private final int[] positionLinks;

    private PositionLinksList(int[] positionLinks)
    {
        this.positionLinks = requireNonNull(positionLinks, "positionLinks is null");
    }

    public static Builder builder(int size)
    {
        return new Builder(size);
    }

    @Override
    public int next(int position, int probePosition, Page allProbeChannelsPage)
    {
        return positionLinks[position];
    }

    @Override
    public int start(int position, int probePosition, Page allProbeChannelsPage)
    {
        return positionLinks[position];
    }

    public int size()
    {
        return positionLinks.length;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeOf(positionLinks);
    }
}
