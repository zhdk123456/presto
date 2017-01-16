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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class SortedPositionLinks
        implements PositionLinks
{
    public static class Builder implements PositionLinks.Builder
    {
        private final Map<Integer, List<Integer>> positionLinks;
        private final int size;
        private final Comparator<Integer> comparator;
        private JoinFilterFunction filterFunction;

        public Builder(int size, Comparator<Integer> comparator)
        {
            this.comparator = comparator;
            this.size = size;
            positionLinks = new HashMap<>();
        }

        @Override
        public void link(int from, int to)
        {
            List<Integer> links = positionLinks.computeIfAbsent(to, key -> new ArrayList<>());
            links.add(to);
            positionLinks.put(from, links);
            positionLinks.remove(to);
        }

        @Override
        public void setFilterFunction(JoinFilterFunction filterFunction)
        {
            this.filterFunction = filterFunction;
        }

        @Override
        public SortedPositionLinks build()
        {
            PositionLinksList.Builder builder = PositionLinksList.builder(size);
            int[][] compactedPositionLinks = new int[size][];

            for (Map.Entry<Integer, List<Integer>> entry : positionLinks.entrySet()) {
                Integer key = entry.getKey();
                List<Integer> positions = entry.getValue();
                Collections.sort(positions, comparator);

                compactedPositionLinks[key] = new int[positions.size()];
                for (int i = 0; i < positions.size(); i++) {
                    compactedPositionLinks[key][i] = positions.get(i);
                }

                for (int i = positions.size() - 2; i >= 0; i--) {
                    builder.link(positions.get(i), positions.get(i + 1));
                }
            }

            return new SortedPositionLinks(
                    builder.build(),
                    compactedPositionLinks,
                    filterFunction);
        }
    }

    private final PositionLinks positionLinks;
    private final int[][] compactedPositionLinks;

    private JoinFilterFunction filterFunction;

    private SortedPositionLinks(PositionLinksList positionLinks, int[][] compactedPositionLinks, JoinFilterFunction filterFunction)
    {
        this.positionLinks = requireNonNull(positionLinks, "positionLinks is null");
        this.compactedPositionLinks = requireNonNull(compactedPositionLinks, "compactedPositionLinks is null");
        this.filterFunction = requireNonNull(filterFunction, "filterFunction is null");
    }

    public static Builder builder(int size, Comparator<Integer> comparator)
    {
        return new Builder(size, comparator);
    }

    @Override
    public int next(int position, int probePosition, Page allProbeChannelsPage)
    {
        int nextPosition = positionLinks.next(position, probePosition, allProbeChannelsPage);
        if (nextPosition < 0) {
            return -1;
        }
        if (applyFilterFilterFunction(nextPosition, probePosition, allProbeChannelsPage)) {
            return nextPosition;
        }
        return -1;
    }

    @Override
    public int start(int startingPosition, int probePosition, Page allProbeChannelsPage)
    {
        if (compactedPositionLinks[startingPosition] == null) {
            return -1;
        }

        int left = 0;
        int right = compactedPositionLinks[startingPosition].length - 1;

        int offset = lowerBound(startingPosition, left, right, probePosition, allProbeChannelsPage);
        if (offset < 0) {
            return -1;
        }
        if (!applyFilterFilterFunction(startingPosition, offset, probePosition, allProbeChannelsPage)) {
            return -1;
        }
        return compactedPositionLinks[startingPosition][offset];
    }

    private int lowerBound(int startingPosition, int first, int last, int probePosition, Page allProbeChannelsPage)
    {
        int it;
        int step;
        int count = last - first;
        while (count > 0) {
            it = first;
            step = count / 2;
            it += step;
            if (!applyFilterFilterFunction(startingPosition, it, probePosition, allProbeChannelsPage)) {
                first = ++it;
                count -= step + 1;
            }
            else {
                count = step;
            }
        }
        return first;
    }

    @Override
    public long getSizeInBytes()
    {
        return 0;
    }

    private boolean applyFilterFilterFunction(int leftPosition, int leftOffset, int rightPosition, Page rightPage)
    {
        return applyFilterFilterFunction(compactedPositionLinks[leftPosition][leftOffset], rightPosition, rightPage);
    }

    private boolean applyFilterFilterFunction(long leftPosition, int rightPosition, Page rightPage)
    {
        return filterFunction.filter((int) leftPosition, rightPosition, rightPage);
    }
}
