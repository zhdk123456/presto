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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static java.util.Objects.requireNonNull;

public class JoinHashSupplier
        implements LookupSourceSupplier
{
    private final ConnectorSession session;
    private final PagesHash pagesHash;
    private final LongArrayList addresses;
    private final List<List<Block>> channels;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final PositionLinks.Builder positionLinks;

    public JoinHashSupplier(
            ConnectorSession session,
            PagesHashStrategy pagesHashStrategy,
            LongArrayList addresses,
            List<List<Block>> channels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory)
    {
        requireNonNull(session, "session is null");
        requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        requireNonNull(addresses, "addresses is null");
        requireNonNull(channels, "channels is null");
        requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");

        if (filterFunctionFactory.isPresent() && filterFunctionFactory.get().getSortChannel().isPresent()) {
            this.positionLinks = SortedPositionLinks.builder(
                    addresses.size(),
                    new PositionComparator(
                            pagesHashStrategy,
                            addresses,
                            filterFunctionFactory.get().getSortChannel().get().isDescending()));
        }
        else {
            this.positionLinks = PositionLinksList.builder(addresses.size());
        }

        this.session = session;
        this.pagesHash = new PagesHash(addresses, pagesHashStrategy, positionLinks);
        this.addresses = addresses;
        this.channels = channels;
        this.filterFunctionFactory = filterFunctionFactory;
    }

    @Override
    public long getHashCollisions()
    {
        return pagesHash.getHashCollisions();
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return pagesHash.getExpectedHashCollisions();
    }

    @Override
    public JoinHash get()
    {
        Optional<JoinFilterFunction> filterFunction = filterFunctionFactory.map(factory -> factory.create(session, addresses, channels));
        if (filterFunction.isPresent()) {
            positionLinks.setFilterFunction(filterFunction.get());
        }
        return new JoinHash(pagesHash, filterFunction, positionLinks.build());
    }

    public static class PositionComparator
            implements Comparator<Integer>
    {
        private PagesHashStrategy pagesHashStrategy;
        private LongArrayList addresses;
        private boolean descending;

        public PositionComparator(PagesHashStrategy pagesHashStrategy, LongArrayList addresses, boolean descending)
        {
            this.pagesHashStrategy = pagesHashStrategy;
            this.addresses = addresses;
            this.descending = descending;
        }

        @Override
        public int compare(Integer leftPosition, Integer rightPosition)
        {
            long leftPageAddress = addresses.getLong(leftPosition);
            int leftBlockIndex = decodeSliceIndex(leftPageAddress);
            int leftBlockPosition = decodePosition(leftPageAddress);

            long rightPageAddress = addresses.getLong(rightPosition);
            int rightBlockIndex = decodeSliceIndex(rightPageAddress);
            int rightBlockPosition = decodePosition(rightPageAddress);

            if (descending) {
                return pagesHashStrategy.compare(rightBlockIndex, rightBlockPosition, leftBlockIndex, leftBlockPosition);
            }
            else {
                return pagesHashStrategy.compare(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
            }
        }
    }
}
