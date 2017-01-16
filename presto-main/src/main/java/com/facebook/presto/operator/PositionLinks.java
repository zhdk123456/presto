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

/**
 * This class is responsible for iterating over build rows, which have
 * same values in hash columns as given probe row (but according to
 * filterFunction can have non matching values on some other column).
 */
public interface PositionLinks
{
    interface Builder
    {
        void link(int from, int to);

        void setFilterFunction(JoinFilterFunction filterFunction);

        PositionLinks build();
    }

    long getSizeInBytes();

    int next(int position, int probePosition, Page allProbeChannelsPage);

    int start(int position, int probePosition, Page allProbeChannelsPage);
}
