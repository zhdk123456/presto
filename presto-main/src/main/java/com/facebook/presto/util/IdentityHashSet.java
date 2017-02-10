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
package com.facebook.presto.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A {@link Set} comparing elements by identity.
 */
public interface IdentityHashSet<T>
        extends Collection<T>
{
    static <T> IdentityHashSet<T> create()
    {
        return new RegularIdentityHashSet<>();
    }

    static <T> IdentityHashSet<T> create(T... elements)
    {
        return create(Arrays.asList(elements));
    }

    static <T> IdentityHashSet<T> create(Iterable<? extends T> elements)
    {
        return new RegularIdentityHashSet<>(elements);
    }

    /**
     * Returns an identity-based set view of the keys contained in this map.
     */
    static <K> IdentityHashSet<K> keySetOf(IdentityHashMap<K, ?> map)
    {
        return new IdentitySetView<K>(requireNonNull(map, "map cannot be null").keySet());
    }
}
