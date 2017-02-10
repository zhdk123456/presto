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

import com.google.common.collect.ForwardingMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Collection;
import java.util.IdentityHashMap;

/**
 * A {@link Multimap} which uses identity-based comparison for keys and values.
 */
public final class IdentityMultimap<K, V>
        extends ForwardingMultimap<K, V>
{
    private final Multimap<K, V> delegate = Multimaps.newMultimap(
            new IdentityHashMap<K, Collection<V>>(),
            IdentityHashSet::create);

    @Override
    public IdentitySetView<K> keySet()
    {
        return new IdentitySetView<K>(super.keySet());
    }

    @Override
    protected Multimap<K, V> delegate()
    {
        return delegate;
    }
}
