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
package com.facebook.presto.util.maps;

import com.google.common.base.Equivalence;
import org.intellij.lang.annotations.Flow;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Map decorator allowing for overriding the equivalence relation
 * for the underlying Map's keys.
 * <p>
 * EquivalenceMap instances retain iteration order of the underlying Map.
 * Collections returned from #entrySet() and #keySet() are unmodifiable
 * views.
 *
 * @param <K>
 * @param <V>
 */
public class EquilvalenceMap<K, V>
        implements Map<K, V>
{
    private final Map<Equivalence.Wrapper<K>, V> delegate;
    private final Equivalence<Object> equivalenceRelation;

    public static <K, V> EquilvalenceMap<K, V> on(MapSupplier supplier, Equivalence<Object> equilvalenceRelation)
    {
        return new EquilvalenceMap<>(supplier, equilvalenceRelation);
    }

    protected EquilvalenceMap(MapSupplier supplier, Equivalence<Object> equivalenceRelation)
    {
        this.delegate = supplier.supply();
        checkArgument(delegate.isEmpty(), "The provided supplier suplied a non-empty Map");
        this.equivalenceRelation = equivalenceRelation;
    }

    @Override
    public int size()
    {
        return delegate.size();
    }

    @Override
    public boolean isEmpty()
    {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return delegate.containsKey(equivalenceRelation.wrap(key));
    }

    @Override
    public boolean containsValue(Object value)
    {
        return delegate.containsValue(value);
    }

    @Override
    public V get(Object key)
    {
        return delegate.get(equivalenceRelation.wrap(key));
    }

    @Override
    public V put(@Flow(target = "this.keys", targetIsContainer = true) K key, @Flow(target = "this.values", targetIsContainer = true) V value)
    {
        return delegate.put(equivalenceRelation.wrap(key), value);
    }

    @Override
    public V remove(Object key)
    {
        return delegate.remove(equivalenceRelation.wrap(key));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map)
    {
        map.entrySet().forEach(e -> {
            delegate.put(equivalenceRelation.wrap(e.getKey()), e.getValue());
        });
    }

    @Override
    public void clear()
    {
        delegate.clear();
    }

    @Override
    public Set<K> keySet()
    {
        return new AbstractSet<K>()
        {
            @Override
            public Iterator<K> iterator()
            {
                return delegate.keySet().stream().map(Equivalence.Wrapper::get).iterator();
            }

            @Override
            public int size()
            {
                return delegate.size();
            }
        };
    }

    @Override
    public Collection<V> values()
    {
        return delegate.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return new AbstractSet<Entry<K, V>>()
        {
            @Override
            public Iterator<Entry<K, V>> iterator()
            {
                return delegate.entrySet().stream().map(e -> {
                    K key = e.getKey().get();
                    return (Entry<K, V>) new AbstractMap.SimpleEntry<>(key, e.getValue());
                }).iterator();
            }

            @Override
            public int size()
            {
                return delegate.size();
            }
        };
    }
}
