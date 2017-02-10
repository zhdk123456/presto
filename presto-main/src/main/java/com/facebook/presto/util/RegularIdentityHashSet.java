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

import com.google.common.collect.ForwardingSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Set;

import static java.util.Objects.requireNonNull;

final class RegularIdentityHashSet<T>
        extends ForwardingSet<T>
        implements IdentityHashSet<T>
{
    private final Set<T> delegate = Sets.newIdentityHashSet();

    public RegularIdentityHashSet()
    {
    }

    public RegularIdentityHashSet(Iterable<? extends T> elements)
    {
        Iterables.addAll(delegate, requireNonNull(elements, "elements cannot be null"));
    }

    @Override
    protected Set<T> delegate()
    {
        return delegate;
    }
}
