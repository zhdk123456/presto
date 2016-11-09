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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class EquivalenceMapTest
{
    @Test
    public void testUsesProvidedEquivalenceForKeys()
    {
        Equivalence<Object> toStringEqualLength = Equivalence.equals().onResultOf(o -> o.toString().length());
        EquivalenceMap<String, Integer> map = EquivalenceMap.on(LinkedHashMap::new, toStringEqualLength);

        assertTrue(map.isEmpty());

        map.put("foo", 1);
        assertEquals(map, ImmutableMap.of("foo", 1));
        assertTrue(map.containsKey("foo"));
        assertTrue(map.containsKey("bar"));
        assertTrue(map.containsValue(1));

        map.put("bar", 2);
        assertEquals(map.get("baz"), Integer.valueOf(2));
        assertEquals(ImmutableMap.of("bar", 2), map);
        assertNotEquals(map, ImmutableMap.of("bar", 2));

        map.remove("qux");
        assertTrue(map.isEmpty());

        Set<String> keys = ImmutableSet.of("a", "b", "aa", "aaa", "bbb");
        Map<String, Integer> expectedMap = ImmutableMap.of("b", 1, "aa", 2, "bbb", 3);

        map.putAll(Maps.asMap(keys, String::length));
        assertEquals(expectedMap, map);
        assertNotEquals(map, expectedMap);

        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testRetainsUnderlyingMapsIterationOrder()
    {
        assertStableIterationOrderWithUnderlyingMap(LinkedHashMap::new);
        try {
            assertStableIterationOrderWithUnderlyingMap(HashMap::new);
        }
        catch (AssertionError error) {
            assertTrue(error.getMessage().contains("Lists differ at element"));
        }
    }

    private void assertStableIterationOrderWithUnderlyingMap(MapSupplier mapSupplier)
    {
        Set<String> keys = ImmutableSet.of("All", "your", "base", "are", "belong", "to", "us");
        Map<String, Integer> expectedMap = Maps.asMap(keys, String::length);

        range(0, 10).forEach(attempt -> {
            EquivalenceMap<String, Integer> map = EquivalenceMap.on(mapSupplier, Equivalence.equals());

            keys.forEach(i -> map.put(i, i.length()));

            assertEquals(map.keySet(), keys);
            assertEquals(map.values(), expectedMap.values());
            assertEquals(map.entrySet(), expectedMap.entrySet());
        });
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "SortedMap delegates are not supported.*")
    public void doesNotAcceptSortedMapsAsDelegates()
    {
        EquivalenceMap.on(TreeMap::new, Equivalence.identity());
    }
}
