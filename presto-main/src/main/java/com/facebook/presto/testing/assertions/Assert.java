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
package com.facebook.presto.testing.assertions;

public class Assert extends org.testng.Assert
{
    private Assert()
    {
    }

    public static void assertEquals(Iterable<?> actual, Iterable<?> expected)
    {
        assertEquals(actual, expected, null);
    }

    public static void assertEquals(Iterable<?> actual, Iterable<?> expected, String message)
    {
        try {
            //do a full, equals-based check first
            org.testng.Assert.assertEquals((Object) actual, (Object) expected, message);
        }
        catch (AssertionError error) {
            //do the check again using Iterable-dedicated variant for a better error message.
            org.testng.Assert.assertEquals(actual, expected, message);
            //if we're here, the Iterables differ on their fields. Use the original error message.
            throw error;
        }
    }
}
