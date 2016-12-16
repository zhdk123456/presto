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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;

/**
 * Scalar function that returns true only for first call during operator instance lifecycle.
 */
@ScalarFunction(value = "produce_true_once", hidden = true, deterministic = false)
public class ProduceTrueOnce
{
    private boolean firstCall = true;

    @SqlType(BOOLEAN)
    public boolean produceTrueOnce()
    {
        if (firstCall) {
            firstCall = false;
            return true;
        }
        return false;
    }
}
