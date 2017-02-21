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

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class SharedPartitionedMemoryContext
{
    private int reservations = 0;
    private Optional<Integer> partitionKey;
    private Optional<Long> inMemorySizeInBytes;
    private Optional<TaskContext> taskContext;

    public synchronized void reserve(int partitionKey, OperatorContext operatorContext, long inMemorySizeInBytes)
    {
        if (this.reservations == 0) {
            this.partitionKey = Optional.of(partitionKey);
            this.inMemorySizeInBytes = Optional.of(inMemorySizeInBytes);
            TaskContext taskContext = operatorContext.getDriverContext().getPipelineContext().getTaskContext();
            this.taskContext = Optional.of(taskContext);
            operatorContext.reserveMemory(inMemorySizeInBytes);
            operatorContext.transferMemoryToTaskContext(inMemorySizeInBytes);
        }
        else {
            checkCurrentReservationPresent();
            checkArgument(this.partitionKey.get() == partitionKey && this.inMemorySizeInBytes.get() == inMemorySizeInBytes,
                    "Inconsistent memory reservation. " + getInconsistenceDescription(partitionKey, inMemorySizeInBytes));
        }
        reservations++;
    }

    public synchronized void free(int partitionKey, long inMemorySizeInBytes)
    {
        checkArgument(reservations > 0, format(
                "Inconsistent memory de-reservation. No current reservation present, " +
                "requested free for %s bytes for partition %s.",
                partitionKey,
                inMemorySizeInBytes
        ));
        checkCurrentReservationPresent();
        checkArgument(this.partitionKey.get() == partitionKey && this.inMemorySizeInBytes.get() == inMemorySizeInBytes,
                "Inconsistent memory de-reservation. " + getInconsistenceDescription(partitionKey, inMemorySizeInBytes));
        reservations--;
        if (reservations == 0) {
            this.partitionKey = null;
            this.inMemorySizeInBytes = null;
            taskContext.get().freeMemory(inMemorySizeInBytes);
            taskContext = null;
        }
    }

    public void checkCurrentReservationPresent()
    {
        checkState(this.partitionKey.isPresent() && this.inMemorySizeInBytes.isPresent(),
                "partitionKey, inMemorySizeBytes and taskContext must be present whenever reservations > 0. That's an error.");
    }

    public String getInconsistenceDescription(int partitionKey, long inMemorySizeInBytes)
    {
        return format("Expected %s bytes for partition %s, got %s bytes for partition %s. " +
                        "Current reservations count is %s. " +
                        "This most likely means the operators sharing this memory do not synchronize their reads with each other.",
                this.inMemorySizeInBytes, this.partitionKey, inMemorySizeInBytes, partitionKey, this.reservations);
    }

    @Override
    public String toString()
    {
        return "SharedPartitionedMemoryContext{" +
                "reservations=" + reservations +
                ", partitionKey=" + partitionKey +
                ", inMemorySizeInBytes=" + inMemorySizeInBytes +
                ", taskContext=" + taskContext +
                '}';
    }
}
