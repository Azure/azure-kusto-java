// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import java.util.concurrent.atomic.AtomicLong

/**
 * A thread-safe wrapper around a list of containers that provides round-robin
 * selection across all clients sharing the same list instance.
 *
 * The counter is stored within this class, so all uploaders sharing the same
 * ConfigurationCache (and thus the same RoundRobinContainerList instances) will
 * distribute their uploads evenly across containers.
 *
 * For example, if uploader A and uploader B share the same cache:
 * - Uploader A's first upload goes to container 0
 * - Uploader B's first upload goes to container 1
 * - Uploader A's second upload goes to container 2
 * - etc.
 *
 * This is similar to C# implementation's MonitoredContainerCyclicEnumerator.
 *
 * @param containers The list of containers to cycle through
 */
class RoundRobinContainerList(
    private val containers: List<ExtendedContainerInfo>,
) : List<ExtendedContainerInfo> by containers {

    /**
     * Atomic counter for round-robin container selection. Uses Long to handle
     * high-frequency uploads without overflow concerns. Initialized to -1 so
     * the first getAndIncrement returns 0.
     */
    private val currentIndex = AtomicLong(-1)

    /**
     * Gets the next starting index for round-robin container selection. Each
     * call returns the next index in sequence, wrapping around.
     *
     * @return The next index to start from (0 to size-1)
     */
    fun getNextStartIndex(): Int {
        if (containers.isEmpty()) {
            return 0
        }
        // Use Math.floorMod to handle potential negative values from overflow
        return Math.floorMod(
            currentIndex.incrementAndGet(),
            containers.size.toLong(),
        )
            .toInt()
    }

    /**
     * Creates a copy of the underlying container list. This is useful when the
     * list needs to be passed to APIs that don't support
     * RoundRobinContainerList.
     */
    fun toList(): List<ExtendedContainerInfo> = containers.toList()

    companion object {
        /** Creates an empty RoundRobinContainerList. */
        fun empty(): RoundRobinContainerList =
            RoundRobinContainerList(emptyList())

        /**
         * Creates a RoundRobinContainerList from a list of
         * ExtendedContainerInfo.
         */
        fun of(
            containers: List<ExtendedContainerInfo>,
        ): RoundRobinContainerList = RoundRobinContainerList(containers)
    }
}
