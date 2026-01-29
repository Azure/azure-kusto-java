// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.util.concurrent.ConcurrentHashMap

/**
 * Tests for RoundRobinContainerList to ensure proper container cycling behavior
 * and thread-safety when multiple uploaders share the same list.
 */
@Execution(ExecutionMode.CONCURRENT)
class RoundRobinContainerListTest {

    private fun createContainers(count: Int): List<ExtendedContainerInfo> =
        (0 until count).map { i ->
            ExtendedContainerInfo(
                ContainerInfo("https://storage$i.blob.core.windows.net/container$i"),
                UploadMethod.STORAGE
            )
        }

    @Test
    fun `empty list returns isEmpty true`() {
        val list = RoundRobinContainerList.empty()
        assertTrue(list.isEmpty())
        assertEquals(0, list.size)
    }

    @Test
    fun `non-empty list returns isEmpty false`() {
        val containers = createContainers(3)
        val list = RoundRobinContainerList.of(containers)
        assertFalse(list.isEmpty())
        assertEquals(3, list.size)
    }

    @Test
    fun `getNextStartIndex cycles through all containers`() {
        val containers = createContainers(3)
        val list = RoundRobinContainerList.of(containers)

        // Should cycle through 0, 1, 2, 0, 1, 2, ...
        assertEquals(0, list.getNextStartIndex())
        assertEquals(1, list.getNextStartIndex())
        assertEquals(2, list.getNextStartIndex())
        assertEquals(0, list.getNextStartIndex())
        assertEquals(1, list.getNextStartIndex())
        assertEquals(2, list.getNextStartIndex())
    }

    @Test
    fun `index access returns correct container`() {
        val containers = createContainers(3)
        val list = RoundRobinContainerList.of(containers)

        assertEquals(containers[0], list[0])
        assertEquals(containers[1], list[1])
        assertEquals(containers[2], list[2])
    }

    @Test
    fun `iterator returns all containers`() {
        val containers = createContainers(3)
        val list = RoundRobinContainerList.of(containers)

        val iterated = list.toList()
        assertEquals(containers, iterated)
    }

    @Test
    fun `single container list always returns index 0`() {
        val containers = createContainers(1)
        val list = RoundRobinContainerList.of(containers)

        // With only 1 container, all calls should return 0
        assertEquals(0, list.getNextStartIndex())
        assertEquals(0, list.getNextStartIndex())
        assertEquals(0, list.getNextStartIndex())
    }

    @Test
    fun `multiple uploaders sharing same list get different start indices`() {
        // Simulate multiple uploaders (A, B, C) sharing the same list
        val containers = createContainers(5)
        val list = RoundRobinContainerList.of(containers)

        // Simulate sequential uploads from different uploaders
        val uploaderAIndex1 = list.getNextStartIndex() // Uploader A's 1st upload
        val uploaderBIndex1 = list.getNextStartIndex() // Uploader B's 1st upload
        val uploaderCIndex1 = list.getNextStartIndex() // Uploader C's 1st upload
        val uploaderAIndex2 = list.getNextStartIndex() // Uploader A's 2nd upload
        val uploaderBIndex2 = list.getNextStartIndex() // Uploader B's 2nd upload

        // Each should get a different starting index (cycling)
        assertEquals(0, uploaderAIndex1)
        assertEquals(1, uploaderBIndex1)
        assertEquals(2, uploaderCIndex1)
        assertEquals(3, uploaderAIndex2)
        assertEquals(4, uploaderBIndex2)
    }

    @Test
    fun `concurrent access produces even distribution`() = runBlocking {
        val containers = createContainers(4)
        val list = RoundRobinContainerList.of(containers)

        // Track which containers are selected
        val containerSelectionCounts = ConcurrentHashMap<Int, Int>()
        (0..3).forEach { containerSelectionCounts[it] = 0 }

        // Simulate 1000 concurrent uploads
        val numConcurrentUploads = 1000
        val jobs = (0 until numConcurrentUploads).map {
            async(Dispatchers.Default) {
                val index = list.getNextStartIndex()
                containerSelectionCounts.compute(index) { _, count -> (count ?: 0) + 1 }
            }
        }
        jobs.awaitAll()

        // Verify distribution is even (each container should have ~250 selections)
        val expectedPerContainer = numConcurrentUploads / containers.size
        val tolerance = expectedPerContainer * 0.1 // 10% tolerance for timing variations

        containerSelectionCounts.forEach { (index, count) ->
            assertTrue(
                count >= expectedPerContainer - tolerance && count <= expectedPerContainer + tolerance,
                "Container $index was selected $count times, expected around $expectedPerContainer"
            )
        }

        // Total should equal numConcurrentUploads
        assertEquals(numConcurrentUploads, containerSelectionCounts.values.sum())
    }

    @Test
    fun `counter handles integer overflow gracefully`() {
        val containers = createContainers(3)
        val list = RoundRobinContainerList.of(containers)

        // Call getNextStartIndex many times to verify it doesn't break
        // We can't easily test Int.MAX_VALUE overflow, but we can verify stability
        repeat(10000) {
            val index = list.getNextStartIndex()
            assertTrue(index in 0..2, "Index $index should be in range 0..2")
        }
    }

    @Test
    fun `forEach iterates over all containers`() {
        val containers = createContainers(3)
        val list = RoundRobinContainerList.of(containers)

        val visited = mutableListOf<ExtendedContainerInfo>()
        list.forEach { visited.add(it) }

        assertEquals(containers, visited)
    }

    @Test
    fun `isNotEmpty returns correct value`() {
        val emptyList = RoundRobinContainerList.empty()
        val nonEmptyList = RoundRobinContainerList.of(createContainers(2))

        assertFalse(emptyList.isNotEmpty())
        assertTrue(nonEmptyList.isNotEmpty())
    }

    @Test
    fun `different list instances have independent counters`() {
        val containers1 = createContainers(3)
        val containers2 = createContainers(3)

        val list1 = RoundRobinContainerList.of(containers1)
        val list2 = RoundRobinContainerList.of(containers2)

        // Advance list1's counter
        list1.getNextStartIndex() // 0
        list1.getNextStartIndex() // 1
        list1.getNextStartIndex() // 2

        // list2's counter should still be at 0
        assertEquals(0, list2.getNextStartIndex())
        
        // Verify list1 continues from where it left off
        assertEquals(0, list1.getNextStartIndex()) // wraps back to 0
    }

    @Test
    fun `shared list between multiple simulated uploaders - verifies fix`() {
        // This test specifically validates the fix for the reported issue:
        // "if uploader A and uploader B (sharing the same configurationcache) upload,
        // they will both use storage 0 first"
        
        val containers = createContainers(4)
        val sharedList = RoundRobinContainerList.of(containers)

        // Simulate Uploader A's first upload
        val uploaderAFirst = sharedList.getNextStartIndex()
        assertEquals(0, uploaderAFirst, "Uploader A's first upload should use container 0")

        // Simulate Uploader B's first upload (SHOULD use container 1, not 0!)
        val uploaderBFirst = sharedList.getNextStartIndex()
        assertEquals(1, uploaderBFirst, "Uploader B's first upload should use container 1, NOT 0")

        // Simulate Uploader A's second upload
        val uploaderASecond = sharedList.getNextStartIndex()
        assertEquals(2, uploaderASecond, "Uploader A's second upload should use container 2")

        // Simulate Uploader B's second upload
        val uploaderBSecond = sharedList.getNextStartIndex()
        assertEquals(3, uploaderBSecond, "Uploader B's second upload should use container 3")

        // Back to container 0
        val uploaderCFirst = sharedList.getNextStartIndex()
        assertEquals(0, uploaderCFirst, "Uploader C's first upload should wrap back to container 0")
    }
}