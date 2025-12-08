// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo
import com.microsoft.azure.kusto.ingest.v2.models.ContainerSettings
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import java.time.Duration

class DefaultConfigurationCacheTest {

    // create a function that returns a ConfigurationResponse
    // First return value1 then value2
    private var callCount = 0

    private suspend fun mockConfigurationProvider(): ConfigurationResponse {
        callCount++
        return if (callCount == 1) {
            ConfigurationResponse(
                containerSettings =
                ContainerSettings(
                    preferredUploadMethod = "REST",
                    containers =
                    listOf(
                        ContainerInfo(
                            path =
                            "https://example1.blob.core.windows.net/container1",
                        ),
                    ),
                ),
            )
        } else {
            ConfigurationResponse(
                containerSettings =
                ContainerSettings(
                    preferredUploadMethod = "QUEUE",
                    containers =
                    listOf(
                        ContainerInfo(
                            path =
                            "https://example2.blob.core.windows.net/container2",
                        ),
                    ),
                ),
            )
        }
    }

    @Test
    fun `when cache expires, configuration API is invoked`(): Unit =
        runBlocking {
            val refreshInterval = Duration.ofMillis(500) // 0.5 seconds
            val cache =
                DefaultConfigurationCache(
                    clientDetails = ClientDetails.createDefault(),
                    refreshInterval = refreshInterval,
                    configurationProvider =
                    ::mockConfigurationProvider,
                )

            // First call should fetch from provider
            val config1 = cache.getConfiguration()
            assertNotNull(config1)
            assertEquals(
                "REST",
                config1.containerSettings?.preferredUploadMethod,
            )
            assertEquals(1, callCount)

            // Wait less than refresh interval and call again, should return cached value
            Thread.sleep(300)
            val config2 = cache.getConfiguration()
            assertNotNull(config2)
            assertEquals(
                "REST",
                config2.containerSettings?.preferredUploadMethod,
            )
            assertEquals(
                1,
                callCount,
            ) // callCount should not have increased

            // Wait for cache to expire
            Thread.sleep(600)
            val config3 = cache.getConfiguration()
            assertNotNull(config3)
            assertEquals(
                "QUEUE",
                config3.containerSettings?.preferredUploadMethod,
            )
            assertEquals(2, callCount) // callCount should have increased

            cache.close()
        }

    @Test
    fun `configuration is automatically refreshed during client lifetime`(): Unit = runBlocking {
        // Simulate a short refresh interval for testing
        val refreshInterval = Duration.ofMillis(500)
        var fetchCount = 0

        val configurationProvider: suspend () -> ConfigurationResponse = {
            fetchCount++
            ConfigurationResponse(
                containerSettings =
                ContainerSettings(
                    preferredUploadMethod =
                    "METHOD_$fetchCount",
                    containers =
                    listOf(
                        ContainerInfo(
                            path =
                            "https://container$fetchCount.blob.core.windows.net/data",
                        ),
                    ),
                ),
            )
        }

        val cache =
            DefaultConfigurationCache(
                clientDetails = ClientDetails.createDefault(),
                refreshInterval = refreshInterval,
                configurationProvider = configurationProvider,
            )

        // First operation - should fetch fresh config
        val config1 = cache.getConfiguration()
        assertNotNull(config1)
        assertEquals(
            "METHOD_1",
            config1.containerSettings?.preferredUploadMethod,
        )
        assertEquals(1, fetchCount, "First call should fetch from provider")

        // Second operation immediately - should use cached value
        val config2 = cache.getConfiguration()
        assertEquals(
            "METHOD_1",
            config2.containerSettings?.preferredUploadMethod,
        )
        assertEquals(1, fetchCount, "Should still use cached value")

        // Wait for cache to expire
        Thread.sleep(600)

        // Third operation after expiry - should automatically refresh
        val config3 = cache.getConfiguration()
        assertNotNull(config3)
        assertEquals(
            "METHOD_2",
            config3.containerSettings?.preferredUploadMethod,
        )
        assertEquals(2, fetchCount, "Cache expired, should fetch fresh data")

        // Fourth operation immediately - should use newly cached value
        val config4 = cache.getConfiguration()
        assertEquals(
            "METHOD_2",
            config4.containerSettings?.preferredUploadMethod,
        )
        assertEquals(2, fetchCount, "Should use newly cached value")

        // Wait for cache to expire again
        Thread.sleep(600)

        // Fifth operation - should refresh again
        val config5 = cache.getConfiguration()
        assertNotNull(config5)
        assertEquals(
            "METHOD_3",
            config5.containerSettings?.preferredUploadMethod,
        )
        assertEquals(
            3,
            fetchCount,
            "Cache expired again, should fetch fresh data",
        )

        cache.close()
    }

    @Test
    fun `configuration refresh handles concurrent requests safely`(): Unit =
        runBlocking {
            val refreshInterval = Duration.ofMillis(100)
            var fetchCount = 0
            val lock = Any()

            val configurationProvider: suspend () -> ConfigurationResponse =
                {
                    val currentFetch =
                        synchronized(lock) {
                            fetchCount++
                            fetchCount
                        }
                    // Simulate network delay
                    kotlinx.coroutines.delay(50)
                    ConfigurationResponse(
                        containerSettings =
                        ContainerSettings(
                            preferredUploadMethod =
                            "CONCURRENT_$currentFetch",
                            containers =
                            listOf(
                                ContainerInfo(
                                    path =
                                    "https://concurrent.blob.core.windows.net/data",
                                ),
                            ),
                        ),
                    )
                }

            val cache =
                DefaultConfigurationCache(
                    clientDetails = ClientDetails.createDefault(),
                    refreshInterval = refreshInterval,
                    configurationProvider = configurationProvider,
                )

            // First call to populate cache
            val initialConfig = cache.getConfiguration()
            assertNotNull(initialConfig)
            assertEquals(1, synchronized(lock) { fetchCount })

            // Wait for expiry
            Thread.sleep(150)

            // Make multiple concurrent requests after cache expires
            val results = coroutineScope {
                List(10) { async { cache.getConfiguration() } }
                    .awaitAll()
            }

            // All results should be consistent (same data)
            // Due to the double-check locking pattern, only one of the concurrent
            // fetches will actually update the cache
            val uniqueResults =
                results.map {
                    it.containerSettings?.preferredUploadMethod
                }
                    .toSet()
            assertEquals(
                1,
                uniqueResults.size,
                "All concurrent requests should return the same cached value",
            )

            // Multiple provider calls may happen due to concurrent access before
            // synchronization,
            // but this is acceptable as only one result gets cached
            val finalFetchCount = synchronized(lock) { fetchCount }
            assert(finalFetchCount >= 2) {
                "At least one refresh should have occurred"
            }

            cache.close()
        }
}
