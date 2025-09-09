// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo
import com.microsoft.azure.kusto.ingest.v2.models.ContainerSettings
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
}
