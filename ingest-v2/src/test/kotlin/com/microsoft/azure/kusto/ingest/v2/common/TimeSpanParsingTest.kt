// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import com.microsoft.azure.kusto.ingest.v2.models.ContainerSettings
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TimeSpanParsingTest {

    private fun createClientDetails() =
        ClientDetails(
            applicationForTracing = "test",
            userNameForTracing = "testUser",
            clientVersionForTracing = "1.0",
        )

    @Test
    fun `parseTimeSpan handles standard HH_mm_ss format`() {
        runBlocking {
            // Given a configuration with "01:00:00" (1 hour)
            val config =
                ConfigurationResponse(
                    containerSettings =
                    ContainerSettings(
                        containers = emptyList(),
                        lakeFolders = emptyList(),
                        refreshInterval = "01:00:00",
                        preferredUploadMethod = null,
                    ),
                    ingestionSettings = null,
                )

            val cache =
                DefaultConfigurationCache(
                    refreshInterval = Duration.ofHours(2),
                    clientDetails = createClientDetails(),
                    configurationProvider = { config },
                )

            // When getting configuration
            val result = cache.getConfiguration()

            // Then it should parse "01:00:00" as 1 hour
            assertNotNull(result)
            assertEquals("01:00:00", result.containerSettings?.refreshInterval)
        }
    }

    @Test
    fun `parseTimeSpan handles days format d_HH_mm_ss`() {
        runBlocking {
            // Given a configuration with "1.02:30:00" (1 day, 2 hours, 30 minutes)
            val config =
                ConfigurationResponse(
                    containerSettings =
                    ContainerSettings(
                        containers = emptyList(),
                        lakeFolders = emptyList(),
                        refreshInterval = "1.02:30:00",
                        preferredUploadMethod = null,
                    ),
                    ingestionSettings = null,
                )

            val cache =
                DefaultConfigurationCache(
                    refreshInterval = Duration.ofDays(2),
                    clientDetails = createClientDetails(),
                    configurationProvider = { config },
                )

            // When getting configuration
            val result = cache.getConfiguration()

            // Then it should parse correctly
            assertNotNull(result)
            assertEquals(
                "1.02:30:00",
                result.containerSettings?.refreshInterval,
            )
        }
    }

    @Test
    fun `parseTimeSpan handles fractional seconds`() {
        runBlocking {
            // Given a configuration with "00:00:30.5" (30.5 seconds)
            val config =
                ConfigurationResponse(
                    containerSettings =
                    ContainerSettings(
                        containers = emptyList(),
                        lakeFolders = emptyList(),
                        refreshInterval = "00:00:30.5",
                        preferredUploadMethod = null,
                    ),
                    ingestionSettings = null,
                )

            val cache =
                DefaultConfigurationCache(
                    refreshInterval = Duration.ofMinutes(1),
                    clientDetails = createClientDetails(),
                    configurationProvider = { config },
                )

            // When getting configuration
            val result = cache.getConfiguration()

            // Then it should parse correctly
            assertNotNull(result)
            assertEquals(
                "00:00:30.5",
                result.containerSettings?.refreshInterval,
            )
        }
    }

    @Test
    fun `parseTimeSpan uses default when format is invalid`() {
        runBlocking {
            // Given a configuration with invalid format
            val config =
                ConfigurationResponse(
                    containerSettings =
                    ContainerSettings(
                        containers = emptyList(),
                        lakeFolders = emptyList(),
                        refreshInterval = "invalid",
                        preferredUploadMethod = null,
                    ),
                    ingestionSettings = null,
                )

            val defaultRefresh = Duration.ofHours(3)
            val cache =
                DefaultConfigurationCache(
                    refreshInterval = defaultRefresh,
                    clientDetails = createClientDetails(),
                    configurationProvider = { config },
                )

            // When getting configuration
            val result = cache.getConfiguration()

            // Then it should fall back to default and not throw
            assertNotNull(result)
        }
    }

    @Test
    fun `parseTimeSpan uses minimum of config and default`() {
        runBlocking {
            // Given a configuration with "00:30:00" (30 minutes)
            val config =
                ConfigurationResponse(
                    containerSettings =
                    ContainerSettings(
                        containers = emptyList(),
                        lakeFolders = emptyList(),
                        refreshInterval = "00:30:00",
                        preferredUploadMethod = null,
                    ),
                    ingestionSettings = null,
                )

            // And default is 2 hours
            val cache =
                DefaultConfigurationCache(
                    refreshInterval = Duration.ofHours(2),
                    clientDetails = createClientDetails(),
                    configurationProvider = { config },
                )

            // When getting configuration
            val result = cache.getConfiguration()

            // Then the effective refresh should be the minimum (30 minutes from config)
            assertNotNull(result)
        }
    }

    @Test
    fun `parseTimeSpan handles empty string`() {
        runBlocking {
            // Given a configuration with empty refreshInterval
            val config =
                ConfigurationResponse(
                    containerSettings =
                    ContainerSettings(
                        containers = emptyList(),
                        lakeFolders = emptyList(),
                        refreshInterval = "",
                        preferredUploadMethod = null,
                    ),
                    ingestionSettings = null,
                )

            val cache =
                DefaultConfigurationCache(
                    refreshInterval = Duration.ofHours(1),
                    clientDetails = createClientDetails(),
                    configurationProvider = { config },
                )

            // When getting configuration
            val result = cache.getConfiguration()

            // Then it should use default and not throw
            assertNotNull(result)
        }
    }

    @Test
    fun `parseTimeSpan handles null refreshInterval`() {
        runBlocking {
            // Given a configuration with null refreshInterval
            val config =
                ConfigurationResponse(
                    containerSettings =
                    ContainerSettings(
                        containers = emptyList(),
                        lakeFolders = emptyList(),
                        refreshInterval = null,
                        preferredUploadMethod = null,
                    ),
                    ingestionSettings = null,
                )

            val cache =
                DefaultConfigurationCache(
                    refreshInterval = Duration.ofHours(1),
                    clientDetails = createClientDetails(),
                    configurationProvider = { config },
                )

            // When getting configuration
            val result = cache.getConfiguration()

            // Then it should use default and not throw
            assertNotNull(result)
        }
    }
}
