// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.CONFIG_CACHE_DEFAULT_REFRESH_INTERVAL_HOURS
import com.microsoft.azure.kusto.ingest.v2.CONFIG_CACHE_DEFAULT_SKIP_SECURITY_CHECKS
import com.microsoft.azure.kusto.ingest.v2.ConfigurationClient
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.models.S2SToken
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import com.microsoft.azure.kusto.ingest.v2.uploader.ExtendedContainerInfo
import com.microsoft.azure.kusto.ingest.v2.uploader.RoundRobinContainerList
import com.microsoft.azure.kusto.ingest.v2.uploader.UploadMethod
import java.lang.AutoCloseable
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.min

/**
 * Wrapper around ConfigurationResponse that includes shared RoundRobinContainerList
 * instances for even distribution of uploads across containers.
 *
 * This class holds the configuration response along with pre-created container lists
 * that maintain their own atomic counters. All uploaders sharing the same cache
 * will use the same RoundRobinContainerList instances, ensuring proper load
 * distribution.
 */
class CachedConfigurationData(
    val response: ConfigurationResponse,
) {
    /**
     * Lazily initialized RoundRobinContainerList for storage containers.
     * The list is created once and reused for all requests until the cache refreshes.
     */
    val storageContainerList: RoundRobinContainerList by lazy {
        val containers = response.containerSettings?.containers
        if (containers.isNullOrEmpty()) {
            RoundRobinContainerList.empty()
        } else {
            RoundRobinContainerList.of(
                containers.map { ExtendedContainerInfo(it, UploadMethod.STORAGE) }
            )
        }
    }

    /**
     * Lazily initialized RoundRobinContainerList for lake containers.
     * The list is created once and reused for all requests until the cache refreshes.
     */
    val lakeContainerList: RoundRobinContainerList by lazy {
        val lakeFolders = response.containerSettings?.lakeFolders
        if (lakeFolders.isNullOrEmpty()) {
            RoundRobinContainerList.empty()
        } else {
            RoundRobinContainerList.of(
                lakeFolders.map { ExtendedContainerInfo(it, UploadMethod.LAKE) }
            )
        }
    }

    // Delegate all ConfigurationResponse properties
    val containerSettings get() = response.containerSettings
    val ingestionSettings get() = response.ingestionSettings
}

/**
 * Interface for caching configuration data.
 *
 * Implementations should handle automatic refresh of stale data based on a
 * refresh interval. When used with client builders, the configuration will be
 * fetched at least once during client construction to ensure fresh data is
 * available.
 */
interface ConfigurationCache : AutoCloseable {
    val refreshInterval: Duration

    /**
     * Gets the current configuration, refreshing it if necessary based on the
     * refresh interval. This method may return cached data if the cache is
     * still valid.
     *
     * The returned CachedConfigurationData includes shared RoundRobinContainerList
     * instances that provide even distribution of uploads across containers for
     * all uploaders sharing this cache.
     */
    suspend fun getConfiguration(): CachedConfigurationData

    override fun close()
}

/**
 * Default implementation of ConfigurationCache with time-based expiration.
 *
 * This cache automatically refreshes configuration data when it becomes stale
 * based on the configured refresh interval. The implementation is thread-safe
 * and handles concurrent requests efficiently.
 *
 * Refresh behavior:
 * - Configuration is refreshed automatically when the refresh interval expires
 * - If refresh fails, the existing cached configuration is returned (if
 *   available)
 * - The first call to getConfiguration() will always fetch fresh data
 * - Concurrent refresh attempts are synchronized to prevent duplicate fetches
 *
 * @param refreshInterval Duration after which cached configuration is
 *   considered stale
 * @param dmUrl Data management endpoint URL (required if configurationProvider
 *   is null)
 * @param tokenCredential Authentication credentials (required if
 *   configurationProvider is null)
 * @param skipSecurityChecks Whether to skip security validation (required if
 *   configurationProvider is null)
 * @param clientDetails Client identification details for tracking
 * @param configurationProvider Optional custom provider for configuration data.
 *   If provided, dmUrl/tokenCredential/skipSecurityChecks are not required.
 */
class DefaultConfigurationCache(
    override val refreshInterval: Duration =
        Duration.ofHours(CONFIG_CACHE_DEFAULT_REFRESH_INTERVAL_HOURS),
    val dmUrl: String? = null,
    val tokenCredential: TokenCredential? = null,
    val skipSecurityChecks: Boolean? =
        CONFIG_CACHE_DEFAULT_SKIP_SECURITY_CHECKS,
    val clientDetails: ClientDetails,
    val configurationProvider: (suspend () -> ConfigurationResponse)? = null,
    val s2sTokenProvider: (suspend () -> S2SToken)? = null,
    val s2sFabricPrivateLinkAccessContext: String? = null,
) : ConfigurationCache {
    companion object {
        /**
         * Creates a DefaultConfigurationCache for Java callers.
         *
         * This factory method provides a convenient way to create a cache from
         * Java without dealing with Kotlin named parameters.
         *
         * @param dmUrl Data management endpoint URL
         * @param tokenCredential Authentication credentials
         * @param clientDetails Client identification details for tracking
         * @return A new DefaultConfigurationCache instance
         */
        @JvmStatic
        fun create(
            dmUrl: String,
            tokenCredential: TokenCredential,
            clientDetails: ClientDetails,
        ): DefaultConfigurationCache =
            DefaultConfigurationCache(
                dmUrl = dmUrl,
                tokenCredential = tokenCredential,
                clientDetails = clientDetails,
            )

        /**
         * Creates a DefaultConfigurationCache with all options for Java
         * callers.
         *
         * @param dmUrl Data management endpoint URL
         * @param tokenCredential Authentication credentials
         * @param skipSecurityChecks Whether to skip security validation
         * @param clientDetails Client identification details for tracking
         * @param refreshInterval Duration after which cached configuration is
         *   stale
         * @return A new DefaultConfigurationCache instance
         */
        @JvmStatic
        fun create(
            dmUrl: String,
            tokenCredential: TokenCredential,
            skipSecurityChecks: Boolean,
            clientDetails: ClientDetails,
            refreshInterval: Duration,
        ): DefaultConfigurationCache =
            DefaultConfigurationCache(
                refreshInterval = refreshInterval,
                dmUrl = dmUrl,
                tokenCredential = tokenCredential,
                skipSecurityChecks = skipSecurityChecks,
                clientDetails = clientDetails,
            )
    }

    init {
        if (
            configurationProvider == null &&
            (
                dmUrl == null ||
                    tokenCredential == null ||
                    skipSecurityChecks == null
                )
        ) {
            throw IllegalArgumentException(
                "Either configurationProvider or all of dmUrl, tokenCredential, and skipSecurityChecks must be provided",
            )
        }
    }

    private val provider: suspend () -> ConfigurationResponse =
        configurationProvider
            ?: {
                ConfigurationClient(
                    dmUrl!!,
                    tokenCredential!!,
                    skipSecurityChecks!!,
                    clientDetails,
                    s2sTokenProvider,
                    s2sFabricPrivateLinkAccessContext,
                )
                    .getConfigurationDetails()
            }

    /**
     * Holds the configuration, its refresh timestamp, and the effective refresh
     * interval atomically. This prevents race conditions between checking
     * expiration and updating, and ensures we use the correct refresh interval
     * from when the config was fetched.
     *
     * The CachedConfigurationData wrapper includes pre-created RoundRobinContainerList
     * instances that are shared by all uploaders using this cache.
     */
    private data class CachedData(
        val configuration: CachedConfigurationData,
        val timestamp: Long,
        val refreshInterval: Long,
    )

    private val cache = AtomicReference<CachedData?>(null)

    /**
     * Parses a .NET TimeSpan format string to a Java Duration.
     *
     * Supports formats:
     * - HH:mm:ss (e.g., "01:00:00" = 1 hour)
     * - d.HH:mm:ss (e.g., "1.02:30:00" = 1 day, 2 hours, 30 minutes)
     * - HH:mm:ss.fffffff (with fractional seconds)
     *
     * @param timeSpan The TimeSpan string to parse
     * @return The parsed Duration, or null if parsing fails
     */
    private fun parseTimeSpanToDuration(timeSpan: String): Duration? {
        return try {
            // Split by '.' to handle days (format: "d.HH:mm:ss")
            val parts = timeSpan.split('.')
            val timePart = if (parts.size > 1) parts[1] else parts[0]
            val days = if (parts.size > 1) parts[0].toLongOrNull() ?: 0L else 0L

            // Split time part by ':' to get hours, minutes, seconds
            val timeParts = timePart.split(':')
            if (timeParts.size < 3) return null

            val hours = timeParts[0].toLongOrNull() ?: return null
            val minutes = timeParts[1].toLongOrNull() ?: return null

            // Handle fractional seconds (e.g., "30.1234567")
            val secondsPart = timeParts[2]
            val secondsValue = secondsPart.toDoubleOrNull() ?: return null

            // Build duration
            var duration =
                Duration.ofDays(days)
                    .plusHours(hours)
                    .plusMinutes(minutes)
                    .plusSeconds(secondsValue.toLong())

            // Add fractional seconds if present
            val fractionalSeconds = (secondsValue - secondsValue.toLong())
            if (fractionalSeconds > 0) {
                duration =
                    duration.plusMillis((fractionalSeconds * 1000).toLong())
            }

            duration
        } catch (_: Exception) {
            null
        }
    }

    /**
     * Helper function to calculate effective refresh interval from a
     * configuration response. If the configuration specifies a refresh
     * interval, use the minimum of that and the default. Otherwise, use the
     * default refresh interval.
     */
    private fun calculateEffectiveRefreshInterval(
        config: CachedConfigurationData?,
    ): Long {
        val configRefreshInterval = config?.containerSettings?.refreshInterval
        return if (configRefreshInterval?.isNotEmpty() == true) {
            val parsedDuration = parseTimeSpanToDuration(configRefreshInterval)
            if (parsedDuration != null) {
                min(this.refreshInterval.toMillis(), parsedDuration.toMillis())
            } else {
                // If parsing fails, log warning and use default
                this.refreshInterval.toMillis()
            }
        } else {
            this.refreshInterval.toMillis()
        }
    }

    override suspend fun getConfiguration(): CachedConfigurationData {
        val currentTime = System.currentTimeMillis()
        val cachedData = cache.get()

        // Check if we need to refresh based on the effective refresh interval
        // stored with the cached data
        val needsRefresh =
            cachedData == null ||
                (currentTime - cachedData.timestamp) >=
                cachedData.refreshInterval

        if (needsRefresh) {
            // Attempt to refresh - only one thread will succeed
            val newConfigResponse =
                runCatching { provider() }
                    .getOrElse {
                        // If fetch fails, return cached if available, otherwise rethrow
                        cachedData?.configuration?.response ?: throw it
                    }

            // Wrap the response in CachedConfigurationData to create shared
            // RoundRobinContainerList instances
            val newConfig = CachedConfigurationData(newConfigResponse)

            // Calculate effective refresh interval from the NEW configuration
            val newEffectiveRefreshInterval =
                calculateEffectiveRefreshInterval(newConfig)

            // Atomically update if still needed (prevents thundering herd)
            cache.updateAndGet { current ->
                // Only update if current is null or still stale based on its
                // stored effective interval
                if (
                    current == null ||
                    (currentTime - current.timestamp) >=
                    current.refreshInterval
                ) {
                    CachedData(
                        newConfig,
                        currentTime,
                        newEffectiveRefreshInterval,
                    )
                } else {
                    // Another thread already refreshed
                    current
                }
            }
        }

        // Return the cached value (guaranteed non-null after refresh)
        return cache.get()!!.configuration
    }

    override fun close() {
        // No resources to clean up in this implementation
    }
}
