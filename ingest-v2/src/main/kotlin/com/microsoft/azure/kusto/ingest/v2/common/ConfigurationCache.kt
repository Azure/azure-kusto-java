// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.CONFIG_CACHE_DEFAULT_REFRESH_INTERVAL_HOURS
import com.microsoft.azure.kusto.ingest.v2.CONFIG_CACHE_DEFAULT_SKIP_SECURITY_CHECKS
import com.microsoft.azure.kusto.ingest.v2.ConfigurationClient
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import java.lang.AutoCloseable
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.min

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
     */
    suspend fun getConfiguration(): ConfigurationResponse

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
                )
                    .getConfigurationDetails()
            }

    /**
     * Holds the configuration, its refresh timestamp, and the effective refresh
     * interval atomically. This prevents race conditions between checking
     * expiration and updating, and ensures we use the correct refresh interval
     * from when the config was fetched.
     */
    private data class CachedData(
        val configuration: ConfigurationResponse,
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
        config: ConfigurationResponse?,
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

    override suspend fun getConfiguration(): ConfigurationResponse {
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
            val newConfig =
                runCatching { provider() }
                    .getOrElse {
                        // If fetch fails, return cached if available, otherwise rethrow
                        cachedData?.configuration ?: throw it
                    }

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
