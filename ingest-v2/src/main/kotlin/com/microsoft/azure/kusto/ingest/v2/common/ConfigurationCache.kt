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
     * Holds both the configuration and its refresh timestamp atomically.
     * This prevents race conditions between checking expiration and updating.
     */
    private data class CachedData(
        val configuration: ConfigurationResponse,
        val timestamp: Long
    )

    private val cache = AtomicReference<CachedData?>(null)

    override suspend fun getConfiguration(): ConfigurationResponse {
        val currentTime = System.currentTimeMillis()
        val cached = cache.get()
        
        // Check if we need to refresh
        val needsRefresh = cached == null || 
            (currentTime - cached.timestamp) >= refreshInterval.toMillis()
        
        if (needsRefresh) {
            // Attempt to refresh - only one thread will succeed
            val newConfig = runCatching { provider() }
                .getOrElse { 
                    // If fetch fails, return cached if available, otherwise rethrow
                    cached?.configuration ?: throw it 
                }
            
            // Atomically update if still needed (prevents thundering herd)
            cache.updateAndGet { current ->
                val currentTimestamp = current?.timestamp ?: 0
                // Only update if current is null or still stale
                if (current == null || 
                    (currentTime - currentTimestamp) >= refreshInterval.toMillis()) {
                    CachedData(newConfig, currentTime)
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
