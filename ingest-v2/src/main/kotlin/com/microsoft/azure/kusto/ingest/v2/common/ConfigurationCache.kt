/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2.common

import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import java.lang.AutoCloseable
import java.time.Duration
import kotlin.text.compareTo

interface ConfigurationCache : AutoCloseable {
    val refreshInterval: Duration

    suspend fun getConfiguration(): ConfigurationResponse

    override fun close()
}

class DefaultConfigurationCache(
    override val refreshInterval: Duration = Duration.ofHours(1),
    private val configurationProvider: suspend () -> ConfigurationResponse,
) : ConfigurationCache {
    @Volatile private var cachedConfiguration: ConfigurationResponse? = null
    private var lastRefreshTime: Long = 0

    override suspend fun getConfiguration(): ConfigurationResponse {
        val currentTime = System.currentTimeMillis()
        val needsRefresh =
            cachedConfiguration == null ||
                (currentTime - lastRefreshTime) >=
                refreshInterval.toMillis()
        if (needsRefresh) {
            val newConfig =
                runCatching { configurationProvider() }
                    .getOrElse { cachedConfiguration ?: throw it }
            synchronized(this) {
                // Double-check in case another thread refreshed while we were waiting
                val stillNeedsRefresh =
                    cachedConfiguration == null ||
                        (currentTime - lastRefreshTime) >=
                        refreshInterval.toMillis()
                if (stillNeedsRefresh) {
                    cachedConfiguration = newConfig
                    lastRefreshTime = currentTime
                }
            }
        }
        return cachedConfiguration!!
    }

    override fun close() {
        // No resources to clean up in this implementation
    }
}
