// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.CONFIG_CACHE_DEFAULT_REFRESH_INTERVAL_HOURS
import com.microsoft.azure.kusto.ingest.v2.CONFIG_CACHE_DEFAULT_SKIP_SECURITY_CHECKS
import com.microsoft.azure.kusto.ingest.v2.ConfigurationClient
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import java.lang.AutoCloseable
import java.time.Duration

interface ConfigurationCache : AutoCloseable {
    val refreshInterval: Duration

    suspend fun getConfiguration(): ConfigurationResponse

    override fun close()
}

// TODO check for the refresh once the Default is created
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
                runCatching { provider() }
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
