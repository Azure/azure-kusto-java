// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.KustoBaseApiClient
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_CONCURRENCY
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient
import com.microsoft.azure.kusto.ingest.v2.common.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.uploaders.IUploader
import com.microsoft.azure.kusto.ingest.v2.uploaders.ManagedUploader

class QueuedIngestionClientBuilder
private constructor(private val dmUrl: String) :
    BaseIngestClientBuilder<QueuedIngestionClientBuilder>() {

    private var maxConcurrency: Int = UPLOAD_CONTAINER_MAX_CONCURRENCY
    private var maxDataSize: Long = UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES
    private var ignoreFileSize: Boolean = false
    private var uploader: IUploader? = null
    private var closeUploader: Boolean = false
    private var configuration: ConfigurationCache? = null

    override fun self(): QueuedIngestionClientBuilder = this

    companion object {
        @JvmStatic
        fun create(dmUrl: String): QueuedIngestionClientBuilder {
            require(dmUrl.isNotBlank()) { "Data Ingestion URI cannot be blank" }
            val normalizedUrl =
                if (dmUrl.matches(Regex("https://(?!ingest-)[^/]+.*"))) {
                    // If the URL starts with https:// and does not already have ingest-, add it
                    dmUrl.replace(
                        Regex("https://([^/]+)"),
                        "https://ingest-$1",
                    )
                } else {
                    dmUrl
                }
            return QueuedIngestionClientBuilder(normalizedUrl)
        }
    }

    fun withMaxConcurrency(concurrency: Int): QueuedIngestionClientBuilder {
        require(concurrency > 0) {
            "Max concurrency must be positive, got: $concurrency"
        }
        this.maxConcurrency = concurrency
        return this
    }

    fun withMaxDataSize(bytes: Long): QueuedIngestionClientBuilder {
        require(bytes > 0) { "Max data size must be positive, got: $bytes" }
        this.maxDataSize = bytes
        return this
    }

    fun withIgnoreFileSize(ignore: Boolean): QueuedIngestionClientBuilder {
        this.ignoreFileSize = ignore
        return this
    }

    fun withUploader(
        uploader: IUploader,
        closeUploader: Boolean,
    ): QueuedIngestionClientBuilder {
        this.uploader = uploader
        this.closeUploader = closeUploader
        return this
    }

    fun withConfiguration(
        configuration: ConfigurationCache,
    ): QueuedIngestionClientBuilder {
        this.configuration = configuration
        return this
    }

    fun build(): QueuedIngestClient {
        requireNotNull(tokenCredential) {
            "Authentication is required. Call withAuthentication() before build()"
        }
        // TODO: Construct KustoBaseApiClient and ConfigurationCache as needed
        val apiClient =
            createApiClient(
                dmUrl,
                tokenCredential!!,
                clientDetails ?: ClientDetails.createDefault(),
                skipSecurityChecks,
            )
        return QueuedIngestClient(
            apiClient = apiClient,
            cachedConfiguration =
            configuration ?: DefaultConfigurationCache(),
            uploader = uploader ?: createDefaultUploader(),
            shouldDisposeUploader = closeUploader,
        )
    }

    private fun createApiClient(
        dmUrl: String,
        tokenCredential: TokenCredential,
        clientDetails: ClientDetails,
        skipSecurityChecks: Boolean,
    ): KustoBaseApiClient {
        return KustoBaseApiClient(
            dmUrl = dmUrl,
            tokenCredential = tokenCredential,
            skipSecurityChecks = skipSecurityChecks,
            clientDetails = clientDetails,
        )
    }

    private fun createDefaultUploader(): IUploader {
        val managedUploader =
            ManagedUploader(
                ignoreSizeLimit = ignoreFileSize,
                maxConcurrency = maxConcurrency,
                maxDataSize = maxDataSize,
                configurationCache =
                configuration ?: DefaultConfigurationCache(),
            )
        return managedUploader
    }
}
