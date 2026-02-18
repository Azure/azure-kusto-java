// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.microsoft.azure.kusto.ingest.v2.IngestClientBase
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.uploader.IUploader

class QueuedIngestClientBuilder private constructor(private val dmUrl: String) :
    BaseIngestClientBuilder<QueuedIngestClientBuilder>() {

    override fun self(): QueuedIngestClientBuilder = this

    companion object {
        @JvmStatic
        fun create(dmUrl: String): QueuedIngestClientBuilder {
            require(dmUrl.isNotBlank()) { "Data Ingestion URI cannot be blank" }
            // Make sure to convert it to ingest-url if user passed engine-url
            return QueuedIngestClientBuilder(
                IngestClientBase.getIngestionEndpoint(dmUrl) ?: dmUrl,
            )
        }
    }

    fun withMaxConcurrency(concurrency: Int): QueuedIngestClientBuilder {
        require(concurrency > 0) {
            "Max concurrency must be positive, got: $concurrency"
        }
        this.maxConcurrency = concurrency
        return this
    }

    fun withMaxDataSize(bytes: Long): QueuedIngestClientBuilder {
        require(bytes > 0) { "Max data size must be positive, got: $bytes" }
        this.maxDataSize = bytes
        return this
    }

    fun withIgnoreFileSize(ignore: Boolean): QueuedIngestClientBuilder {
        this.ignoreFileSize = ignore
        return this
    }

    fun withUploader(
        uploader: IUploader,
        closeUploader: Boolean,
    ): QueuedIngestClientBuilder {
        this.uploader = uploader
        this.closeUploader = closeUploader
        return this
    }

    fun withConfiguration(
        configuration: ConfigurationCache,
    ): QueuedIngestClientBuilder {
        this.configuration = configuration
        return this
    }

    fun build(): QueuedIngestClient {
        requireNotNull(tokenCredential) {
            "Authentication is required. Call withAuthentication() before build()"
        }
        val effectiveClientDetails =
            clientDetails ?: ClientDetails.createDefault()
        val effectiveConfiguration =
            configuration
                ?: DefaultConfigurationCache(
                    dmUrl = this.dmUrl,
                    tokenCredential = this.tokenCredential,
                    skipSecurityChecks = this.skipSecurityChecks,
                    clientDetails = effectiveClientDetails,
                    s2sTokenProvider = this.s2sTokenProvider,
                    s2sFabricPrivateLinkAccessContext =
                        this.s2sFabricPrivateLinkAccessContext,
                )
        val apiClient =
            createApiClient(
                this.dmUrl,
                this.tokenCredential!!,
                effectiveClientDetails,
                this.skipSecurityChecks,
            )

        val effectiveUploader =
            uploader
                ?: createDefaultUploader(
                    configuration = effectiveConfiguration,
                    ignoreFileSize = this.ignoreFileSize,
                    maxConcurrency = this.maxConcurrency,
                    maxDataSize = this.maxDataSize,
                )
        return QueuedIngestClient(
            apiClient = apiClient,
            // TODO Question if this is redundant. ConfigurationCache is already held by
            // uploader
            cachedConfiguration = effectiveConfiguration,
            uploader = effectiveUploader,
            shouldDisposeUploader = closeUploader,
        )
    }
}
