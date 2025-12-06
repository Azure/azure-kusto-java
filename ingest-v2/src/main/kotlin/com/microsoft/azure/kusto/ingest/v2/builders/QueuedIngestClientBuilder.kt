// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_CONCURRENCY
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient
import com.microsoft.azure.kusto.ingest.v2.common.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.uploaders.IUploader
import com.microsoft.azure.kusto.ingest.v2.uploaders.ManagedUploader

class QueuedIngestClientBuilder private constructor(private val dmUrl: String) :
    BaseIngestClientBuilder<QueuedIngestClientBuilder>() {

    private var maxConcurrency: Int = UPLOAD_CONTAINER_MAX_CONCURRENCY
    private var maxDataSize: Long = UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES
    private var ignoreFileSize: Boolean = false
    private var uploader: IUploader? = null
    private var closeUploader: Boolean = false
    private var configuration: ConfigurationCache? = null

    override fun self(): QueuedIngestClientBuilder = this

    companion object {
        @JvmStatic
        fun create(dmUrl: String): QueuedIngestClientBuilder {
            require(dmUrl.isNotBlank()) { "Data Ingestion URI cannot be blank" }
            return QueuedIngestClientBuilder(dmUrl)
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
        setEndpoint(dmUrl)
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
                )
        val apiClient =
            createApiClient(
                this.dmUrl,
                this.tokenCredential!!,
                effectiveClientDetails,
                this.skipSecurityChecks,
            )

        return QueuedIngestClient(
            apiClient = apiClient,
            // TODO Question if this is redundant. ConfigurationCache is already held by
            // uploader
            cachedConfiguration = effectiveConfiguration,
            uploader =
            uploader
                ?: createDefaultUploader(
                    effectiveConfiguration,
                ),
            shouldDisposeUploader = closeUploader,
        )
    }

    private fun createDefaultUploader(
        configuration: ConfigurationCache,
    ): IUploader {
        val managedUploader =
            ManagedUploader(
                ignoreSizeLimit = ignoreFileSize,
                maxConcurrency = maxConcurrency,
                maxDataSize = maxDataSize,
                configurationCache = configuration,
            )
        return managedUploader
    }
}
