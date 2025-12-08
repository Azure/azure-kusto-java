// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.microsoft.azure.kusto.ingest.v2.client.ManagedStreamingIngestClient
import com.microsoft.azure.kusto.ingest.v2.client.policy.DefaultManagedStreamingPolicy
import com.microsoft.azure.kusto.ingest.v2.client.policy.ManagedStreamingPolicy
import com.microsoft.azure.kusto.ingest.v2.common.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.uploaders.IUploader

class ManagedStreamingIngestClientBuilder
private constructor(private val dmUrl: String) :
    BaseIngestClientBuilder<ManagedStreamingIngestClientBuilder>() {
    override fun self(): ManagedStreamingIngestClientBuilder = this

    private var managedStreamingPolicy: ManagedStreamingPolicy? = null

    companion object {
        @JvmStatic
        fun create(dmUrl: String): ManagedStreamingIngestClientBuilder {
            require(dmUrl.isNotBlank()) { "Data Ingestion URI cannot be blank" }
            return ManagedStreamingIngestClientBuilder(dmUrl)
        }
    }

    fun withUploader(
        uploader: IUploader,
        closeUploader: Boolean,
    ): ManagedStreamingIngestClientBuilder {
        this.uploader = uploader
        this.closeUploader = closeUploader
        return this
    }

    fun withManagedStreamingIngestPolicy(
        managedStreamingPolicy: ManagedStreamingPolicy,
    ): ManagedStreamingIngestClientBuilder {
        this.managedStreamingPolicy = managedStreamingPolicy
        return this
    }

    fun build(): ManagedStreamingIngestClient {
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

        val effectiveUploader =
            uploader
                ?: createDefaultUploader(
                    configuration = effectiveConfiguration,
                    ignoreFileSize = this.ignoreFileSize,
                    maxConcurrency = this.maxConcurrency,
                    maxDataSize = this.maxDataSize,
                )

        val queuedIngestClient =
            QueuedIngestClientBuilder.create(this.dmUrl)
                .withConfiguration(effectiveConfiguration)
                .withClientDetails(
                    effectiveClientDetails
                        .getApplicationForTracing(),
                    effectiveClientDetails
                        .getClientVersionForTracing(),
                    effectiveClientDetails.getUserNameForTracing(),
                )
                .withAuthentication(this.tokenCredential!!)
                .withUploader(effectiveUploader, closeUploader)
                .build()

        val effectiveManagedStreamingPolicy =
            managedStreamingPolicy ?: DefaultManagedStreamingPolicy()
        val streamingIngestClient =
            StreamingIngestClientBuilder.create(this.dmUrl)
                .withClientDetails(
                    effectiveClientDetails
                        .getApplicationForTracing(),
                    effectiveClientDetails
                        .getClientVersionForTracing(),
                    effectiveClientDetails.getUserNameForTracing(),
                )
                .withAuthentication(this.tokenCredential!!)
                .build()

        return ManagedStreamingIngestClient(
            streamingIngestClient,
            queuedIngestClient,
            effectiveManagedStreamingPolicy,
        )
    }
}
