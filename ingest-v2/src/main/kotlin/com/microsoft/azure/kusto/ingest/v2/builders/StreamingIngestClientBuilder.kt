// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.microsoft.azure.kusto.ingest.v2.client.StreamingIngestClient
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails

class StreamingIngestClientBuilder
private constructor(private val clusterUrl: String) :
    BaseIngestClientBuilder<StreamingIngestClientBuilder>() {

    override fun self(): StreamingIngestClientBuilder = this

    companion object {
        @JvmStatic
        fun create(clusterUrl: String): StreamingIngestClientBuilder {
            require(clusterUrl.isNotBlank()) { "Cluster URI cannot be blank" }
            // Make sure to convert it to cluster-url if user passed ingest-url
            return StreamingIngestClientBuilder(
                normalizeAndCheckEngineUrl(clusterUrl),
            )
        }
    }

    fun build(): StreamingIngestClient {
        requireNotNull(tokenCredential) {
            "Authentication is required. Call withAuthentication() before build()"
        }
        val effectiveClientDetails =
            clientDetails ?: ClientDetails.createDefault()
        val apiClient =
            createApiClient(
                this.clusterUrl,
                this.tokenCredential!!,
                effectiveClientDetails,
                this.skipSecurityChecks,
            )
        return StreamingIngestClient(
            apiClient = apiClient,
        )
    }
}
