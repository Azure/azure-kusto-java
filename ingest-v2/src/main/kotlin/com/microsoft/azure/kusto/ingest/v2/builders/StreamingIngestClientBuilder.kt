// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.StreamingIngestClient
import com.microsoft.azure.kusto.ingest.v2.common.ClientDetails


class StreamingIngestClientBuilder private constructor(
    private val engineUrl: String,
) : BaseIngestClientBuilder<StreamingIngestClientBuilder>() {

    companion object {
        @JvmStatic
        fun create(engineUrl: String): StreamingIngestClientBuilder {
            require(engineUrl.isNotBlank()) { "Engine URL cannot be blank" }
            return StreamingIngestClientBuilder(engineUrl)
        }
    }

    fun build(): StreamingIngestClient {
        requireNotNull(tokenCredential) {
            "Authentication is required. Call withAuthentication() before build()"
        }

        return StreamingIngestClient(
            engineUrl = engineUrl,
            tokenCredential = tokenCredential!!,
            skipSecurityChecks = skipSecurityChecks,
            clientDetails = clientDetails,
        )
    }
}
