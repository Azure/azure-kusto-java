// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.microsoft.azure.kusto.ingest.v2.QueuedIngestionClient
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_CONCURRENCY
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES

class QueuedIngestionClientBuilder
private constructor(private val dmUrl: String) :
    BaseIngestClientBuilder<QueuedIngestionClientBuilder>() {

    private var maxConcurrency: Int = UPLOAD_CONTAINER_MAX_CONCURRENCY
    private var maxDataSize: Long = UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES
    private var ignoreFileSize: Boolean = false

    companion object {
        @JvmStatic
        fun create(dmUrl: String): QueuedIngestionClientBuilder {
            require(dmUrl.isNotBlank()) { "Data Ingestion URI cannot be blank" }
            return QueuedIngestionClientBuilder(dmUrl)
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

    fun build(): QueuedIngestionClient {
        requireNotNull(tokenCredential) {
            "Authentication is required. Call withAuthentication() before build()"
        }

        return QueuedIngestionClient(
            dmUrl = dmUrl,
            tokenCredential = tokenCredential!!,
            skipSecurityChecks = skipSecurityChecks,
            clientDetails = clientDetails,
            maxConcurrency = maxConcurrency,
            maxDataSize = maxDataSize,
            ignoreFileSize = ignoreFileSize,
        )
    }
}
