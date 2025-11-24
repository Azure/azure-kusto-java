// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.container

import java.time.OffsetDateTime

sealed class UploadResult {
    abstract val sourceName: String
    abstract val startedAt: OffsetDateTime
    abstract val completedAt: OffsetDateTime

    data class Success(
        override val sourceName: String,
        override val startedAt: OffsetDateTime,
        override val completedAt: OffsetDateTime,
        val blobUrl: String,
        val sizeBytes: Long
    ) : UploadResult()

    data class Failure(
        override val sourceName: String,
        override val startedAt: OffsetDateTime,
        override val completedAt: OffsetDateTime,
        val errorCode: UploadErrorCode,
        val errorMessage: String,
        val exception: Exception?,
        val isPermanent: Boolean = false
    ) : UploadResult()
}

data class UploadResults(
    val successes: List<UploadResult.Success>,
    val failures: List<UploadResult.Failure>
) {
    val totalCount: Int get() = successes.size + failures.size
    val successCount: Int get() = successes.size
    val failureCount: Int get() = failures.size
    val hasFailures: Boolean get() = failures.isNotEmpty()
    val allSucceeded: Boolean get() = failures.isEmpty()
    
    companion object {
        fun empty() = UploadResults(emptyList(), emptyList())
        
        fun singleSuccess(result: UploadResult.Success) = 
            UploadResults(listOf(result), emptyList())
        
        fun singleFailure(result: UploadResult.Failure) = 
            UploadResults(emptyList(), listOf(result))
    }
}
