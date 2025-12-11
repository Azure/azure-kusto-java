// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader.models

import com.microsoft.azure.kusto.ingest.v2.common.BatchOperationResult
import java.time.Instant

sealed class UploadResult {
    abstract val sourceName: String
    abstract val startedAt: Instant
    abstract val completedAt: Instant

    data class Success(
        override val sourceName: String,
        override val startedAt: Instant,
        override val completedAt: Instant,
        val blobUrl: String,
        val sizeBytes: Long,
    ) : UploadResult()

    data class Failure(
        override val sourceName: String,
        override val startedAt: Instant,
        override val completedAt: Instant,
        val errorCode: UploadErrorCode,
        val errorMessage: String,
        val exception: Exception?,
        val isPermanent: Boolean = false,
    ) : UploadResult()
}

data class UploadResults(
    override val successes: List<UploadResult.Success>,
    override val failures: List<UploadResult.Failure>,
) : BatchOperationResult<UploadResult.Success, UploadResult.Failure>
