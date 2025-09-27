// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.utils

import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import kotlin.collections.contains

object IngestionResultUtils {

    fun hasFailedResults(results: List<BlobStatus>): Boolean {
        return results.any { it.status == BlobStatus.Status.Failed }
    }

    fun isCompleted(results: List<BlobStatus>?): Boolean {
        return results?.isNotEmpty() == true &&
            results.all { result ->
                result.status in
                    listOf(
                        BlobStatus.Status.Succeeded,
                        BlobStatus.Status.Failed,
                        BlobStatus.Status.Canceled,
                    )
            }
    }

    fun isInProgress(results: List<BlobStatus>?): Boolean {
        return results?.any { result ->
            result.status in
                listOf(
                    BlobStatus.Status.Queued,
                    BlobStatus.Status.InProgress,
                )
        } == true
    }

    fun getFailedResults(results: List<BlobStatus>?): List<BlobStatus> {
        return results?.filter { it.status == BlobStatus.Status.Failed }
            ?: emptyList()
    }

    fun getSucceededResults(results: List<BlobStatus>?): List<BlobStatus> {
        return results?.filter { it.status == BlobStatus.Status.Succeeded }
            ?: emptyList()
    }
}
