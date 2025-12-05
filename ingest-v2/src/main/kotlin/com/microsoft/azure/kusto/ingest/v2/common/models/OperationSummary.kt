// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import java.time.Clock
import java.time.OffsetDateTime

class OperationSummary {
    var operationId: String = ""
    var startTime: OffsetDateTime = OffsetDateTime.MIN
    var lastUpdateTime: OffsetDateTime = OffsetDateTime.MIN
    var inProgressCount: Long = 0
    var succeededCount: Long = 0
    var cancelledCount: Long = 0
    var failedCount: Long = 0

    val status: IngestStatus
        get() =
            when {
                inProgressCount > 0 -> IngestStatus.InProgress
                succeededCount > 0 &&
                    failedCount.toInt() == 0 &&
                    cancelledCount.toInt() == 0 ->
                    IngestStatus.Succeeded
                succeededCount > 0 -> IngestStatus.PartialSuccess
                else -> IngestStatus.Failed
            }

    constructor(
        ingestionOperation: IngestResponse,
        statusResponse: StatusResponse? = null,
    ) {
        startTime = statusResponse?.startTime ?: OffsetDateTime.MIN
        if (statusResponse != null && statusResponse.status != null) {
            succeededCount += statusResponse.status.succeeded ?: 0L
            failedCount += statusResponse.status.failed ?: 0L
            cancelledCount += statusResponse.status.canceled ?: 0L
            inProgressCount += statusResponse.status.inProgress ?: 0L
            if (
                statusResponse.startTime != null &&
                statusResponse.startTime.isBefore(startTime)
            ) {
                startTime = statusResponse.startTime
            }
        }
        operationId = ingestionOperation.ingestionOperationId ?: ""
        lastUpdateTime =
            statusResponse?.lastUpdated
                ?: OffsetDateTime.now(Clock.systemUTC())
    }
}
