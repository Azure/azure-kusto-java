// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.Status
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse

enum class IngestKind {
    STREAMING,
    QUEUED,
}

data class ExtendedIngestResponse(
    val ingestResponse: IngestResponse,
    val ingestionType: IngestKind,
)

data class ExtendedStatus(val status: Status, val ingestionType: IngestKind)

data class ExtendedStatusResponse(
    val status: StatusResponse,
    val ingestionType: IngestKind,
)
