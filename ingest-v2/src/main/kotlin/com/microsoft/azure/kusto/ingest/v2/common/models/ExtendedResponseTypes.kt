// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse

enum class IngestKind {
    STREAMING,
    QUEUED,
}

data class ExtendedIngestResponse(
    val ingestResponse: IngestResponse,
    val ingestionType: IngestKind,
)
