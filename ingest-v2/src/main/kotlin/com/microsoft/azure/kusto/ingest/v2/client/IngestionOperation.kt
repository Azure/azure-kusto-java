// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind

/** Represents an ingestion operation that can be tracked. */
data class IngestionOperation(
    /** Unique identifier for the ingestion operation. */
    val operationId: String,

    /** The database name where data was ingested. */
    val database: String,

    /** The table name where data was ingested. */
    val table: String,

    /** The kind of ingestion (e.g., STREAMING, QUEUED). */
    val ingestKind: IngestKind,
)
