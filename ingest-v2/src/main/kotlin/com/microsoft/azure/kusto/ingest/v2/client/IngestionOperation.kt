// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import java.util.UUID

/** Represents an ingestion operation that can be tracked. */
data class IngestionOperation(
    /** Unique identifier for the ingestion operation. */
    val operationId: UUID,

    /** The database name where data was ingested. */
    val database: String,

    /** The table name where data was ingested. */
    val table: String,
)
