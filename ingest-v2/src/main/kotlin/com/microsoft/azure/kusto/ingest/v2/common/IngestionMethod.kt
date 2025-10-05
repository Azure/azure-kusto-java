// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

import kotlinx.serialization.Serializable

/** Represents the available ingestion methods. */
@Serializable
enum class IngestionMethod {
    /** Queued ingestion method. */
    QUEUED,

    /** Streaming ingestion method. */
    STREAMING,
}
