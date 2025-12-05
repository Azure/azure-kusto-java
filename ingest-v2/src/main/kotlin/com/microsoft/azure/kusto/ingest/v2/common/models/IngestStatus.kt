// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

enum class IngestStatus {
    InProgress,
    Succeeded,
    PartialSuccess,
    Failed,
    Cancelled,
}
