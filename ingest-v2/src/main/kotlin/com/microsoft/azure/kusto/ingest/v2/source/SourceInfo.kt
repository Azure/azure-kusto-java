// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2.source

import java.util.UUID

interface SourceInfo {
    /**
     * Checks that this SourceInfo is defined appropriately.
     */
    fun validate()

    var sourceId: UUID?
}
