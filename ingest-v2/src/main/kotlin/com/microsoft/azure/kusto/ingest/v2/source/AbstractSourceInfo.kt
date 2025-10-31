// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2.source

import java.util.UUID

abstract class AbstractSourceInfo : SourceInfo {
    override var sourceId: UUID? = null
}
