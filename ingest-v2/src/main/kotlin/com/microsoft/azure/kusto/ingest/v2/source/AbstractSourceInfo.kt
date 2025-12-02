// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID

abstract class AbstractSourceInfo : SourceInfo {
    val logger: Logger
        get() = LoggerFactory.getLogger(SourceInfo::class.java)

    override var sourceId: UUID = UUID.randomUUID()
}
