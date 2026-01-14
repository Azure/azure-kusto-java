// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.common.utils.PathUtils
import com.microsoft.azure.kusto.ingest.v2.models.Format
import java.io.Closeable
import java.util.UUID

abstract class IngestionSource(
    open val format: Format,
    open val compressionType: CompressionType,
    open val sourceId: UUID = UUID.randomUUID(),
) : Closeable {

    var name: String
        private set

    init {
        name = initName()
    }

    override fun close() {
        // No-op by default, override if needed
    }

    protected fun initName(): String {
        val type =
            this::class.simpleName?.removeSuffix("Source")?.lowercase()
                ?: "source"
        return "${type}_${PathUtils.sanitizeFileName(sourceId)}${format.value}$compressionType"
    }

    override fun toString(): String {
        return "${this::class.simpleName} - `$name`"
    }
}
