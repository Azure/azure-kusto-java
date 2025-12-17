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
    baseName: String? = null,
    open val sourceId: UUID = UUID.randomUUID(),
) : Closeable {

    var name: String
        private set

    init {
        name = initName(baseName)
    }

    override fun close() {
        // No-op by default, override if needed
    }

    protected fun initName(baseName: String? = null): String {
        val type =
            this::class.simpleName?.removeSuffix("Source")?.lowercase()
                ?: "source"
        return "${type}_${PathUtils.sanitizeFileName(baseName, sourceId)}${format.value}$compressionType"
    }

    override fun toString(): String {
        return "${this::class.simpleName} - `$name`"
    }
}
