// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.common.utils.PathUtils
import com.microsoft.azure.kusto.ingest.v2.models.Format
import java.lang.AutoCloseable

abstract class IngestionSource(
    open val format: Format,
    open val compressionType: CompressionType?,
    open val url: String?,
    open val sourceId: String?,
) : AutoCloseable {
    var name: String? = null
        private set

    fun initName(baseName: String? = null) {
        val type =
            this::class.simpleName?.lowercase()?.removeSuffix("source")
                ?: "unknown"
        name =
            "${type}_${PathUtils.sanitizeFileName(baseName, sourceId)}${format}$compressionType"
    }
}

// Placeholder classes for missing dependencies
object ExtendedDataSourceCompressionType {
    fun detectFromUri(url: String): CompressionType? = null
}
