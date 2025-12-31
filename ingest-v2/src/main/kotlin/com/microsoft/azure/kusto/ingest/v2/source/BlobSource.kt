// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.models.Format
import java.util.UUID

/**
 * Represents a blob-based ingestion source. This source references data that
 * already exists in blob storage.
 */
class BlobSource @JvmOverloads constructor(
    val blobPath: String,
    format: Format = Format.csv,
    sourceId: UUID = UUID.randomUUID(),
    compressionType: CompressionType = CompressionType.NONE,
    baseName: String? = null,
) : IngestionSource(format, compressionType, baseName, sourceId) {

    /**
     * The exact size of the blob in bytes if available. This is only set when
     * the blob was created by uploading a local source. Returns null if size is
     * not available (e.g., for external blob URLs).
     */
    var blobExactSize: Long? = null
        internal set

    init {
        require(blobPath.isNotBlank()) { "blobPath cannot be blank" }
    }

    /** Returns the exact size of the blob in bytes if available. */
    fun size(): Long? = blobExactSize

    override fun toString(): String {
        return "${super.toString()} blobPath: '${blobPath.split("?").first()}'"
    }

    /** Returns the blob path for tracing purposes (without SAS token). */
    internal fun getPathForTracing(): String {
        return blobPath.split("?").first()
    }
}
