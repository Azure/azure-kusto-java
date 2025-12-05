// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.models.Format
import java.io.InputStream
import java.util.UUID

/** Abstract base class for local ingestion sources (file or stream). */
abstract class LocalSource(
    format: Format,
    val leaveOpen: Boolean,
    compressionType: CompressionType = CompressionType.NONE,
    baseName: String? = null,
    sourceId: UUID = UUID.randomUUID(),
) : IngestionSource(format, compressionType, baseName, sourceId) {
    protected var mStream: InputStream? = null

    /**
     * Indicates whether the stream should be compressed during upload. Binary
     * formats should not be compressed as they already have internal
     * compression.
     */
    val shouldCompress: Boolean
        get() =
            (compressionType == CompressionType.NONE) &&
                !FormatUtil.isBinaryFormat(format)

    /** Returns the data stream for ingestion. */
    abstract fun data(): InputStream

    /**
     * Returns the approximate size of the data in bytes. For files, returns the
     * exact file size. For streams, attempts to determine available bytes (may
     * not be accurate for all stream types). Returns null if size cannot be
     * determined.
     */
    abstract fun size(): Long?

    /** Resets the stream position to the beginning. */
    fun reset() {
        data().apply { reset() }
    }

    override fun close() {
        if (!leaveOpen) {
            mStream?.close()
        }
    }

    /** Generates a unique blob name for upload. */
    fun generateBlobName(): String {
        // Binary formats should not be compressed, so effective compression stays NONE
        val effectiveCompression =
            if (shouldCompress) {
                CompressionType.GZIP
            } else {
                compressionType
            }
        return "${sourceId}_${format.value}.$effectiveCompression"
    }

    /**
     * Returns the path or name for tracing purposes. Subclasses can override
     * this to provide specific information.
     */
    internal open fun getPathOrNameForTracing(): String {
        return name
    }
}
