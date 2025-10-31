// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.models.Format
import java.io.InputStream
import java.util.UUID

abstract class LocalSource(
    override val format: Format,
    val leaveOpen: Boolean,
    override val compressionType: CompressionType = CompressionType.NONE,
    val baseName: String? = null,
    override val sourceId: UUID = UUID.randomUUID(),
) : IngestionSource(format, compressionType, baseName, sourceId) {

    // Lazily initialized input stream for ingestion source
    protected lateinit var mStream: InputStream

    // Indicates whether the stream should be left open after ingestion.
    // val leaveOpen: Boolean // Already a constructor property

    internal val shouldCompress: Boolean
        get() =
            (compressionType == CompressionType.NONE) &&
                !isBinaryFormat(format)

    abstract fun data(): InputStream

    fun reset() {
        data().reset()
    }

    override fun close() {
        if (!leaveOpen) {
            if (this::mStream.isInitialized) {
                mStream.close()
            }
        }
    }

    fun isBinaryFormat(format: Format): Boolean {
        return when (format) {
            Format.avro,
            Format.parquet,
            Format.orc,
            Format.apacheavro,
            -> true
            else -> false
        }
    }
}

class StreamSource(
    stream: InputStream,
    format: Format,
    sourceCompression: CompressionType,
    sourceId: UUID = UUID.randomUUID(),
    name: String? = null,
    leaveOpen: Boolean = false,
) : LocalSource(format, leaveOpen, sourceCompression, name, sourceId) {

    init {
        mStream = stream
        initName(name)
    }

    override fun data(): InputStream {
        return mStream
            ?: throw IllegalStateException("Stream is not initialized")
    }
}
