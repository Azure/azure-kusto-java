// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import java.io.InputStream

abstract class LocalSource(
    override val format: DataFormat,
    val leaveOpen: Boolean,
    override val compressionType: CompressionType = CompressionType.NONE,
    val baseName: String? = null,
    override val sourceId: String? = null,
) : IngestionSource(format, compressionType, baseName, sourceId) {

    protected var mStream: InputStream? = null

    // Indicates whether the stream should be left open after ingestion.
    // val leaveOpen: Boolean // Already a constructor property

    internal val shouldCompress: Boolean
        get() =
            (compressionType == CompressionType.NONE) &&
                !format.isBinaryFormat()

    abstract fun data(): InputStream

    fun reset() {
        data().reset()
    }

    override fun close() {
        if (!leaveOpen) {
            mStream?.close()
        }
    }
}

class StreamSource(
    stream: InputStream,
    format: DataFormat,
    sourceCompression: CompressionType,
    sourceId: String? = null,
    name: String? = null,
    leaveOpen: Boolean = false,
) : LocalSource(format, leaveOpen, sourceCompression, name, sourceId) {

    init {
        mStream = stream
        initName(name)
    }

    override fun data(): InputStream {
        return mStream!!
    }
}
