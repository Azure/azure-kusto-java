// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.models.Format
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream

abstract class LocalSource(
    override val format: Format,
    val leaveOpen: Boolean,
    override val compressionType: CompressionType = CompressionType.NONE,
    baseName: String? = null,
    override val sourceId: String? = null,
) : IngestionSource(format, compressionType, baseName, sourceId) {
    protected var mStream: InputStream? = null

    // Indicates whether the stream should be left open after ingestion.
    // val leaveOpen: Boolean // Already a constructor property
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
    format: Format,
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

class FileSource(
    path: Path,
    format: Format,
    compressionType: CompressionType = CompressionType.NONE,
    name: String? = null,
    sourceId: String? = null,
    leaveOpen: Boolean = false,
) : LocalSource(format, leaveOpen, compressionType, name, sourceId) {

    private val fileStream: InputStream =
        when (compressionType) {
            CompressionType.GZIP ->
                GZIPInputStream(Files.newInputStream(path))
            CompressionType.ZIP -> {
                val zipStream = ZipInputStream(Files.newInputStream(path))
                zipStream.nextEntry // Move to first entry
                zipStream
            }
            else -> Files.newInputStream(path)
        }

    init {
        mStream = fileStream
        initName(name)
    }

    override fun data(): InputStream {
        return mStream!!
    }

    override fun close() {
        if (!leaveOpen) {
            fileStream.close()
        }
    }
}
