// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.models.Format
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.ZipInputStream

abstract class LocalSource(
    val format: Format,
    val leaveOpen: Boolean,
    val compressionType: CompressionType = CompressionType.NONE,
    baseName: String? = null,
    override var sourceId: UUID = UUID.randomUUID(),
) : AbstractSourceInfo() {

    init {
        initName(baseName)
    }

    // Lazily initialized input stream for ingestion source
    protected lateinit var mStream: InputStream

    lateinit var name: String
        private set

    fun initName(baseName: String? = null) {
        name = "${baseName ?: sourceId.toString()}_$format.$compressionType"
    }

    // Indicates whether the stream should be left open after ingestion.
    abstract fun data(): InputStream

    /**
     * Returns the approximate size of the data in bytes.
     * For files, returns the exact file size.
     * For streams, attempts to determine available bytes (may not be accurate for all stream types).
     * Returns null if size cannot be determined.
     */
    abstract fun size(): Long?

    fun reset() {
        data().reset()
    }

    open fun close() {
        if (!leaveOpen) {
            if (this::mStream.isInitialized) {
                mStream.close()
            }
        }
    }

    /**
     * Prepares the source data for blob upload, handling compression if needed.
     * Returns a pair of (InputStream, size, effectiveCompressionType)
     */
    fun prepareForUpload(): Triple<InputStream, Long?, CompressionType> {
        // Binary formats (Parquet, AVRO, ORC) already have internal compression and should not be compressed again
        val shouldCompress = (compressionType == CompressionType.NONE) && !FormatUtil.isBinaryFormat(format)
        
        return if (shouldCompress) {
            // Compress using GZIP for non-binary formats
            val byteStream = ByteArrayOutputStream()
            GZIPOutputStream(byteStream).use { gzipOut ->
                data().copyTo(gzipOut)
            }
            val bytes = byteStream.toByteArray()
            Triple(
                bytes.inputStream(),
                bytes.size.toLong(),
                CompressionType.GZIP,
            )
        } else {
            val stream = data()
            val size =
                when (this) {
                    is FileSourceInfo -> Files.size(path)
                    is StreamSourceInfo ->
                        try {
                            stream.available().toLong()
                        } catch (_: Exception) {
                            null
                        }
                    else -> null
                }
            Triple(stream, size, compressionType)
        }
    }

    /** Generates a unique blob name for upload */
    fun generateBlobName(): String {
        // Binary formats should not be compressed, so effective compression stays NONE
        val shouldCompress = (compressionType == CompressionType.NONE) && !FormatUtil.isBinaryFormat(format)
        val effectiveCompression =
            if (shouldCompress) {
                CompressionType.GZIP
            } else {
                compressionType
            }
        return "${sourceId}_${format.value}.$effectiveCompression"
    }

    override fun validate() {
        // Basic validation - subclasses can override for specific validation
    }
}

class StreamSourceInfo(
    stream: InputStream,
    format: Format,
    sourceCompression: CompressionType,
    sourceId: UUID = UUID.randomUUID(),
    name: String? = null,
    leaveOpen: Boolean = false,
) : LocalSource(format, leaveOpen, sourceCompression, name, sourceId) {

    init {
        mStream = stream
    }

    override fun data(): InputStream {
        return mStream
    }

    override fun size(): Long? {
        return try {
            mStream.available().toLong()
        } catch (e: Exception) {
            logger.warn("Could not determine stream size: ${e.message}")
            null
        }
    }
}

class FileSourceInfo(
    val path: Path,
    format: Format,
    compressionType: CompressionType = CompressionType.NONE,
    name: String? = null,
    sourceId: UUID = UUID.randomUUID(),
    leaveOpen: Boolean = false,
) : LocalSource(format, leaveOpen, compressionType, name, sourceId) {

    // Expose file path for direct file upload APIs
    private val fileStream: InputStream =
        when (compressionType) {
            CompressionType.GZIP ->
                GZIPInputStream(Files.newInputStream(path))
            CompressionType.ZIP -> {
                val zipStream = ZipInputStream(Files.newInputStream(path))
                zipStream.nextEntry
                zipStream
            }
            else -> Files.newInputStream(path)
        }

    // Move to first entry
    init {
        mStream = fileStream
    }

    override fun data(): InputStream {
        return mStream
    }

    override fun size(): Long? {
        return try {
            Files.size(path)
        } catch (e: Exception) {
            logger.warn("Could not determine file size for ${path}: ${e.message}")
            null
        }
    }

    override fun close() {
        if (!leaveOpen) {
            fileStream.close()
        }
    }
}
