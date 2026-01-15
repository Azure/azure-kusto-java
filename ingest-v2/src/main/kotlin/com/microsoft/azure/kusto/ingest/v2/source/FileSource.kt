// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.common.exceptions.InvalidUploadStreamException
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadErrorCode
import java.io.FileNotFoundException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.util.UUID

/** Represents a file-based ingestion source. */
class FileSource @JvmOverloads constructor(
    val path: Path,
    format: Format,
    sourceId: UUID = UUID.randomUUID(),
    compressionType: CompressionType? = null,
) :
    LocalSource(
        format,
        leaveOpen = false,
        compressionType =
        compressionType ?: detectCompressionFromPath(path),
        sourceId = sourceId,
    ) {
    override fun data(): InputStream {
        if (mStream == null) {
            try {
                if (!Files.exists(path)) {
                    throw InvalidUploadStreamException(
                        fileName = path.toString(),
                        blobName = null,
                        failureSubCode = UploadErrorCode.SOURCE_NOT_FOUND,
                        isPermanent = true,
                        cause =
                        FileNotFoundException(
                            "File not found: $path",
                        ),
                    )
                }
                mStream = Files.newInputStream(path)
            } catch (e: NoSuchFileException) {
                throw InvalidUploadStreamException(
                    fileName = path.toString(),
                    blobName = null,
                    failureSubCode = UploadErrorCode.SOURCE_NOT_FOUND,
                    isPermanent = true,
                    cause = e,
                )
            } catch (e: Exception) {
                throw InvalidUploadStreamException(
                    fileName = path.toString(),
                    blobName = null,
                    failureSubCode = UploadErrorCode.SOURCE_NOT_READABLE,
                    isPermanent = false,
                    cause = e,
                )
            }
            if (mStream == null) {
                throw InvalidUploadStreamException(
                    fileName = path.toString(),
                    blobName = null,
                    failureSubCode = UploadErrorCode.SOURCE_IS_EMPTY,
                    isPermanent = true,
                )
            }
        }
        return mStream!!
    }

    override fun size(): Long? {
        return try {
            Files.size(path)
        } catch (_: Exception) {
            null
        }
    }

    override fun toString(): String {
        return "${super.toString()} path: '$path'"
    }

    override fun getPathOrNameForTracing(): String {
        return path.toString()
    }

    companion object {
        /** Detects compression type from file path based on file extension. */
        fun detectCompressionFromPath(path: Path): CompressionType {
            val fileName =
                path.fileName?.toString()?.lowercase()
                    ?: return CompressionType.NONE
            return when {
                fileName.endsWith(".gz") || fileName.endsWith(".gzip") ->
                    CompressionType.GZIP
                fileName.endsWith(".zip") -> CompressionType.ZIP
                else -> CompressionType.NONE
            }
        }
    }
}
