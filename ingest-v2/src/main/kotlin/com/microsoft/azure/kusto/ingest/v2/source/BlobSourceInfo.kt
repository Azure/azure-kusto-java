// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.common.BatchOperationResult
import com.microsoft.azure.kusto.ingest.v2.container.BlobUploadContainer
import com.microsoft.azure.kusto.ingest.v2.container.UploadErrorCode
import com.microsoft.azure.kusto.ingest.v2.container.UploadSource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID

class BlobSourceInfo : AbstractSourceInfo {
    var blobPath: String
        private set

    // For internal usage - only when we create the blob
    var blobExactSize: Long? = null
        private set

    var compressionType: CompressionType? = null

    constructor(blobPath: String) {
        this.blobPath = blobPath
    }

    constructor(
        blobPath: String,
        compressionType: CompressionType?,
        sourceId: UUID,
    ) {
        this.blobPath = blobPath
        this.compressionType = compressionType
        this.sourceId = sourceId
    }

    override fun validate() {
        require(blobPath.isNotBlank()) { "blobPath cannot be blank" }
    }

    /**
     * Returns the exact size of the blob in bytes if available. This is only
     * set when the blob was created by uploading a local source. Returns null
     * if size is not available (e.g., for external blob URLs).
     */
    fun size(): Long? {
        return blobExactSize
    }

    companion object {
        /**
         * Create BlobSourceInfo from LocalSource (FileSourceInfo or
         * StreamSourceInfo) using BlobUploadContainer
         */
        val logger: Logger
            get() = LoggerFactory.getLogger(BlobSourceInfo::class.java)

        private suspend fun fromLocalSource(
            localSource: LocalSource,
            blobUploadContainer: BlobUploadContainer,
        ): BlobSourceInfo {
            val (inputStream, size, effectiveCompression) =
                localSource.prepareForUpload()
            val blobName = localSource.generateBlobName()
            val blobPath =
                blobUploadContainer.uploadAsync(blobName, inputStream)
            logger.info(
                "Uploading blob to path {} with blob name {}",
                blobPath.split("?").first(),
                blobName,
            )
            return BlobSourceInfo(
                blobPath,
                effectiveCompression,
                localSource.sourceId,
            )
                .apply { blobExactSize = size }
        }

        /**
         * Create BlobSourceInfo from FileSourceInfo using BlobUploadContainer
         */
        suspend fun fromFileSourceInfo(
            fileSourceInfo: FileSourceInfo,
            blobUploadContainer: BlobUploadContainer,
        ): BlobSourceInfo = fromLocalSource(fileSourceInfo, blobUploadContainer)

        /**
         * Create BlobSourceInfo from StreamSourceInfo using BlobUploadContainer
         */
        suspend fun fromStreamSourceInfo(
            streamSourceInfo: StreamSourceInfo,
            blobUploadContainer: BlobUploadContainer,
        ): BlobSourceInfo =
            fromLocalSource(streamSourceInfo, blobUploadContainer)

        // batch convert multiple LocalSource objects to BlobSourceInfo using parallel uploads
        suspend fun fromLocalSourcesBatch(
            localSources: List<LocalSource>,
            blobUploadContainer: BlobUploadContainer,
        ): BatchConversionResult {
            if (localSources.isEmpty()) {
                return BatchConversionResult(emptyList(), emptyList())
            }

            logger.info(
                "Starting batch conversion of {} local sources",
                localSources.size,
            )

            val uploadSources =
                localSources.map { source ->
                    val (inputStream, size, effectiveCompression) =
                        source.prepareForUpload()
                    val blobName = source.generateBlobName()
                    UploadSource(
                        name = blobName,
                        stream = inputStream,
                        sizeBytes = size ?: -1,
                    )
                }

            val uploadResults =
                blobUploadContainer.uploadManyAsync(uploadSources)

            val blobSources = mutableListOf<BlobSourceInfo>()
            val failures = mutableListOf<SourceConversionFailure>()

            val sourceMap = localSources.associateBy { it.generateBlobName() }

            uploadResults.successes.forEach { success ->
                val originalSource = sourceMap[success.sourceName]
                if (originalSource != null) {
                    blobSources.add(
                        BlobSourceInfo(
                            blobPath = success.blobUrl,
                            compressionType =
                            if (
                                originalSource
                                    .compressionType ==
                                CompressionType
                                    .NONE
                            ) {
                                CompressionType
                                    .GZIP // Auto-compressed during
                                // upload
                            } else {
                                originalSource
                                    .compressionType
                            },
                            sourceId = originalSource.sourceId,
                        )
                            .apply { blobExactSize = success.sizeBytes },
                    )
                }
            }

            uploadResults.failures.forEach { failure ->
                val originalSource = sourceMap[failure.sourceName]
                if (originalSource != null) {
                    failures.add(
                        SourceConversionFailure(
                            source = originalSource,
                            errorCode = failure.errorCode,
                            errorMessage = failure.errorMessage,
                            exception = failure.exception,
                            isPermanent = failure.isPermanent,
                        ),
                    )
                }
            }

            logger.info(
                "Batch conversion completed: {} successes, {} failures",
                blobSources.size,
                failures.size,
            )

            return BatchConversionResult(blobSources, failures)
        }
    }
}

/** Represents a failure during source conversion to blob. */
data class SourceConversionFailure(
    val source: LocalSource,
    val errorCode: UploadErrorCode,
    val errorMessage: String,
    val exception: Exception?,
    val isPermanent: Boolean,
)

data class BatchConversionResult(
    override val successes: List<BlobSourceInfo>,
    override val failures: List<SourceConversionFailure>,
) : BatchOperationResult<BlobSourceInfo, SourceConversionFailure>
