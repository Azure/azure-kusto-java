// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.container.BlobUploadContainer
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
    }
}
