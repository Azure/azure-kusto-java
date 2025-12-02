// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import java.io.File
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

    constructor(blobPath: String, compressionType: CompressionType?) {
        this.blobPath = blobPath
        this.compressionType = compressionType
    }

    constructor(
        blobPath: String,
        compressionType: CompressionType?,
        sourceId: UUID?,
    ) {
        this.blobPath = blobPath
        this.compressionType = compressionType
        this.sourceId = sourceId
    }

    override fun validate() {
        require(blobPath.isNotBlank()) { "blobPath cannot be blank" }
    }

    companion object {
        /** For internal usage, adding blobExactSize */
        fun fromFile(
            blobPath: String,
            filePath: String,
            sourceId: UUID?,
            sourceCompressionType: CompressionType?,
            gotCompressed: Boolean,
        ): BlobSourceInfo {
            val blobSourceInfo =
                BlobSourceInfo(
                    blobPath,
                    if (gotCompressed) {
                        CompressionType.GZIP
                    } else {
                        sourceCompressionType
                    },
                    sourceId,
                )
            if (sourceCompressionType == null) {
                blobSourceInfo.blobExactSize = File(filePath).length()
            }
            return blobSourceInfo
        }

        /** For internal usage, adding blobExactSize */
        fun fromStream(
            blobPath: String,
            size: Int,
            sourceId: UUID?,
            compressionType: CompressionType?,
        ): BlobSourceInfo {
            val blobSourceInfo =
                BlobSourceInfo(blobPath, compressionType, sourceId)
            blobSourceInfo.blobExactSize = size.toLong()
            return blobSourceInfo
        }
    }
}
