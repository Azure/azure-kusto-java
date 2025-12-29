// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader.compression

import java.io.InputStream

/**
 * Strategy interface for stream compression.
 * Follows the Strategy Design Pattern to allow different compression algorithms.
 */
interface CompressionStrategy {
    /**
     * Compresses the input stream and returns a compressed stream.
     *
     * @param input The input stream to compress
     * @param estimatedSize Optional estimated size of the input for buffer optimization
     * @return A compressed input stream ready for upload
     * @throws CompressionException if compression fails
     */
    suspend fun compress(input: InputStream, estimatedSize: Long = 0): InputStream

    /**
     * Returns the compression type identifier for this strategy.
     */
    val compressionType: String
}
