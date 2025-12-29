// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader.compression

import java.io.InputStream

/**
 * No-op compression strategy that passes through data unchanged.
 *
 * Used for:
 * - Binary formats (Parquet, Avro, ORC) that have built-in compression
 * - Already compressed data (GZIP, ZIP)
 * - Cases where compression is explicitly disabled
 *
 * **Thread Safety:** This class is thread-safe and stateless.
 */
class NoCompressionStrategy : CompressionStrategy {

    override val compressionType: String = "none"

    companion object {
        /** Singleton instance for reuse (class is stateless). */
        val INSTANCE: NoCompressionStrategy by lazy { NoCompressionStrategy() }
    }

    /**
     * Returns the input stream unchanged.
     *
     * @param input The input stream (returned as-is)
     * @param estimatedSize Ignored for no-op compression
     * @return The same input stream unchanged
     */
    override suspend fun compress(
        input: InputStream,
        estimatedSize: Long,
    ): InputStream = input
}
