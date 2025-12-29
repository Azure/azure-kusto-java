// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader.compression

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.util.zip.GZIPOutputStream

/**
 * GZIP compression strategy implementation.
 *
 * Compresses input streams using GZIP compression, which is the standard
 * compression format supported by Azure Data Explorer for ingestion.
 *
 * **Memory Considerations:**
 * - For small to medium data (< 100MB), uses in-memory compression
 * - Pre-allocates buffer based on estimated size for efficiency
 * - Uses Dispatchers.IO for blocking compression operations
 *
 * **Thread Safety:** This class is thread-safe and stateless.
 */
class GzipCompressionStrategy : CompressionStrategy {

    private val logger: Logger =
        LoggerFactory.getLogger(GzipCompressionStrategy::class.java)

    override val compressionType: String = "gzip"

    companion object {
        /**
         * Default buffer size when estimated size is unknown. 64KB is a good
         * balance between memory usage and performance.
         */
        private const val DEFAULT_BUFFER_SIZE = 64 * 1024

        /**
         * Maximum initial buffer size to prevent excessive memory allocation.
         * Even for large files, we cap at 10MB initial allocation.
         */
        private const val MAX_INITIAL_BUFFER_SIZE = 10 * 1024 * 1024
    }

    /**
     * Compresses the input stream using GZIP compression.
     *
     * @param input The input stream to compress
     * @param estimatedSize Estimated size of the input for buffer optimization.
     *   If 0 or unknown, uses a default buffer size.
     * @return A ByteArrayInputStream containing the compressed data
     * @throws CompressionException if compression fails
     */
    override suspend fun compress(
        input: InputStream,
        estimatedSize: Long,
    ): InputStream =
        withContext(Dispatchers.IO) {
            try {
                val initialBufferSize =
                    calculateInitialBufferSize(estimatedSize)
                logger.debug(
                    "Starting GZIP compression with initial buffer size: {} bytes (estimated input: {} bytes)",
                    initialBufferSize,
                    estimatedSize,
                )

                val startTime = System.currentTimeMillis()
                val byteArrayOutputStream =
                    ByteArrayOutputStream(initialBufferSize)

                GZIPOutputStream(byteArrayOutputStream).use { gzipStream ->
                    input.copyTo(gzipStream)
                }

                val compressedBytes = byteArrayOutputStream.toByteArray()
                val compressionTime = System.currentTimeMillis() - startTime

                val compressionRatio =
                    if (estimatedSize > 0) {
                        (
                            (
                                1.0 -
                                    compressedBytes.size
                                        .toDouble() /
                                    estimatedSize
                                ) * 100
                            )
                            .coerceIn(0.0, 100.0)
                    } else {
                        0.0
                    }

                logger.debug(
                    "GZIP compression complete: {} bytes -> {} bytes ({}% reduction) in {} ms",
                    estimatedSize,
                    compressedBytes.size,
                    compressionRatio,
                    compressionTime,
                )

                ByteArrayInputStream(compressedBytes)
            } catch (e: IOException) {
                logger.error("GZIP compression failed: {}", e.message)
                throw CompressionException(
                    "Failed to compress stream using GZIP",
                    e,
                )
            } catch (e: OutOfMemoryError) {
                logger.error(
                    "GZIP compression failed due to memory constraints: {}",
                    e.message,
                )
                throw CompressionException(
                    "Insufficient memory to compress stream of estimated size: $estimatedSize bytes",
                    e,
                )
            }
        }

    /**
     * Calculates optimal initial buffer size based on estimated input size.
     *
     * For GZIP compression, typical compression ratios are:
     * - JSON/CSV: 70-90% reduction
     * - Text data: 60-80% reduction
     *
     * We estimate compressed size as ~30% of original to avoid excessive buffer
     * resizing while not over-allocating.
     */
    private fun calculateInitialBufferSize(estimatedSize: Long): Int {
        return when {
            estimatedSize <= 0 -> DEFAULT_BUFFER_SIZE
            estimatedSize < DEFAULT_BUFFER_SIZE -> DEFAULT_BUFFER_SIZE
            else -> {
                // Estimate compressed size as 30% of original, capped at MAX_INITIAL_BUFFER_SIZE
                val estimatedCompressedSize = (estimatedSize * 0.3).toLong()
                minOf(estimatedCompressedSize, MAX_INITIAL_BUFFER_SIZE.toLong())
                    .toInt()
            }
        }
    }
}
