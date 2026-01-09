// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader.compression

import kotlinx.coroutines.runBlocking
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.util.zip.GZIPInputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** Unit tests for compression strategy implementations. */
class CompressionStrategyTest {

    // ==================== GzipCompressionStrategy Tests ====================

    @Test
    fun `GzipCompressionStrategy should have correct compression type`() {
        val strategy = GzipCompressionStrategy()
        assertEquals("gzip", strategy.compressionType)
    }

    @Test
    fun `GzipCompressionStrategy should compress data correctly`() =
        runBlocking {
            val strategy = GzipCompressionStrategy()
            val originalData =
                "Hello, World! This is a test string for compression."
            val inputStream =
                ByteArrayInputStream(originalData.toByteArray())

            val compressedStream =
                strategy.compress(
                    inputStream,
                    originalData.length.toLong(),
                )

            // Verify we can decompress and get original data
            val decompressedData =
                GZIPInputStream(compressedStream)
                    .bufferedReader()
                    .readText()
            assertEquals(originalData, decompressedData)
        }

    @Test
    fun `GzipCompressionStrategy should compress empty stream`() = runBlocking {
        val strategy = GzipCompressionStrategy()
        val inputStream = ByteArrayInputStream(ByteArray(0))

        val compressedStream = strategy.compress(inputStream, 0)

        // Verify we can decompress empty data
        val decompressedData =
            GZIPInputStream(compressedStream).bufferedReader().readText()
        assertEquals("", decompressedData)
    }

    @Test
    fun `GzipCompressionStrategy should handle large data`() = runBlocking {
        val strategy = GzipCompressionStrategy()
        val largeData = "A".repeat(100_000)
        val inputStream = ByteArrayInputStream(largeData.toByteArray())

        val compressedStream =
            strategy.compress(inputStream, largeData.length.toLong())

        // Verify compressed size is smaller than original
        val compressedBytes = compressedStream.readBytes()
        assertTrue(compressedBytes.size < largeData.length)

        // Verify we can decompress and get original data
        val decompressedData =
            GZIPInputStream(ByteArrayInputStream(compressedBytes))
                .bufferedReader()
                .readText()
        assertEquals(largeData, decompressedData)
    }

    @Test
    fun `GzipCompressionStrategy should work with unknown estimated size`() =
        runBlocking {
            val strategy = GzipCompressionStrategy()
            val originalData = "Test data without estimated size"
            val inputStream =
                ByteArrayInputStream(originalData.toByteArray())

            // Use 0 as estimated size (unknown)
            val compressedStream = strategy.compress(inputStream, 0)

            val decompressedData =
                GZIPInputStream(compressedStream)
                    .bufferedReader()
                    .readText()
            assertEquals(originalData, decompressedData)
        }

    @Test
    fun `GzipCompressionStrategy should work with negative estimated size`() =
        runBlocking {
            val strategy = GzipCompressionStrategy()
            val originalData = "Test data with negative estimated size"
            val inputStream =
                ByteArrayInputStream(originalData.toByteArray())

            // Use negative as estimated size (invalid)
            val compressedStream = strategy.compress(inputStream, -1)

            val decompressedData =
                GZIPInputStream(compressedStream)
                    .bufferedReader()
                    .readText()
            assertEquals(originalData, decompressedData)
        }

    @Test
    fun `GzipCompressionStrategy should handle binary data`() = runBlocking {
        val strategy = GzipCompressionStrategy()
        val binaryData = ByteArray(256) { it.toByte() }
        val inputStream = ByteArrayInputStream(binaryData)

        val compressedStream =
            strategy.compress(inputStream, binaryData.size.toLong())

        val decompressedData = GZIPInputStream(compressedStream).readBytes()
        assertTrue(binaryData.contentEquals(decompressedData))
    }

    @Test
    fun `GzipCompressionStrategy should throw CompressionException on IO error`(): Unit = runBlocking {
        val strategy = GzipCompressionStrategy()
        val failingStream =
            object : InputStream() {
                override fun read(): Int {
                    throw IOException("Simulated IO error")
                }
            }

        assertFailsWith<CompressionException> {
            strategy.compress(failingStream, 100)
        }
    }

    // ==================== NoCompressionStrategy Tests ====================

    @Test
    fun `NoCompressionStrategy should have correct compression type`() {
        val strategy = NoCompressionStrategy()
        assertEquals("none", strategy.compressionType)
    }

    @Test
    fun `NoCompressionStrategy should return same stream`() = runBlocking {
        val strategy = NoCompressionStrategy()
        val originalData = "Test data"
        val inputStream = ByteArrayInputStream(originalData.toByteArray())

        val resultStream =
            strategy.compress(inputStream, originalData.length.toLong())

        // Should be the exact same stream instance
        assertSame(inputStream, resultStream)
    }

    @Test
    fun `NoCompressionStrategy should pass through data unchanged`() =
        runBlocking {
            val strategy = NoCompressionStrategy()
            val originalData = "Unchanged data"
            val inputStream =
                ByteArrayInputStream(originalData.toByteArray())

            val resultStream =
                strategy.compress(
                    inputStream,
                    originalData.length.toLong(),
                )
            val resultData = resultStream.bufferedReader().readText()

            assertEquals(originalData, resultData)
        }

    @Test
    fun `NoCompressionStrategy INSTANCE should be singleton`() {
        val instance1 = NoCompressionStrategy.INSTANCE
        val instance2 = NoCompressionStrategy.INSTANCE

        assertSame(instance1, instance2)
    }

    @Test
    fun `NoCompressionStrategy should handle empty stream`() = runBlocking {
        val strategy = NoCompressionStrategy()
        val inputStream = ByteArrayInputStream(ByteArray(0))

        val resultStream = strategy.compress(inputStream, 0)

        assertSame(inputStream, resultStream)
        assertEquals(0, resultStream.available())
    }

    @Test
    fun `NoCompressionStrategy should ignore estimated size`() = runBlocking {
        val strategy = NoCompressionStrategy()
        val originalData = "Test"
        val inputStream = ByteArrayInputStream(originalData.toByteArray())

        // Any estimated size should work the same
        val resultStream = strategy.compress(inputStream, 999999)

        assertSame(inputStream, resultStream)
    }

    // ==================== CompressionException Tests ====================

    @Test
    fun `CompressionException should store message`() {
        val exception = CompressionException("Test error message")
        assertEquals("Test error message", exception.message)
    }

    @Test
    fun `CompressionException should store cause`() {
        val cause = IOException("Original error")
        val exception = CompressionException("Wrapper message", cause)

        assertEquals("Wrapper message", exception.message)
        assertSame(cause, exception.cause)
    }

    @Test
    fun `CompressionException should allow null cause`() {
        val exception = CompressionException("Message only")

        assertEquals("Message only", exception.message)
        assertEquals(null, exception.cause)
    }
}
