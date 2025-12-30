// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.models.Format
import java.io.ByteArrayInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.io.TempDir

/**
 * Unit tests for Source classes (FileSource, StreamSource, BlobSource).
 */
class SourceClassesTest {

    @Test
    fun `FileSource should detect GZIP compression from file extension`(@TempDir tempDir: Path) {
        val testFile = tempDir.resolve("test.json.gz")
        Files.write(testFile, byteArrayOf(1, 2, 3))

        val source = FileSource(testFile, Format.json)

        assertEquals(CompressionType.GZIP, source.compressionType)
        assertEquals(Format.json, source.format)
        assertNotNull(source.sourceId)
    }

    @Test
    fun `FileSource should detect ZIP compression from file extension`(@TempDir tempDir: Path) {
        val testFile = tempDir.resolve("test.json.zip")
        Files.write(testFile, byteArrayOf(1, 2, 3))

        val source = FileSource(testFile, Format.json)

        assertEquals(CompressionType.ZIP, source.compressionType)
    }

    @Test
    fun `FileSource should default to NONE compression for uncompressed files`(@TempDir tempDir: Path) {
        val testFile = tempDir.resolve("test.json")
        Files.write(testFile, byteArrayOf(1, 2, 3))

        val source = FileSource(testFile, Format.json)

        assertEquals(CompressionType.NONE, source.compressionType)
    }

    @Test
    fun `FileSource should allow explicit compression type override`(@TempDir tempDir: Path) {
        val testFile = tempDir.resolve("test.json")
        Files.write(testFile, byteArrayOf(1, 2, 3))

        val source = FileSource(testFile, Format.json, CompressionType.GZIP)

        assertEquals(CompressionType.GZIP, source.compressionType)
    }

    @Test
    fun `FileSource should return file size`(@TempDir tempDir: Path) {
        val testData = "Test data".toByteArray()
        val testFile = tempDir.resolve("test.json")
        Files.write(testFile, testData)

        val source = FileSource(testFile, Format.json)

        assertEquals(testData.size.toLong(), source.size())
    }

    @Test
    fun `FileSource should use custom sourceId when provided`(@TempDir tempDir: Path) {
        val testFile = tempDir.resolve("test.json")
        Files.write(testFile, byteArrayOf(1, 2, 3))
        val customId = UUID.randomUUID()

        val source = FileSource(testFile, Format.json, CompressionType.NONE, customId)

        assertEquals(customId, source.sourceId)
    }

    @Test
    fun `StreamSource should create with basic parameters`() {
        val testData = "Test data".toByteArray()
        val stream = ByteArrayInputStream(testData)

        val source = StreamSource(
            stream = stream,
            sourceCompression = CompressionType.NONE,
            format = Format.json,
            sourceId = UUID.randomUUID(),
            name = "test-stream",
            leaveOpen = false
        )

        assertEquals(CompressionType.NONE, source.compressionType)
        assertEquals(Format.json, source.format)
        assertNotNull(source.data())
    }

    @Test
    fun `BlobSource should create with blob path and format`() {
        val blobPath = "https://storage.blob.core.windows.net/container/blob.json"

        val source = BlobSource(
            blobPath = blobPath,
            format = Format.json,
            compressionType = CompressionType.NONE,
            sourceId = UUID.randomUUID()
        )

        assertEquals(blobPath, source.blobPath)
        assertEquals(Format.json, source.format)
        assertEquals(CompressionType.NONE, source.compressionType)
    }

    @Test
    fun `CompressionType enum should have expected values`() {
        assertEquals(3, CompressionType.values().size)
        assertTrue(CompressionType.values().contains(CompressionType.NONE))
        assertTrue(CompressionType.values().contains(CompressionType.GZIP))
        assertTrue(CompressionType.values().contains(CompressionType.ZIP))
    }

    @Test
    fun `FileSource detectCompressionFromPath should handle various extensions`() {
        val gzPath = Path.of("test.json.gz")
        assertEquals(CompressionType.GZIP, FileSource.detectCompressionFromPath(gzPath))

        val zipPath = Path.of("test.json.zip")
        assertEquals(CompressionType.ZIP, FileSource.detectCompressionFromPath(zipPath))

        val plainPath = Path.of("test.json")
        assertEquals(CompressionType.NONE, FileSource.detectCompressionFromPath(plainPath))
    }

    @Test
    fun `FormatUtil should correctly identify binary formats`() {
        assertTrue(FormatUtil.isBinaryFormat(Format.avro))
        assertTrue(FormatUtil.isBinaryFormat(Format.parquet))
        assertTrue(FormatUtil.isBinaryFormat(Format.orc))

        assertTrue(!FormatUtil.isBinaryFormat(Format.json))
        assertTrue(!FormatUtil.isBinaryFormat(Format.csv))
        assertTrue(!FormatUtil.isBinaryFormat(Format.tsv))
    }
}
