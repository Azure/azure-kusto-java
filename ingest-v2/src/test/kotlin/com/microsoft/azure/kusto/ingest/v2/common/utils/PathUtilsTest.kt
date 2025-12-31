// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.*

class PathUtilsTest {

    @Test
    fun `sanitizeFileName creates valid name with baseName and sourceId`() {
        val sourceId = UUID.fromString("e493b23d-684f-4f4c-8ba8-3edfaca09427")
        val result = PathUtils.sanitizeFileName("myfile.csv", sourceId)
        
        assertTrue(result.contains("e493b23d-684f-4f4c-8ba8-3edfaca09427"))
        assertTrue(result.contains("myfile-csv"))
    }

    @Test
    fun `sanitizeFileName handles null baseName`() {
        val sourceId = UUID.fromString("e493b23d-684f-4f4c-8ba8-3edfaca09427")
        val result = PathUtils.sanitizeFileName(null, sourceId)
        
        assertEquals("e493b23d-684f-4f4c-8ba8-3edfaca09427", result)
    }

    @Test
    fun `sanitizeFileName handles empty baseName`() {
        val sourceId = UUID.fromString("e493b23d-684f-4f4c-8ba8-3edfaca09427")
        val result = PathUtils.sanitizeFileName("", sourceId)
        
        assertEquals("e493b23d-684f-4f4c-8ba8-3edfaca09427", result)
    }

    @Test
    fun `sanitizeFileName replaces forbidden characters`() {
        val sourceId = UUID.fromString("e493b23d-684f-4f4c-8ba8-3edfaca09427")
        val result = PathUtils.sanitizeFileName("my file@#\$%.csv", sourceId)
        
        assertTrue(result.contains("my-file"))
        assertTrue(result.contains("csv"))
    }

    @Test
    fun `sanitizeFileName truncates long names`() {
        val sourceId = UUID.fromString("e493b23d-684f-4f4c-8ba8-3edfaca09427")
        val longName = "a".repeat(150) + ".csv"
        val result = PathUtils.sanitizeFileName(longName, sourceId)
        
        assertTrue(result.contains("__trunc"))
        assertTrue(result.length <= 160)
    }

    @Test
    fun `createFileNameForUpload generates valid format`() {
        val name = "dataset.csv"
        val result = PathUtils.createFileNameForUpload(name)
        
        assertTrue(result.startsWith("Ingest.V2.Java_"))
        assertTrue(result.endsWith("_dataset.csv"))
        assertTrue(result.contains("_"))
    }

    @Test
    fun `createFileNameForUpload generates unique names`() {
        val name = "test.json"
        val result1 = PathUtils.createFileNameForUpload(name)
        Thread.sleep(10) // Ensure different timestamps
        val result2 = PathUtils.createFileNameForUpload(name)
        
        assertNotEquals(result1, result2)
    }

    @Test
    fun `getBasename extracts filename from windows path`() {
        val result = PathUtils.getBasename("C:\\path\\to\\file.csv.gz")
        assertEquals("file.csv.gz", result)
    }

    @Test
    fun `getBasename extracts filename from unix path`() {
        val result = PathUtils.getBasename("/path/to/file.csv.gz")
        assertEquals("file.csv.gz", result)
    }

    @Test
    fun `getBasename extracts filename from URL`() {
        val result = PathUtils.getBasename("https://example.com/path/to/file.csv.gz")
        assertEquals("file.csv.gz", result)
    }

    @Test
    fun `getBasename handles URL with query parameters`() {
        val result = PathUtils.getBasename("https://example.com/path/file.csv?query=value")
        assertEquals("file.csv", result)
    }

    @Test
    fun `getBasename handles URL with fragment`() {
        val result = PathUtils.getBasename("https://example.com/path/file.csv#section")
        assertEquals("file.csv", result)
    }

    @Test
    fun `getBasename handles URL with semicolon`() {
        val result = PathUtils.getBasename("https://example.com/path/file.csv;jsessionid=123")
        assertEquals("file.csv", result)
    }

    @Test
    fun `getBasename returns null for null input`() {
        val result = PathUtils.getBasename(null)
        assertNull(result)
    }

    @Test
    fun `getBasename returns blank for blank input`() {
        val result = PathUtils.getBasename("   ")
        assertEquals("   ", result)
    }

    @Test
    fun `getBasename handles simple filename`() {
        val result = PathUtils.getBasename("file.csv")
        assertEquals("file.csv", result)
    }

    @Test
    fun `getBasename handles mixed path separators`() {
        val result = PathUtils.getBasename("C:\\path/to\\file.csv")
        assertEquals("file.csv", result)
    }

    @Test
    fun `getBasename handles path ending with separator`() {
        val result = PathUtils.getBasename("/path/to/")
        assertEquals("", result)
    }

    @Test
    fun `getBasename handles blob storage URL`() {
        val result = PathUtils.getBasename("https://account.blob.core.windows.net/container/file.csv.gz?sp=r&st=2024")
        assertEquals("file.csv.gz", result)
    }

    @Test
    fun `sanitizeFileName preserves hyphens and underscores`() {
        val sourceId = UUID.randomUUID()
        val result = PathUtils.sanitizeFileName("my-file_name.csv", sourceId)
        
        assertTrue(result.contains("my-file_name-csv"))
    }

    @Test
    fun `sanitizeFileName preserves alphanumeric characters`() {
        val sourceId = UUID.randomUUID()
        val result = PathUtils.sanitizeFileName("file123ABC.csv", sourceId)
        
        assertTrue(result.contains("file123ABC-csv"))
    }

    @Test
    fun `createFileNameForUpload handles special characters in name`() {
        val name = "my-file@#\$.csv"
        val result = PathUtils.createFileNameForUpload(name)
        
        assertTrue(result.startsWith("Ingest.V2.Java_"))
        assertTrue(result.endsWith("_my-file@#\$.csv"))
    }
}
