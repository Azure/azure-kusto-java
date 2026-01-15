// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.utils

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.*

class PathUtilsTest {

    @Test
    fun `sanitizeFileName creates valid name with sourceId`() {
        val sourceId = UUID.fromString("e493b23d-684f-4f4c-8ba8-3edfaca09427")
        val result = PathUtils.sanitizeFileName(sourceId)

        assertTrue(result.contains("e493b23d-684f-4f4c-8ba8-3edfaca09427"))
    }

    @Test
    fun `sanitizeFileName returns sanitized sourceId`() {
        val sourceId = UUID.fromString("e493b23d-684f-4f4c-8ba8-3edfaca09427")
        val result = PathUtils.sanitizeFileName(sourceId)

        assertEquals("e493b23d-684f-4f4c-8ba8-3edfaca09427", result)
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
        val result =
            PathUtils.getBasename("https://example.com/path/to/file.csv.gz")
        assertEquals("file.csv.gz", result)
    }

    @Test
    fun `getBasename handles URL with query parameters`() {
        val result =
            PathUtils.getBasename(
                "https://example.com/path/file.csv?query=value",
            )
        assertEquals("file.csv", result)
    }

    @Test
    fun `getBasename handles URL with fragment`() {
        val result =
            PathUtils.getBasename(
                "https://example.com/path/file.csv#section",
            )
        assertEquals("file.csv", result)
    }

    @Test
    fun `getBasename handles URL with semicolon`() {
        val result =
            PathUtils.getBasename(
                "https://example.com/path/file.csv;jsessionid=123",
            )
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
        val result =
            PathUtils.getBasename(
                "https://account.blob.core.windows.net/container/file.csv.gz?sp=r&st=2024",
            )
        assertEquals("file.csv.gz", result)
    }

    @Test
    fun `createFileNameForUpload handles special characters in name`() {
        val name = "my-file@#\$.csv"
        val result = PathUtils.createFileNameForUpload(name)

        assertTrue(result.startsWith("Ingest.V2.Java_"))
        assertTrue(result.endsWith("_my-file@#\$.csv"))
    }
}
