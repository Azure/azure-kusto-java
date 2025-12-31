// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.utils

import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IngestionUtilsTest {

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for uncompressed avro`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.avro, CompressionType.NONE)
        assertEquals(0.55, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for uncompressed apacheavro`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.apacheavro, CompressionType.NONE)
        assertEquals(0.55, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for uncompressed csv`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.csv, CompressionType.NONE)
        assertEquals(0.45, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for compressed csv`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.csv, CompressionType.GZIP)
        assertEquals(3.6, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for uncompressed json`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.json, CompressionType.NONE)
        assertEquals(0.33, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for compressed json`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.json, CompressionType.GZIP)
        assertEquals(3.60, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for compressed multijson`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.multijson, CompressionType.GZIP)
        assertEquals(5.15, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for uncompressed txt`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.txt, CompressionType.NONE)
        assertEquals(0.15, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for compressed txt`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.txt, CompressionType.GZIP)
        assertEquals(1.8, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for compressed psv`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.psv, CompressionType.GZIP)
        assertEquals(1.5, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for uncompressed parquet`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.parquet, CompressionType.NONE)
        assertEquals(3.35, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns default factor for unknown format`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.w3clogfile, CompressionType.NONE)
        assertEquals(1.0, factor, 0.001)
    }

    @Test
    fun `getRowStoreEstimatedFactor returns default factor for null format with no compression`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(null, CompressionType.NONE)
        assertEquals(0.45, factor, 0.001) // Defaults to CSV
    }

    @Test
    fun `getRowStoreEstimatedFactor returns correct factor for null format with compression`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(null, CompressionType.GZIP)
        assertEquals(3.6, factor, 0.001) // Defaults to compressed CSV
    }

    @Test
    fun `getRowStoreEstimatedFactor handles ZIP compression type`() {
        val factor = IngestionUtils.getRowStoreEstimatedFactor(Format.json, CompressionType.ZIP)
        assertEquals(3.60, factor, 0.001)
    }
}
