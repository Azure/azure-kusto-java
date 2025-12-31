// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ExtendedResponseTypesTest {

    @Test
    fun `IngestKind enum has correct values`() {
        assertEquals(2, IngestKind.values().size)
        assertNotNull(IngestKind.STREAMING)
        assertNotNull(IngestKind.QUEUED)
    }

    @Test
    fun `IngestKind STREAMING has correct name`() {
        assertEquals("STREAMING", IngestKind.STREAMING.name)
    }

    @Test
    fun `IngestKind QUEUED has correct name`() {
        assertEquals("QUEUED", IngestKind.QUEUED.name)
    }

    @Test
    fun `IngestKind valueOf works correctly`() {
        assertEquals(IngestKind.STREAMING, IngestKind.valueOf("STREAMING"))
        assertEquals(IngestKind.QUEUED, IngestKind.valueOf("QUEUED"))
    }

    @Test
    fun `ExtendedIngestResponse creates correctly with STREAMING kind`() {
        val ingestResponse = IngestResponse(ingestionOperationId = "op-123")
        val extended = ExtendedIngestResponse(
            ingestResponse = ingestResponse,
            ingestionType = IngestKind.STREAMING
        )
        
        assertEquals(ingestResponse, extended.ingestResponse)
        assertEquals(IngestKind.STREAMING, extended.ingestionType)
        assertEquals("op-123", extended.ingestResponse.ingestionOperationId)
    }

    @Test
    fun `ExtendedIngestResponse creates correctly with QUEUED kind`() {
        val ingestResponse = IngestResponse(ingestionOperationId = "op-456")
        val extended = ExtendedIngestResponse(
            ingestResponse = ingestResponse,
            ingestionType = IngestKind.QUEUED
        )
        
        assertEquals(ingestResponse, extended.ingestResponse)
        assertEquals(IngestKind.QUEUED, extended.ingestionType)
        assertEquals("op-456", extended.ingestResponse.ingestionOperationId)
    }

    @Test
    fun `ExtendedIngestResponse data class equality works`() {
        val response1 = IngestResponse(ingestionOperationId = "op-123")
        val response2 = IngestResponse(ingestionOperationId = "op-123")
        val response3 = IngestResponse(ingestionOperationId = "op-456")
        
        val extended1 = ExtendedIngestResponse(response1, IngestKind.STREAMING)
        val extended2 = ExtendedIngestResponse(response2, IngestKind.STREAMING)
        val extended3 = ExtendedIngestResponse(response3, IngestKind.STREAMING)
        val extended4 = ExtendedIngestResponse(response1, IngestKind.QUEUED)
        
        assertEquals(extended1, extended2)
        assertNotEquals(extended1, extended3)
        assertNotEquals(extended1, extended4)
    }

    @Test
    fun `ExtendedIngestResponse data class hashCode works`() {
        val response = IngestResponse(ingestionOperationId = "op-123")
        val extended1 = ExtendedIngestResponse(response, IngestKind.STREAMING)
        val extended2 = ExtendedIngestResponse(response.copy(), IngestKind.STREAMING)
        
        assertEquals(extended1.hashCode(), extended2.hashCode())
    }

    @Test
    fun `ExtendedIngestResponse data class copy works`() {
        val response = IngestResponse(ingestionOperationId = "op-123")
        val original = ExtendedIngestResponse(response, IngestKind.STREAMING)
        val copied = original.copy(ingestionType = IngestKind.QUEUED)
        
        assertEquals(original.ingestResponse, copied.ingestResponse)
        assertEquals(IngestKind.STREAMING, original.ingestionType)
        assertEquals(IngestKind.QUEUED, copied.ingestionType)
    }

    @Test
    fun `ExtendedIngestResponse toString includes all fields`() {
        val response = IngestResponse(ingestionOperationId = "op-123")
        val extended = ExtendedIngestResponse(response, IngestKind.STREAMING)
        
        val stringRep = extended.toString()
        assertTrue(stringRep.contains("ingestResponse"))
        assertTrue(stringRep.contains("ingestionType"))
    }
}
