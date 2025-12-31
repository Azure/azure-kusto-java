// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class IngestionOperationTest {

    @Test
    fun `IngestionOperation creates correctly with all fields`() {
        val operation = IngestionOperation(
            operationId = "op-12345",
            database = "TestDB",
            table = "TestTable",
            ingestKind = IngestKind.STREAMING
        )
        
        assertEquals("op-12345", operation.operationId)
        assertEquals("TestDB", operation.database)
        assertEquals("TestTable", operation.table)
        assertEquals(IngestKind.STREAMING, operation.ingestKind)
    }

    @Test
    fun `IngestionOperation creates correctly with QUEUED kind`() {
        val operation = IngestionOperation(
            operationId = "op-67890",
            database = "ProductionDB",
            table = "Logs",
            ingestKind = IngestKind.QUEUED
        )
        
        assertEquals("op-67890", operation.operationId)
        assertEquals("ProductionDB", operation.database)
        assertEquals("Logs", operation.table)
        assertEquals(IngestKind.QUEUED, operation.ingestKind)
    }

    @Test
    fun `IngestionOperation data class equality works correctly`() {
        val op1 = IngestionOperation("op-1", "db", "table", IngestKind.STREAMING)
        val op2 = IngestionOperation("op-1", "db", "table", IngestKind.STREAMING)
        val op3 = IngestionOperation("op-2", "db", "table", IngestKind.STREAMING)
        val op4 = IngestionOperation("op-1", "db", "table", IngestKind.QUEUED)
        
        assertEquals(op1, op2)
        assertNotEquals(op1, op3)
        assertNotEquals(op1, op4)
    }

    @Test
    fun `IngestionOperation data class hashCode works correctly`() {
        val op1 = IngestionOperation("op-1", "db", "table", IngestKind.STREAMING)
        val op2 = IngestionOperation("op-1", "db", "table", IngestKind.STREAMING)
        
        assertEquals(op1.hashCode(), op2.hashCode())
    }

    @Test
    fun `IngestionOperation data class copy works correctly`() {
        val original = IngestionOperation("op-1", "db1", "table1", IngestKind.STREAMING)
        val copied = original.copy(database = "db2", table = "table2")
        
        assertEquals("op-1", copied.operationId)
        assertEquals("db2", copied.database)
        assertEquals("table2", copied.table)
        assertEquals(IngestKind.STREAMING, copied.ingestKind)
    }

    @Test
    fun `IngestionOperation copy can change operationId`() {
        val original = IngestionOperation("op-1", "db", "table", IngestKind.STREAMING)
        val copied = original.copy(operationId = "op-2")
        
        assertEquals("op-2", copied.operationId)
        assertEquals(original.database, copied.database)
        assertEquals(original.table, copied.table)
        assertEquals(original.ingestKind, copied.ingestKind)
    }

    @Test
    fun `IngestionOperation copy can change ingestKind`() {
        val original = IngestionOperation("op-1", "db", "table", IngestKind.STREAMING)
        val copied = original.copy(ingestKind = IngestKind.QUEUED)
        
        assertEquals(original.operationId, copied.operationId)
        assertEquals(original.database, copied.database)
        assertEquals(original.table, copied.table)
        assertEquals(IngestKind.QUEUED, copied.ingestKind)
    }

    @Test
    fun `IngestionOperation toString contains all fields`() {
        val operation = IngestionOperation("op-1", "db", "table", IngestKind.STREAMING)
        val stringRep = operation.toString()
        
        assertTrue(stringRep.contains("op-1"))
        assertTrue(stringRep.contains("db"))
        assertTrue(stringRep.contains("table"))
        assertTrue(stringRep.contains("STREAMING"))
    }

    @Test
    fun `IngestionOperation handles special characters in fields`() {
        val operation = IngestionOperation(
            operationId = "op-with-dashes-123",
            database = "Database.With.Dots",
            table = "Table_With_Underscores",
            ingestKind = IngestKind.STREAMING
        )
        
        assertEquals("op-with-dashes-123", operation.operationId)
        assertEquals("Database.With.Dots", operation.database)
        assertEquals("Table_With_Underscores", operation.table)
    }

    @Test
    fun `IngestionOperation handles empty strings`() {
        val operation = IngestionOperation("", "", "", IngestKind.QUEUED)
        
        assertEquals("", operation.operationId)
        assertEquals("", operation.database)
        assertEquals("", operation.table)
        assertEquals(IngestKind.QUEUED, operation.ingestKind)
    }

    @Test
    fun `IngestionOperation component functions work correctly`() {
        val operation = IngestionOperation("op-1", "db", "table", IngestKind.STREAMING)
        
        val (id, db, tbl, kind) = operation
        
        assertEquals("op-1", id)
        assertEquals("db", db)
        assertEquals("table", tbl)
        assertEquals(IngestKind.STREAMING, kind)
    }
}
