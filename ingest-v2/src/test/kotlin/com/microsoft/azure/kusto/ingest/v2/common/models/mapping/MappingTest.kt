// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models.mapping

import com.microsoft.azure.kusto.ingest.v2.models.Format
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Unit tests for mapping classes. */
class MappingTest {

    // ==================== ColumnMapping Tests ====================

    @Test
    fun `ColumnMapping should store column name and type`() {
        val mapping =
            ColumnMapping(columnName = "TestColumn", columnType = "string")

        assertEquals("TestColumn", mapping.columnName)
        assertEquals("string", mapping.columnType)
    }

    @Test
    fun `ColumnMapping should set and get path`() {
        val mapping = ColumnMapping("col1", "string")

        mapping.setPath("$.data.value")
        assertEquals("$.data.value", mapping.getPath())
    }

    @Test
    fun `ColumnMapping getPath should return null when not set`() {
        val mapping = ColumnMapping("col1", "string")
        assertNull(mapping.getPath())
    }

    @Test
    fun `ColumnMapping should set and get transform`() {
        val mapping = ColumnMapping("col1", "string")

        mapping.setTransform(TransformationMethod.SourceLineNumber)
        assertEquals(
            TransformationMethod.SourceLineNumber,
            mapping.getTransform(),
        )
    }

    @Test
    fun `ColumnMapping getTransform should return null when not set`() {
        val mapping = ColumnMapping("col1", "string")
        assertNull(mapping.getTransform())
    }

    @Test
    fun `ColumnMapping getTransform should return null for blank transform`() {
        val mapping = ColumnMapping("col1", "string")
        mapping.properties[MappingConstants.Transform.name] = ""
        assertNull(mapping.getTransform())
    }

    @Test
    fun `ColumnMapping should set and get ordinal`() {
        val mapping = ColumnMapping("col1", "string")

        mapping.setOrdinal(5)
        assertEquals(5, mapping.getOrdinal())
    }

    @Test
    fun `ColumnMapping getOrdinal should return null when not set`() {
        val mapping = ColumnMapping("col1", "string")
        assertNull(mapping.getOrdinal())
    }

    @Test
    fun `ColumnMapping getOrdinal should return null for blank ordinal`() {
        val mapping = ColumnMapping("col1", "string")
        mapping.properties[MappingConstants.Ordinal.name] = ""
        assertNull(mapping.getOrdinal())
    }

    @Test
    fun `ColumnMapping should set and get constant value`() {
        val mapping = ColumnMapping("col1", "string")

        mapping.setConstantValue("constant-value")
        assertEquals("constant-value", mapping.getConstantValue())
    }

    @Test
    fun `ColumnMapping getConstantValue should return null when not set`() {
        val mapping = ColumnMapping("col1", "string")
        assertNull(mapping.getConstantValue())
    }

    @Test
    fun `ColumnMapping should set and get field`() {
        val mapping = ColumnMapping("col1", "string")

        mapping.setField("fieldName")
        assertEquals("fieldName", mapping.getField())
    }

    @Test
    fun `ColumnMapping getField should return null when not set`() {
        val mapping = ColumnMapping("col1", "string")
        assertNull(mapping.getField())
    }

    @Test
    fun `ColumnMapping should set and get columns`() {
        val mapping = ColumnMapping("col1", "string")

        mapping.setColumns("col1,col2,col3")
        assertEquals("col1,col2,col3", mapping.getColumns())
    }

    @Test
    fun `ColumnMapping getColumns should return null when not set`() {
        val mapping = ColumnMapping("col1", "string")
        assertNull(mapping.getColumns())
    }

    @Test
    fun `ColumnMapping should set and get storage data type`() {
        val mapping = ColumnMapping("col1", "string")

        mapping.setStorageDataType("int64")
        assertEquals("int64", mapping.getStorageDataType())
    }

    @Test
    fun `ColumnMapping getStorageDataType should return null when not set`() {
        val mapping = ColumnMapping("col1", "string")
        assertNull(mapping.getStorageDataType())
    }

    // ==================== ColumnMapping isValid Tests ====================

    @Test
    fun `ColumnMapping isValid for CSV should require non-blank columnName`() {
        val validMapping = ColumnMapping("col1", "string")
        assertTrue(validMapping.isValid(Format.csv))

        val invalidMapping = ColumnMapping("", "string")
        assertFalse(invalidMapping.isValid(Format.csv))
    }

    @Test
    fun `ColumnMapping isValid for sstream should require non-blank columnName`() {
        val validMapping = ColumnMapping("col1", "string")
        assertTrue(validMapping.isValid(Format.sstream))

        val invalidMapping = ColumnMapping("", "string")
        assertFalse(invalidMapping.isValid(Format.sstream))
    }

    @Test
    fun `ColumnMapping isValid for JSON should require columnName and path`() {
        val validMapping = ColumnMapping("col1", "string")
        validMapping.setPath("$.data")
        assertTrue(validMapping.isValid(Format.json))

        val invalidNoPath = ColumnMapping("col1", "string")
        assertFalse(invalidNoPath.isValid(Format.json))

        val invalidNoName = ColumnMapping("", "string")
        invalidNoName.setPath("$.data")
        assertFalse(invalidNoName.isValid(Format.json))
    }

    @Test
    fun `ColumnMapping isValid for JSON should accept SourceLineNumber transform without path`() {
        val mapping = ColumnMapping("col1", "long")
        mapping.setTransform(TransformationMethod.SourceLineNumber)
        assertTrue(mapping.isValid(Format.json))
    }

    @Test
    fun `ColumnMapping isValid for JSON should accept SourceLocation transform without path`() {
        val mapping = ColumnMapping("col1", "string")
        mapping.setTransform(TransformationMethod.SourceLocation)
        assertTrue(mapping.isValid(Format.json))
    }

    @Test
    fun `ColumnMapping isValid for parquet should require columnName and path`() {
        val validMapping = ColumnMapping("col1", "string")
        validMapping.setPath("$.data")
        assertTrue(validMapping.isValid(Format.parquet))

        val invalidNoPath = ColumnMapping("col1", "string")
        assertFalse(invalidNoPath.isValid(Format.parquet))
    }

    @Test
    fun `ColumnMapping isValid for orc should require columnName and path`() {
        val validMapping = ColumnMapping("col1", "string")
        validMapping.setPath("$.data")
        assertTrue(validMapping.isValid(Format.orc))
    }

    @Test
    fun `ColumnMapping isValid for w3clogfile should require columnName and path`() {
        val validMapping = ColumnMapping("col1", "string")
        validMapping.setPath("$.field")
        assertTrue(validMapping.isValid(Format.w3clogfile))
    }

    @Test
    fun `ColumnMapping isValid for avro should require columnName and columns`() {
        val validMapping = ColumnMapping("col1", "string")
        validMapping.setColumns("avroCol1,avroCol2")
        assertTrue(validMapping.isValid(Format.avro))

        val invalidNoColumns = ColumnMapping("col1", "string")
        assertFalse(invalidNoColumns.isValid(Format.avro))
    }

    @Test
    fun `ColumnMapping isValid for apacheavro should require columnName and columns`() {
        val validMapping = ColumnMapping("col1", "string")
        validMapping.setColumns("avroCol1")
        assertTrue(validMapping.isValid(Format.apacheavro))
    }

    @Test
    fun `ColumnMapping isValid should return false for unsupported format`() {
        val mapping = ColumnMapping("col1", "string")
        // txt format doesn't have specific validation rules in the switch
        assertFalse(mapping.isValid(Format.txt))
    }

    // ==================== ColumnMapping data class Tests ====================

    @Test
    fun `ColumnMapping should support equals and hashCode`() {
        val mapping1 = ColumnMapping("col1", "string")
        val mapping2 = ColumnMapping("col1", "string")

        assertEquals(mapping1, mapping2)
        assertEquals(mapping1.hashCode(), mapping2.hashCode())
    }

    @Test
    fun `ColumnMapping should support copy`() {
        val original = ColumnMapping("col1", "string")
        original.setPath("$.path")

        val copied = original.copy(columnName = "col2")

        assertEquals("col2", copied.columnName)
        assertEquals("string", copied.columnType)
        assertEquals("$.path", copied.getPath())
    }

    // ==================== TransformationMethod Tests ====================

    @Test
    fun `TransformationMethod should have all expected values`() {
        val values = TransformationMethod.values()
        assertTrue(values.contains(TransformationMethod.None))
        assertTrue(
            values.contains(
                TransformationMethod.PropertyBagArrayToDictionary,
            ),
        )
        assertTrue(values.contains(TransformationMethod.SourceLocation))
        assertTrue(values.contains(TransformationMethod.SourceLineNumber))
        assertTrue(values.contains(TransformationMethod.GetPathElement))
        assertTrue(values.contains(TransformationMethod.UnknownMethod))
        assertTrue(
            values.contains(TransformationMethod.DateTimeFromUnixSeconds),
        )
        assertTrue(
            values.contains(
                TransformationMethod.DateTimeFromUnixMilliseconds,
            ),
        )
    }

    @Test
    fun `TransformationMethod valueOf should return correct enum`() {
        assertEquals(
            TransformationMethod.None,
            TransformationMethod.valueOf("None"),
        )
        assertEquals(
            TransformationMethod.SourceLineNumber,
            TransformationMethod.valueOf("SourceLineNumber"),
        )
    }

    // ==================== InlineIngestionMapping Tests ====================

    @Test
    fun `InlineIngestionMapping should store column mappings and type`() {
        val columnMappings =
            listOf(
                ColumnMapping("col1", "string"),
                ColumnMapping("col2", "int"),
            )

        val mapping =
            InlineIngestionMapping(
                columnMappings = columnMappings,
                ingestionMappingType =
                InlineIngestionMapping.IngestionMappingType.JSON,
            )

        assertEquals(2, mapping.columnMappings?.size)
        assertEquals(
            InlineIngestionMapping.IngestionMappingType.JSON,
            mapping.ingestionMappingType,
        )
    }

    @Test
    fun `InlineIngestionMapping should support null values`() {
        val mapping = InlineIngestionMapping()

        assertNull(mapping.columnMappings)
        assertNull(mapping.ingestionMappingType)
    }

    @Test
    fun `InlineIngestionMapping copy constructor should create deep copy`() {
        val columnMappings =
            listOf(
                ColumnMapping("col1", "string").apply {
                    setPath("$.data")
                },
            )

        val original =
            InlineIngestionMapping(
                columnMappings = columnMappings,
                ingestionMappingType =
                InlineIngestionMapping.IngestionMappingType.JSON,
            )

        val copied = InlineIngestionMapping(original)

        assertEquals(original.columnMappings?.size, copied.columnMappings?.size)
        assertEquals(original.ingestionMappingType, copied.ingestionMappingType)
        assertEquals("col1", copied.columnMappings?.get(0)?.columnName)
    }

    @Test
    fun `InlineIngestionMapping copy constructor should handle null columnMappings`() {
        val original =
            InlineIngestionMapping(
                columnMappings = null,
                ingestionMappingType =
                InlineIngestionMapping.IngestionMappingType.CSV,
            )

        val copied = InlineIngestionMapping(original)

        assertNull(copied.columnMappings)
        assertEquals(
            InlineIngestionMapping.IngestionMappingType.CSV,
            copied.ingestionMappingType,
        )
    }

    // ==================== IngestionMappingType Tests ====================

    @Test
    fun `IngestionMappingType CSV should have correct kusto value`() {
        assertEquals(
            "Csv",
            InlineIngestionMapping.IngestionMappingType.CSV.kustoValue,
        )
    }

    @Test
    fun `IngestionMappingType JSON should have correct kusto value`() {
        assertEquals(
            "Json",
            InlineIngestionMapping.IngestionMappingType.JSON.kustoValue,
        )
    }

    @Test
    fun `IngestionMappingType AVRO should have correct kusto value`() {
        assertEquals(
            "Avro",
            InlineIngestionMapping.IngestionMappingType.AVRO.kustoValue,
        )
    }

    @Test
    fun `IngestionMappingType PARQUET should have correct kusto value`() {
        assertEquals(
            "Parquet",
            InlineIngestionMapping.IngestionMappingType.PARQUET.kustoValue,
        )
    }

    @Test
    fun `IngestionMappingType SSTREAM should have correct kusto value`() {
        assertEquals(
            "SStream",
            InlineIngestionMapping.IngestionMappingType.SSTREAM.kustoValue,
        )
    }

    @Test
    fun `IngestionMappingType ORC should have correct kusto value`() {
        assertEquals(
            "Orc",
            InlineIngestionMapping.IngestionMappingType.ORC.kustoValue,
        )
    }

    @Test
    fun `IngestionMappingType APACHEAVRO should have correct kusto value`() {
        assertEquals(
            "ApacheAvro",
            InlineIngestionMapping.IngestionMappingType.APACHEAVRO
                .kustoValue,
        )
    }

    @Test
    fun `IngestionMappingType W3CLOGFILE should have correct kusto value`() {
        assertEquals(
            "W3CLogFile",
            InlineIngestionMapping.IngestionMappingType.W3CLOGFILE
                .kustoValue,
        )
    }

    @Test
    fun `IngestionMappingType should have all expected values`() {
        val values = InlineIngestionMapping.IngestionMappingType.values()
        assertEquals(8, values.size)
    }

    // ==================== MappingConstants Tests ====================

    @Test
    fun `MappingConstants should have expected constant names`() {
        assertEquals("Path", MappingConstants.Path.name)
        assertEquals("Transform", MappingConstants.Transform.name)
        assertEquals("Ordinal", MappingConstants.Ordinal.name)
        assertEquals("ConstValue", MappingConstants.ConstValue.name)
        assertEquals("Field", MappingConstants.Field.name)
        assertEquals("Columns", MappingConstants.Columns.name)
        assertEquals("StorageDataType", MappingConstants.StorageDataType.name)
    }
}
