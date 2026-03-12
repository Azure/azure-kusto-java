// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.ColumnMapping
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping
import com.microsoft.azure.kusto.ingest.v2.models.Format
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class IngestRequestPropertiesBuilderTest {

    @Test
    fun `build should throw exception when both mapping reference and inline mapping are set`() {
        // Create an IngestionMapping with a reference
        val mappingWithReference =
            IngestionMapping(
                ingestionMappingReference = "my_mapping_ref",
                ingestionMappingType =
                IngestionMapping.IngestionMappingType.CSV,
            )

        // Create an IngestionMapping with column mappings
        val col1 = ColumnMapping("col1", "string")
        col1.setPath("$.col1")
        val col2 = ColumnMapping("col2", "int")
        col2.setPath("$.col2")

        val mappingWithColumns =
            IngestionMapping(
                columnMappings = listOf(col1, col2),
                ingestionMappingType =
                IngestionMapping.IngestionMappingType.CSV,
            )

        // Scenario 1: First set reference, then set inline mapping
        // This should work because withIngestionMapping clears the opposite mapping type
        val properties1 =
            IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(mappingWithReference)
                .withIngestionMapping(mappingWithColumns)
                .build()

        // The last call should have cleared the reference and set inline mapping
        assertNull(properties1.ingestionMappingReference)
        assertNotNull(properties1.ingestionMapping)

        // Scenario 2: First set inline mapping, then set reference
        // This should also work because withIngestionMapping clears the opposite mapping type
        val properties2 =
            IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(mappingWithColumns)
                .withIngestionMapping(mappingWithReference)
                .build()

        // The last call should have cleared the inline mapping and set reference
        assertEquals("my_mapping_ref", properties2.ingestionMappingReference)
        assertNull(properties2.ingestionMapping)
    }

    @Test
    fun `build should succeed when only mapping reference is set`() {
        val mappingWithReference =
            IngestionMapping(
                ingestionMappingReference = "my_mapping_ref",
                ingestionMappingType =
                IngestionMapping.IngestionMappingType.CSV,
            )

        val properties =
            IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(mappingWithReference)
                .build()

        assertEquals("my_mapping_ref", properties.ingestionMappingReference)
        assertNull(properties.ingestionMapping)
    }

    @Test
    fun `build should succeed when only inline mapping is set`() {
        val col1 = ColumnMapping("col1", "string")
        col1.setPath("$.col1")
        val col2 = ColumnMapping("col2", "int")
        col2.setPath("$.col2")

        val mappingWithColumns =
            IngestionMapping(
                columnMappings = listOf(col1, col2),
                ingestionMappingType =
                IngestionMapping.IngestionMappingType.CSV,
            )

        val properties =
            IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(mappingWithColumns)
                .build()

        assertNull(properties.ingestionMappingReference)
        assertNotNull(properties.ingestionMapping)
        assertTrue(properties.ingestionMapping!!.contains("col1"))
        assertTrue(properties.ingestionMapping.contains("col2"))
    }

    @Test
    fun `withIngestionMapping should override previous mapping correctly`() {
        val mappingWithReference =
            IngestionMapping(
                ingestionMappingReference = "my_mapping_ref",
                ingestionMappingType =
                IngestionMapping.IngestionMappingType.CSV,
            )

        val col1 = ColumnMapping("col1", "string")
        col1.setPath("$.col1")

        val mappingWithColumns =
            IngestionMapping(
                columnMappings = listOf(col1),
                ingestionMappingType =
                IngestionMapping.IngestionMappingType.CSV,
            )

        // The last withIngestionMapping call should take precedence
        // and clear the opposite mapping type
        val builder =
            IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(mappingWithReference)

        // At this point, ingestionMappingReference is set, inlineIngestionMapping is null

        builder.withIngestionMapping(mappingWithColumns)

        // Now inlineIngestionMapping should be set, ingestionMappingReference should be null
        val properties = builder.build()

        assertNull(properties.ingestionMappingReference)
        assertNotNull(properties.ingestionMapping)
    }

    @Test
    fun `build should combine tags correctly`() {
        val properties =
            IngestRequestPropertiesBuilder.create()
                .withAdditionalTags(listOf("tag1", "tag2"))
                .withDropByTags(listOf("drop1"))
                .withIngestByTags(listOf("ingest1", "ingest2"))
                .build()

        val tags = properties.tags!!
        assertEquals(5, tags.size)
        assertTrue(tags.contains("tag1"))
        assertTrue(tags.contains("tag2"))
        assertTrue(tags.contains("drop-by:drop1"))
        assertTrue(tags.contains("ingest-by:ingest1"))
        assertTrue(tags.contains("ingest-by:ingest2"))
    }

    @Test
    fun `build should set format from mapping type`() {
        val mappingWithReference =
            IngestionMapping(
                ingestionMappingReference = "my_mapping_ref",
                ingestionMappingType =
                IngestionMapping.IngestionMappingType.JSON,
            )

        val properties =
            IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(mappingWithReference)
                .build()

        assertEquals(Format.json, properties.format)
    }

    @Test
    fun `build should use placeholder format when no mapping is set`() {
        val properties =
            IngestRequestPropertiesBuilder.create()
                .withEnableTracking(true)
                .build()

        assertEquals(Format.csv, properties.format)
    }

    @Test
    fun `build should set all properties correctly`() {
        val properties =
            IngestRequestPropertiesBuilder.create()
                .withEnableTracking(true)
                .withSkipBatching(true)
                .withDeleteAfterDownload(true)
                .withIgnoreSizeLimit(true)
                .withIgnoreFirstRecord(true)
                .withIgnoreLastRecordIfInvalid(true)
                .withExtendSchema(true)
                .withRecreateSchema(true)
                .withZipPattern("*.gz")
                .withValidationPolicy("ValidateOnly")
                .withIngestIfNotExists(listOf("tag1"))
                .build()

        assertEquals(true, properties.enableTracking)
        assertEquals(true, properties.skipBatching)
        assertEquals(true, properties.deleteAfterDownload)
        assertEquals(true, properties.ignoreSizeLimit)
        assertEquals(true, properties.ignoreFirstRecord)
        assertEquals(true, properties.ignoreLastRecordIfInvalid)
        assertEquals(true, properties.extendSchema)
        assertEquals(true, properties.recreateSchema)
        assertEquals("*.gz", properties.zipPattern)
        assertEquals("ValidateOnly", properties.validationPolicy)
        assertEquals(listOf("tag1"), properties.ingestIfNotExists)
    }
}
