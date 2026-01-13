// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import java.time.OffsetDateTime

/**
 * Builder class for
 * [com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties] that
 * provides a convenient way to construct instances with dropBy and ingestBy
 * tags that are automatically combined into the tags property.
 *
 * Example usage:
 * ```kotlin
 * val properties = IngestRequestPropertiesBuilder.create()
 *     .withDropByTags(listOf("tag1", "tag2"))
 *     .withIngestByTags(listOf("tag3"))
 *     .withEnableTracking(true)
 *     .build()
 * ```
 */
class IngestRequestPropertiesBuilder private constructor() {
    private var format: Format? = null
    private var enableTracking: Boolean? = null
    private var additionalTags: List<String>? = null
    private var dropByTags: List<String>? = null
    private var ingestByTags: List<String>? = null
    private var ingestIfNotExists: List<String>? = null
    private var skipBatching: Boolean? = null
    private var deleteAfterDownload: Boolean? = null
    private var ingestionMappingReference: String? = null
    private var inlineIngestionMapping: String? = null
    private var ingestionMapping: IngestionMapping? = null
    private var validationPolicy: String? = null
    private var ignoreSizeLimit: Boolean? = null
    private var ignoreFirstRecord: Boolean? = null
    private var ignoreLastRecordIfInvalid: Boolean? = null
    private var creationTime: OffsetDateTime? = null
    private var zipPattern: String? = null
    private var extendSchema: Boolean? = null
    private var recreateSchema: Boolean? = null

    companion object {
        @JvmStatic
        fun create(): IngestRequestPropertiesBuilder {
            return IngestRequestPropertiesBuilder()
        }
    }

    fun withEnableTracking(value: Boolean) = apply {
        this.enableTracking = value
    }

    fun withAdditionalTags(value: List<String>) = apply {
        this.additionalTags = value
    }

    /**
     * Sets the drop-by tags. These will be prefixed with "drop-by:" when
     * combined into the tags property. Drop-by tags are used to mark extents
     * that should be dropped during merge operations. See
     * [Kusto drop-by extent tags documentation](https://docs.microsoft.com/azure/kusto/management/extents-overview#drop-by-extent-tags)
     */
    fun withDropByTags(value: List<String>) = apply { this.dropByTags = value }

    /**
     * Sets the ingest-by tags. These will be prefixed with "ingest-by:" when
     * combined into the tags property. Ingest-by tags are used to prevent
     * duplicate ingestion of data with the same tag. See
     * [Kusto ingest-by extent tags documentation](https://docs.microsoft.com/azure/kusto/management/extents-overview#ingest-by-extent-tags)
     */
    fun withIngestByTags(value: List<String>) = apply {
        this.ingestByTags = value
    }

    fun withIngestIfNotExists(value: List<String>) = apply {
        this.ingestIfNotExists = value
    }

    fun withSkipBatching(value: Boolean) = apply { this.skipBatching = value }

    fun withDeleteAfterDownload(value: Boolean) = apply {
        this.deleteAfterDownload = value
    }

    fun withIngestionMapping(value: IngestionMapping) = apply {
        this.ingestionMapping = value
        // Set format from mapping type if not already set
        if (this.format == null) {
            this.format = value.ingestionMappingType.format
        }
        // Only set reference OR inline mapping, not both
        if (value.ingestionMappingReference.isNotBlank()) {
            this.ingestionMappingReference = value.ingestionMappingReference
            this.inlineIngestionMapping = null
        } else if (value.columnMappings.isNotEmpty()) {
            this.inlineIngestionMapping = value.serializeColumnMappingsToJson()
            this.ingestionMappingReference = null
        }
    }

    fun withValidationPolicy(value: String) = apply {
        this.validationPolicy = value
    }

    fun withIgnoreSizeLimit(value: Boolean) = apply {
        this.ignoreSizeLimit = value
    }

    fun withIgnoreFirstRecord(value: Boolean) = apply {
        this.ignoreFirstRecord = value
    }

    fun withIgnoreLastRecordIfInvalid(value: Boolean) = apply {
        this.ignoreLastRecordIfInvalid = value
    }

    fun withCreationTime(value: OffsetDateTime) = apply {
        this.creationTime = value
    }

    fun withZipPattern(value: String) = apply { this.zipPattern = value }

    fun withExtendSchema(value: Boolean) = apply { this.extendSchema = value }

    fun withRecreateSchema(value: Boolean) = apply {
        this.recreateSchema = value
    }

    /**
     * Builds the [IngestRequestProperties] with combined tags from dropByTags,
     * ingestByTags, and additionalTags.
     *
     * The built properties will have database and table information stored in
     * the underlying map for retrieval by client implementations.
     *
     * Note: The format field will be automatically extracted from the
     * IngestionSource by the client implementation during ingestion using
     * withFormatFromSource(). A placeholder value (Format.csv) is used during
     * build and will be overridden with the actual source format.
     *
     * @return The built IngestRequestProperties
     */
    fun build(): IngestRequestProperties {
        // Combine all tags: additional tags + prefixed ingest-by tags + prefixed drop-by tags
        val combinedTags = mutableListOf<String>()
        additionalTags?.let { combinedTags.addAll(it) }
        ingestByTags?.forEach { tag -> combinedTags.add("ingest-by:$tag") }
        dropByTags?.forEach { tag -> combinedTags.add("drop-by:$tag") }
        // Use format if explicitly set, otherwise use placeholder (will be overridden from source)
        val effectiveFormat = format ?: Format.csv
        val properties =
            IngestRequestProperties(
                format = effectiveFormat,
                enableTracking = enableTracking,
                tags = combinedTags.ifEmpty { null },
                ingestIfNotExists = ingestIfNotExists,
                skipBatching = skipBatching,
                deleteAfterDownload = deleteAfterDownload,
                ingestionMappingReference = ingestionMappingReference,
                ingestionMapping = inlineIngestionMapping,
                validationPolicy = validationPolicy,
                ignoreSizeLimit = ignoreSizeLimit,
                ignoreFirstRecord = ignoreFirstRecord,
                ignoreLastRecordIfInvalid = ignoreLastRecordIfInvalid,
                creationTime = creationTime,
                zipPattern = zipPattern,
                extendSchema = extendSchema,
                recreateSchema = recreateSchema,
            )
        // Store database and table in the HashMap for retrieval
        return properties
    }
}
