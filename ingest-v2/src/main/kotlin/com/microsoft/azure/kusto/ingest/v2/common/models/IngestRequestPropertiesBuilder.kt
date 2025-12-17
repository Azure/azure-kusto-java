// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

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
 * val properties = IngestRequestPropertiesBuilder.create(database = "db", table = "table")
 *     .withFormat(Format.json)
 *     .withDropByTags(listOf("tag1", "tag2"))
 *     .withIngestByTags(listOf("tag3"))
 *     .withEnableTracking(true)
 *     .build()
 * ```
 */
class IngestRequestPropertiesBuilder
private constructor(private val database: String, private val table: String) {
    private var format: com.microsoft.azure.kusto.ingest.v2.models.Format? =
        null
    private var enableTracking: Boolean? = null
    private var additionalTags: List<String>? = null
    private var dropByTags: List<String>? = null
    private var ingestByTags: List<String>? = null
    private var ingestIfNotExists: List<String>? = null
    private var skipBatching: Boolean? = null
    private var deleteAfterDownload: Boolean? = null
    private var ingestionMappingReference: String? = null
    private var ingestionMapping: String? = null
    private var validationPolicy: String? = null
    private var ignoreSizeLimit: Boolean? = null
    private var ignoreFirstRecord: Boolean? = null
    private var ignoreLastRecordIfInvalid: Boolean? = null
    private var creationTime: OffsetDateTime? = null
    private var zipPattern: String? = null
    private var extendSchema: Boolean? = null
    private var recreateSchema: Boolean? = null

    companion object {
        internal const val DATABASE_KEY = "_database"
        internal const val TABLE_KEY = "_table"

        /**
         * Creates a new builder for IngestRequestProperties.
         *
         * @param database The target database name
         * @param table The target table name
         * @return A new IngestRequestPropertiesBuilder instance
         */
        @JvmStatic
        fun create(
            database: String,
            table: String,
        ): IngestRequestPropertiesBuilder {
            return IngestRequestPropertiesBuilder(database, table)
        }
    }

    /**
     * Sets the data format for ingestion.
     *
     * @param value The data format (e.g., Format.json, Format.csv)
     */
    fun withFormat(value: com.microsoft.azure.kusto.ingest.v2.models.Format) =
        apply {
            this.format = value
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

    fun withIngestionMappingReference(value: String) = apply {
        this.ingestionMappingReference = value
    }

    fun withIngestionMapping(value: String) = apply {
        this.ingestionMapping = value
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
     * Builds the
     * [com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties] with
     * combined tags from dropByTags, ingestByTags, and additionalTags.
     *
     * The built properties will have database and table information stored in
     * the underlying map for retrieval by client implementations.
     *
     * @return The built IngestRequestProperties
     * @throws IllegalStateException if format has not been set
     */
    fun build(): IngestRequestProperties {
        requireNotNull(format) {
            "Format must be set before building IngestRequestProperties. Use withFormat() to set it."
        }

        // Combine all tags: additional tags + prefixed ingest-by tags + prefixed drop-by tags
        val combinedTags = mutableListOf<String>()

        additionalTags?.let { combinedTags.addAll(it) }

        ingestByTags?.forEach { tag -> combinedTags.add("ingest-by:$tag") }

        dropByTags?.forEach { tag -> combinedTags.add("drop-by:$tag") }

        val properties =
            IngestRequestProperties(
                format = format!!,
                enableTracking = enableTracking,
                tags = combinedTags.ifEmpty { null },
                ingestIfNotExists = ingestIfNotExists,
                skipBatching = skipBatching,
                deleteAfterDownload = deleteAfterDownload,
                ingestionMappingReference = ingestionMappingReference,
                ingestionMapping = ingestionMapping,
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
        properties.put(DATABASE_KEY, database)
        properties.put(TABLE_KEY, table)

        return properties
    }
}

/**
 * Extension property to extract the database name from IngestRequestProperties.
 */
val IngestRequestProperties.database: String
    get() =
        this.get(IngestRequestPropertiesBuilder.DATABASE_KEY) as? String
            ?: throw IllegalStateException(
                "Database not set in IngestRequestProperties",
            )

/**
 * Extension property to extract the table name from IngestRequestProperties.
 */
val IngestRequestProperties.table: String
    get() =
        this.get(IngestRequestPropertiesBuilder.TABLE_KEY) as? String
            ?: throw IllegalStateException(
                "Table not set in IngestRequestProperties",
            )

/**
 * Extension property to extract drop-by tags from the combined tags list.
 * Returns all tags that start with "drop-by:" prefix.
 */
val IngestRequestProperties.dropByTags: List<String>
    get() =
        tags?.filter { it.startsWith("drop-by:") }
            ?.map { it.removePrefix("drop-by:") } ?: emptyList()

/**
 * Extension property to extract ingest-by tags from the combined tags list.
 * Returns all tags that start with "ingest-by:" prefix.
 */
val IngestRequestProperties.ingestByTags: List<String>
    get() =
        tags?.filter { it.startsWith("ingest-by:") }
            ?.map { it.removePrefix("ingest-by:") } ?: emptyList()

/**
 * Extension property to extract additional (non-prefixed) tags from the
 * combined tags list. Returns all tags that don't start with "drop-by:" or
 * "ingest-by:" prefix.
 */
val IngestRequestProperties.additionalTags: List<String>
    get() =
        tags?.filter {
            !it.startsWith("drop-by:") && !it.startsWith("ingest-by:")
        } ?: emptyList()

/**
 * Creates a copy of this [IngestRequestProperties] with modified tags. Useful
 * for adding or removing drop-by and ingest-by tags without recreating the
 * entire object.
 *
 * @param dropByTags New drop-by tags to replace existing ones (null means keep
 *   existing)
 * @param ingestByTags New ingest-by tags to replace existing ones (null means
 *   keep existing)
 * @param additionalTags New additional tags to replace existing ones (null
 *   means keep existing)
 */
fun IngestRequestProperties.copyWithTags(
    dropByTags: List<String>? = null,
    ingestByTags: List<String>? = null,
    additionalTags: List<String>? = null,
): IngestRequestProperties {
    val newDropByTags = dropByTags ?: this.dropByTags
    val newIngestByTags = ingestByTags ?: this.ingestByTags
    val newAdditionalTags = additionalTags ?: this.additionalTags
    val combinedTags = mutableListOf<String>()
    combinedTags.addAll(newAdditionalTags)
    newIngestByTags.forEach { tag -> combinedTags.add("ingest-by:$tag") }
    newDropByTags.forEach { tag -> combinedTags.add("drop-by:$tag") }
    return this.copy(tags = combinedTags.ifEmpty { null })
}
