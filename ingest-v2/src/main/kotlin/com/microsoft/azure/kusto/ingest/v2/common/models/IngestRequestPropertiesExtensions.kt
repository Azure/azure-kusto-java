// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties

/**
 * Extension properties and functions for
 * [com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties].
 *
 * These extensions provide convenient access to database, table, and tag
 * information stored in the IngestRequestProperties.
 */

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

/**
 * Creates a copy of this [IngestRequestProperties] with the format field set
 * from the provided
 * [com.microsoft.azure.kusto.ingest.v2.source.IngestionSource]. This is used
 * internally by ingest clients to inject the source's format.
 *
 * Note: This function preserves the database and table entries stored in the
 * underlying HashMap.
 *
 * @param source The ingestion source from which to extract the format
 * @return A new IngestRequestProperties with the format from the source
 */
fun IngestRequestProperties.withFormatFromSource(
    source: com.microsoft.azure.kusto.ingest.v2.source.IngestionSource,
): IngestRequestProperties {
    val newProperties = this.copy(format = source.format)
    // Copy over HashMap entries (database and table)
    this.forEach { (key, value) -> newProperties[key] = value }
    return newProperties
}
