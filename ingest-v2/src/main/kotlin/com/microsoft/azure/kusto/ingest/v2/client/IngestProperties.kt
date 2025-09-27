// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.source.DataFormat
import kotlinx.serialization.Serializable
import java.time.Instant

/** Validation policy options for ingestion. */
@Serializable
enum class ValidationPolicy {
    NONE,
    TYPE_AND_COUNT,
    TYPE_ONLY,
    COUNT_ONLY,
    ;

    override fun toString(): String = name.lowercase()
}

/** Type-safe wrapper for ingestion mapping names. */
@JvmInline
value class MappingName(val value: String) {
    init {
        require(value.isNotBlank()) { "Mapping name cannot be blank" }
    }
}

/** Type-safe wrapper for zip patterns. */
@JvmInline
value class ZipPattern(val regex: String) {
    init {
        require(regex.isNotBlank()) { "Zip pattern cannot be blank" }
        // Validate regex pattern
        kotlin.runCatching { regex.toRegex() }
            .onFailure {
                throw IllegalArgumentException(
                    "Invalid regex pattern: $regex",
                    it,
                )
            }
    }
}

/** Represents tag collections for ingestion operations. */
data class IngestionTags(
    val ingestByTags: MutableSet<String> = mutableSetOf(),
    val dropByTags: MutableSet<String> = mutableSetOf(),
    val ingestIfNotExistsTags: MutableSet<String> = mutableSetOf(),
    val additionalTags: MutableSet<String> = mutableSetOf(),
) {
    /** Combines all tags into a formatted list for API consumption. */
    internal fun toFormattedList(): List<String> {
        val result = mutableListOf<String>()

        ingestByTags.forEach { result.add("ingest-by:$it") }
        dropByTags.forEach { result.add("drop-by:$it") }
        ingestIfNotExistsTags.forEach { result.add("ingest-if-not-exists:$it") }
        result.addAll(additionalTags)

        return result
    }

    /** Checks if any tags are defined. */
    fun hasAnyTags(): Boolean =
        ingestByTags.isNotEmpty() ||
            dropByTags.isNotEmpty() ||
            ingestIfNotExistsTags.isNotEmpty() ||
            additionalTags.isNotEmpty()
}

/** Configuration for data mapping during ingestion. */
sealed class IngestionMapping {
    /** Reference to a pre-defined mapping by name. */
    data class Reference(val name: MappingName) : IngestionMapping()

    /** Inline mapping definition. */
    data class Inline(val definition: String) : IngestionMapping() {
        init {
            require(definition.isNotBlank()) {
                "Mapping definition cannot be blank"
            }
        }
    }
}

/**
 * Represents the properties for data ingestion in Kusto using proper Kotlin
 * constructs.
 */
data class IngestProperties(
    // DM-based properties
    val enableTracking: Boolean = false,
    val tags: IngestionTags = IngestionTags(),
    val skipBatching: Boolean = false,
    val deleteAfterDownload: Boolean = false,

    // Direct to engine properties
    val mapping: IngestionMapping? = null,
    val validationPolicy: ValidationPolicy? = null,
    val ignoreSizeLimit: Boolean = false,
    val ignoreFirstRecord: Boolean = false,
    val ignoreLastRecordIfInvalid: Boolean = false,
    val creationTime: Instant? = null,
    val zipPattern: ZipPattern? = null,
    val extendSchema: Boolean = false,
    val recreateSchema: Boolean = false,

    // Additional properties that don't fit the typed structure
    private val additionalProperties: Map<String, Any> = emptyMap(),
) {

    companion object {
        // Property keys for backward compatibility and serialization
        private object Keys {
            const val TRACK = "enableTracking"
            const val TAGS = "tags"
            const val SKIP_BATCHING = "skipBatching"
            const val DELETE_AFTER_DOWNLOAD = "deleteAfterDownload"
            const val FORMAT = "format"
            const val MAPPING_REFERENCE = "mappingReference"
            const val MAPPING = "mapping"
            const val VALIDATION_POLICY = "validationPolicy"
            const val IGNORE_SIZE_LIMIT = "ignoreSizeLimit"
            const val IGNORE_FIRST_RECORD = "ignoreFirstRecord"
            const val IGNORE_LAST_RECORD_IF_INVALID =
                "ignoreLastRecordIfInvalid"
            const val CREATION_TIME = "creationTime"
            const val ZIP_PATTERN = "zipPattern"
            const val EXTEND_SCHEMA = "extend_schema"
            const val RECREATE_SCHEMA = "recreate_schema"
        }

        /** Creates an IngestProperties builder for fluent construction. */
        fun builder(): Builder = Builder()
    }

    /** Builder pattern for constructing IngestProperties instances. */
    class Builder {
        private var enableTracking: Boolean = false
        private var tags: IngestionTags = IngestionTags()
        private var skipBatching: Boolean = false
        private var deleteAfterDownload: Boolean = false
        private var mapping: IngestionMapping? = null
        private var validationPolicy: ValidationPolicy? = null
        private var ignoreSizeLimit: Boolean = false
        private var ignoreFirstRecord: Boolean = false
        private var ignoreLastRecordIfInvalid: Boolean = false
        private var creationTime: Instant? = null
        private var zipPattern: ZipPattern? = null
        private var extendSchema: Boolean = false
        private var recreateSchema: Boolean = false
        private val additionalProperties: MutableMap<String, Any> =
            mutableMapOf()

        fun enableTracking(enable: Boolean) = apply {
            this.enableTracking = enable
        }

        fun skipBatching(skip: Boolean) = apply { this.skipBatching = skip }

        fun deleteAfterDownload(delete: Boolean) = apply {
            this.deleteAfterDownload = delete
        }

        fun ignoreSizeLimit(ignore: Boolean) = apply {
            this.ignoreSizeLimit = ignore
        }

        fun ignoreFirstRecord(ignore: Boolean) = apply {
            this.ignoreFirstRecord = ignore
        }

        fun ignoreLastRecordIfInvalid(ignore: Boolean) = apply {
            this.ignoreLastRecordIfInvalid = ignore
        }

        fun extendSchema(extend: Boolean) = apply { this.extendSchema = extend }

        fun recreateSchema(recreate: Boolean) = apply {
            this.recreateSchema = recreate
        }

        fun mapping(mapping: IngestionMapping?) = apply {
            this.mapping = mapping
        }

        fun mappingReference(name: String) = apply {
            this.mapping =
                if (name.isBlank()) {
                    null
                } else {
                    IngestionMapping.Reference(MappingName(name))
                }
        }

        fun mappingDefinition(definition: String) = apply {
            this.mapping =
                if (definition.isBlank()) {
                    null
                } else {
                    IngestionMapping.Inline(definition)
                }
        }

        fun validationPolicy(policy: ValidationPolicy?) = apply {
            this.validationPolicy = policy
        }

        fun creationTime(time: Instant?) = apply { this.creationTime = time }

        fun zipPattern(pattern: String?) = apply {
            this.zipPattern =
                if (pattern.isNullOrBlank()) null else ZipPattern(pattern)
        }

        fun addIngestByTag(tag: String) = apply { tags.ingestByTags.add(tag) }

        fun addDropByTag(tag: String) = apply { tags.dropByTags.add(tag) }

        fun addIngestIfNotExistsTag(tag: String) = apply {
            tags.ingestIfNotExistsTags.add(tag)
        }

        fun addAdditionalTag(tag: String) = apply {
            tags.additionalTags.add(tag)
        }

        fun addProperty(key: String, value: Any) = apply {
            additionalProperties[key] = value
        }

        fun build(): IngestProperties =
            IngestProperties(
                enableTracking = enableTracking,
                tags = tags,
                skipBatching = skipBatching,
                deleteAfterDownload = deleteAfterDownload,
                mapping = mapping,
                validationPolicy = validationPolicy,
                ignoreSizeLimit = ignoreSizeLimit,
                ignoreFirstRecord = ignoreFirstRecord,
                ignoreLastRecordIfInvalid = ignoreLastRecordIfInvalid,
                creationTime = creationTime,
                zipPattern = zipPattern,
                extendSchema = extendSchema,
                recreateSchema = recreateSchema,
                additionalProperties = additionalProperties.toMap(),
            )
    }

    /** Legacy getter for mapping reference (for backward compatibility). */
    val ingestionMappingReference: String?
        get() =
            when (mapping) {
                is IngestionMapping.Reference -> mapping.name.value
                else -> null
            }

    /** Legacy getter for mapping definition (for backward compatibility). */
    val ingestionMapping: String?
        get() =
            when (mapping) {
                is IngestionMapping.Inline -> mapping.definition
                else -> null
            }

    /** Legacy alias for ingestionMappingReference. */
    val mappingName: String?
        get() = ingestionMappingReference

    /** Legacy getters for tag collections (for backward compatibility). */
    val ingestByTags: Set<String>
        get() = tags.ingestByTags.toSet()

    val dropByTags: Set<String>
        get() = tags.dropByTags.toSet()

    val ingestIfNotExistsTags: Set<String>
        get() = tags.ingestIfNotExistsTags.toSet()

    val additionalTags: Set<String>
        get() = tags.additionalTags.toSet()

    /**
     * Returns a dictionary representation of the properties for API
     * consumption.
     */
    internal fun toDictionary(format: DataFormat): Map<String, Any> {
        val result = mutableMapOf<String, Any>()

        // Add format
        result[Keys.FORMAT] = format.kustoValue

        // Add tracking if enabled
        if (enableTracking) {
            result[Keys.TRACK] = true
        }

        // Add tags if any exist
        if (tags.hasAnyTags()) {
            result[Keys.TAGS] = tags.toFormattedList()
        }

        // Add DM properties
        if (skipBatching) result[Keys.SKIP_BATCHING] = true
        if (deleteAfterDownload) result[Keys.DELETE_AFTER_DOWNLOAD] = true

        // Add mapping
        when (mapping) {
            is IngestionMapping.Reference ->
                result[Keys.MAPPING_REFERENCE] = mapping.name.value
            is IngestionMapping.Inline ->
                result[Keys.MAPPING] = mapping.definition
            null -> {
                /* no mapping */
            }
        }

        // Add validation policy
        validationPolicy?.let { result[Keys.VALIDATION_POLICY] = it.toString() }

        // Add engine properties
        if (ignoreSizeLimit) result[Keys.IGNORE_SIZE_LIMIT] = true
        if (ignoreFirstRecord) result[Keys.IGNORE_FIRST_RECORD] = true
        if (ignoreLastRecordIfInvalid) {
            result[Keys.IGNORE_LAST_RECORD_IF_INVALID] = true
        }
        if (extendSchema) result[Keys.EXTEND_SCHEMA] = true
        if (recreateSchema) result[Keys.RECREATE_SCHEMA] = true

        // Add time properties
        creationTime?.let { result[Keys.CREATION_TIME] = it.toString() }

        // Add zip pattern
        zipPattern?.let { result[Keys.ZIP_PATTERN] = it.regex }

        // Add additional properties
        result.putAll(additionalProperties)

        return result
    }

    /** Legacy method for backward compatibility - gets property by key. */
    @Suppress("UNCHECKED_CAST")
    fun <T> tryGetProperty(key: String): T? {
        return when (key) {
            Keys.TRACK -> enableTracking as? T
            Keys.SKIP_BATCHING -> skipBatching as? T
            Keys.DELETE_AFTER_DOWNLOAD -> deleteAfterDownload as? T
            Keys.MAPPING_REFERENCE -> ingestionMappingReference as? T
            Keys.MAPPING -> ingestionMapping as? T
            Keys.VALIDATION_POLICY -> validationPolicy?.toString() as? T
            Keys.IGNORE_SIZE_LIMIT -> ignoreSizeLimit as? T
            Keys.IGNORE_FIRST_RECORD -> ignoreFirstRecord as? T
            Keys.IGNORE_LAST_RECORD_IF_INVALID ->
                ignoreLastRecordIfInvalid as? T
            Keys.CREATION_TIME -> creationTime as? T
            Keys.ZIP_PATTERN -> zipPattern?.regex as? T
            Keys.EXTEND_SCHEMA -> extendSchema as? T
            Keys.RECREATE_SCHEMA -> recreateSchema as? T
            else -> additionalProperties[key] as? T
        }
    }

    /**
     * Legacy method for backward compatibility - gets property with default.
     */
    fun <T> getPropertyOrDefault(key: String, defaultValue: T? = null): T? {
        return tryGetProperty(key) ?: defaultValue
    }
}

/** Extension functions for fluent API usage. */

/** Creates IngestProperties with DSL-style configuration. */
inline fun ingestProperties(
    configure: IngestProperties.Builder.() -> Unit,
): IngestProperties {
    return IngestProperties.builder().apply(configure).build()
}

/**
 * Converts IngestProperties to API IngestRequestProperties format. This
 * centralizes the conversion logic and simplifies the API wrapper.
 */
internal fun IngestProperties.toApiRequestProperties(
    format: com.microsoft.azure.kusto.ingest.v2.models.Format,
): com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties {
    return com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties(
        format = format,
        enableTracking = enableTracking.takeIf { it },
        tags = tags.toFormattedList().takeIf { it.isNotEmpty() },
        ingestIfNotExists =
        tags.ingestIfNotExistsTags.toList().takeIf {
            it.isNotEmpty()
        },
        skipBatching = skipBatching.takeIf { it },
        deleteAfterDownload = deleteAfterDownload.takeIf { it },
        mappingReference = ingestionMappingReference,
        mapping = ingestionMapping,
        validationPolicy = validationPolicy?.toString(),
        ignoreSizeLimit = ignoreSizeLimit.takeIf { it },
        ignoreFirstRecord = ignoreFirstRecord.takeIf { it },
        ignoreLastRecordIfInvalid = ignoreLastRecordIfInvalid.takeIf { it },
        creationTime =
        creationTime?.let {
            java.time.OffsetDateTime.from(
                it.atOffset(
                    java.time.OffsetDateTime.now().offset,
                ),
            )
        },
        zipPattern = zipPattern?.regex,
        extendSchema = extendSchema.takeIf { it },
        recreateSchema = recreateSchema.takeIf { it },
    )
}
