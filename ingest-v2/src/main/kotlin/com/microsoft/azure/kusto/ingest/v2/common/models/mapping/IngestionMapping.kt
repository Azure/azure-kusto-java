// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models.mapping

import com.microsoft.azure.kusto.ingest.v2.models.Format
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.collections.emptyList
import kotlinx.serialization.Serializable as KSerializable

@KSerializable
class IngestionMapping
private constructor(
    val ingestionMappingReference: String,
    val columnMappings: List<ColumnMapping>,
    val ingestionMappingType: IngestionMappingType,
) {
    constructor(
        other: IngestionMapping,
    ) : this(
        other.ingestionMappingReference,
        other.columnMappings.map {
            ColumnMapping(
                it.columnName,
                columnType = it.columnType,
                properties = it.properties,
            )
        },
        other.ingestionMappingType,
    )

    constructor(
        ingestionMappingReference: String,
        ingestionMappingType: IngestionMappingType,
    ) : this(
        columnMappings = emptyList(),
        ingestionMappingReference = ingestionMappingReference,
        ingestionMappingType = ingestionMappingType,
    )

    constructor(
        columnMappings: List<ColumnMapping>,
        ingestionMappingType: IngestionMappingType,
    ) : this(
        columnMappings = columnMappings,
        ingestionMappingReference = "",
        ingestionMappingType = ingestionMappingType,
    )

    /**
     * Serializes the column mappings to a JSON string representation.
     *
     * @return JSON string representation of the column mappings
     */
    fun serializeColumnMappingsToJson(): String {
        return Json.encodeToString(columnMappings)
    }

    enum class IngestionMappingType(
        val kustoValue: String,
        val format: Format,
    ) {
        CSV("Csv", Format.csv),
        JSON("Json", Format.json),
        AVRO("Avro", Format.avro),
        PARQUET("Parquet", Format.parquet),
        SSTREAM("SStream", Format.sstream),
        ORC("Orc", Format.orc),
        APACHEAVRO("ApacheAvro", Format.apacheavro),
        W3CLOGFILE("W3CLogFile", Format.w3clogfile),
    }
}
