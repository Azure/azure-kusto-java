// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models.mapping

import kotlinx.serialization.Serializable as KSerializable

@KSerializable
data class IngestionMapping(
    var columnMappings: List<ColumnMapping>? = null,
    var ingestionMappingType: IngestionMappingType? = null,
    var ingestionMappingReference: String? = null,
) {
    // construct for a mapping reference
    constructor(
        ingestionMappingReference: String,
        ingestionMappingType: IngestionMappingType,
    ) : this(null, ingestionMappingType, ingestionMappingReference)

    // create as an inline mapping
    constructor(
        columnMappings: List<ColumnMapping>,
        ingestionMappingType: IngestionMappingType,
    ) : this(columnMappings, ingestionMappingType, null)

    constructor(
        other: IngestionMapping,
    ) : this(
        other.columnMappings?.map {
            ColumnMapping(
                it.columnName,
                columnType = it.columnType,
                properties = it.properties,
            )
        },
        other.ingestionMappingType,
        other.ingestionMappingReference,
    )

    fun setIngestionMappingReference(
        ingestionMappingReference: String,
        ingestionMappingType: IngestionMappingType,
    ) {
        this.ingestionMappingReference = ingestionMappingReference
        this.ingestionMappingType = ingestionMappingType
    }

    fun setIngestionMapping(
        columnMappings: List<ColumnMapping>,
        ingestionMappingType: IngestionMappingType,
    ) {
        this.columnMappings = columnMappings
        this.ingestionMappingType = ingestionMappingType
    }

    enum class IngestionMappingType(val kustoValue: String) {
        CSV("Csv"),
        JSON("Json"),
        AVRO("Avro"),
        PARQUET("Parquet"),
        SSTREAM("SStream"),
        ORC("Orc"),
        APACHEAVRO("ApacheAvro"),
        W3CLOGFILE("W3CLogFile"),
    }
}
