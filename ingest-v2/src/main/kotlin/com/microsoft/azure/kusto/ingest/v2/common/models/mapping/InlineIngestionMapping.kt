// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models.mapping

import kotlinx.serialization.Serializable as KSerializable

@KSerializable
data class InlineIngestionMapping(
    var columnMappings: List<ColumnMapping>? = null,
    var ingestionMappingType: IngestionMappingType? = null
) {
    constructor(
        other: InlineIngestionMapping,
    ) : this(
        other.columnMappings?.map {
            ColumnMapping(
                it.columnName,
                columnType = it.columnType,
                properties = it.properties,
            )
        },
        other.ingestionMappingType
    )

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
