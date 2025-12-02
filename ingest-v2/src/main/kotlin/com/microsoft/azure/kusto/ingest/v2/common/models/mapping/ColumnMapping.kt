// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models.mapping

import com.microsoft.azure.kusto.ingest.v2.models.Format
import kotlinx.serialization.Serializable as KSerializable

@KSerializable
data class ColumnMapping(
    val columnName: String,
    val columnType: String,
    val properties: MutableMap<String, String> = mutableMapOf(),
) {

    fun setPath(path: String) {
        properties[MappingConstants.Path.name] = path
    }

    fun getPath(): String? = properties[MappingConstants.Path.name]

    fun setTransform(transform: TransformationMethod) {
        properties[MappingConstants.Transform.name] = transform.name
    }

    fun getTransform(): TransformationMethod? {
        val transform = properties[MappingConstants.Transform.name]
        return if (transform.isNullOrBlank()) {
            null
        } else {
            TransformationMethod.valueOf(transform)
        }
    }

    fun setOrdinal(ordinal: Int) {
        properties[MappingConstants.Ordinal.name] = ordinal.toString()
    }

    fun getOrdinal(): Int? {
        val ordinal = properties[MappingConstants.Ordinal.name]
        return if (ordinal.isNullOrBlank()) null else ordinal.toInt()
    }

    fun setConstantValue(constValue: String) {
        properties[MappingConstants.ConstValue.name] = constValue
    }

    fun getConstantValue(): String? =
        properties[MappingConstants.ConstValue.name]

    fun setField(field: String) {
        properties[MappingConstants.Field.name] = field
    }

    fun getField(): String? = properties[MappingConstants.Field.name]

    fun setColumns(columns: String) {
        properties[MappingConstants.Columns.name] = columns
    }

    fun getColumns(): String? = properties[MappingConstants.Columns.name]

    fun setStorageDataType(dataType: String) {
        properties[MappingConstants.StorageDataType.name] = dataType
    }

    fun getStorageDataType(): String? =
        properties[MappingConstants.StorageDataType.name]

    fun isValid(mappingKind: Format): Boolean {
        return when (mappingKind) {
            Format.csv,
            Format.sstream,
            -> columnName.isNotBlank()
            Format.json,
            Format.parquet,
            Format.orc,
            Format.w3clogfile,
            -> {
                val transformationMethod = getTransform()
                columnName.isNotBlank() &&
                    (
                        !getPath().isNullOrBlank() ||
                            transformationMethod ==
                            TransformationMethod.SourceLineNumber ||
                            transformationMethod ==
                            TransformationMethod.SourceLocation
                        )
            }
            Format.avro,
            Format.apacheavro,
            ->
                columnName.isNotBlank() && !getColumns().isNullOrBlank()
            else -> false
        }
    }
}
